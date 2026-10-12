"""The model's conversation with its tools, driven by a scripted model: every path ends in an answer or a
ToolLoopError, and every tool call gets its tool message."""

from unittest.mock import patch

import pytest
from pydantic import BaseModel

from ein_mesh.roles import toolloop
from ein_mesh.roles.toolloop import SUBMIT, ToolLoop, ToolLoopError
from ein_utcp import ScriptedSource, Toolbox

from .conftest import scripted_model, tool_call, turn

pytestmark = pytest.mark.asyncio


class Answer(BaseModel):
    summary: str
    notes: list[str] = []


CATALOG = ["alertmanager.getAlerts | /api/v2/alerts | (filter) | Get a list of alerts"]


def toolbox(result=None):
    return Toolbox([ScriptedSource("alertmanager", CATALOG, default=result or [{"labels": {"alertname": "A"}}])])


def get_alerts(call_id="c1"):
    return tool_call("call_operation", {"source": "alertmanager", "operation": "getAlerts"}, call_id)


async def run(turns, box=None, token_budget=100, time_budget=60.0):
    acompletion, requests = scripted_model(turns)  # every scripted turn reports 2 tokens
    with patch.object(toolloop.litellm, "acompletion", acompletion):
        loop = ToolLoop("fake", box or toolbox(), Answer, token_budget=token_budget, time_budget=time_budget)
        return await loop.run("sys", "task"), requests, loop


async def test_tool_calls_are_answered_in_order_then_submit_ends_the_loop():
    box = toolbox()
    answer, requests, loop = await run([
        turn([get_alerts("c1"), tool_call("list_results", {}, "c2")]),  # two calls in one turn
        turn([tool_call(SUBMIT, {"summary": "A is firing", "notes": ["A - evidence: call-1"]})]),
    ], box)
    assert answer == Answer(summary="A is firing", notes=["A - evidence: call-1"])
    assert loop.tool_calls == 2 and loop.turns == 2
    messages = requests[1]["messages"]
    assert [m["role"] for m in messages] == ["system", "user", "assistant", "tool", "tool"]
    assert [m["tool_call_id"] for m in messages if m["role"] == "tool"] == ["c1", "c2"]
    assert '"alertname": "A"' in messages[3]["content"] and messages[4]["content"].startswith("call-1:")
    assert box.calls_made() == ["alertmanager.getAlerts"]
    assert [t["function"]["name"] for t in requests[0]["tools"]][-1] == SUBMIT and requests[0]["tool_choice"] == "auto"


async def test_a_plain_json_answer_is_accepted_without_a_tool_call():
    answer, requests, _ = await run([turn(content='```json\n{"summary": "done"}\n```')])
    assert answer.summary == "done" and len(requests) == 1


@pytest.mark.parametrize("budget, turns_before_submit", [({"token_budget": 4}, 2), ({"time_budget": 0.0}, 0)])
async def test_a_spent_budget_forces_a_submit(budget, turns_before_submit):
    answer, requests, loop = await run([
        turn([get_alerts("c1")]), turn([get_alerts("c2")]),  # keeps calling
        turn([tool_call(SUBMIT, {"summary": "forced"})]),
    ][2 - turns_before_submit:], **budget)
    assert answer.summary == "forced" and loop.tool_calls == turns_before_submit
    forced = requests[turns_before_submit]
    assert forced["tool_choice"] == {"type": "function", "function": {"name": SUBMIT}}
    assert [t["function"]["name"] for t in forced["tools"]] == [SUBMIT]
    assert forced["messages"][-1]["role"] == "user" and "submit" in forced["messages"][-1]["content"]


async def test_an_invalid_submit_is_corrected_once_then_accepted():
    answer, requests, _ = await run([
        turn([tool_call(SUBMIT, {"notes": "not a list"}, "s1")]),
        turn([tool_call(SUBMIT, {"summary": "fixed", "notes": []}, "s2")]),
    ])
    assert answer.summary == "fixed"
    correction = requests[1]["messages"][-1]
    assert correction["role"] == "tool" and correction["tool_call_id"] == "s1" and "invalid report" in correction["content"]


async def test_an_empty_turn_falls_back_to_forced_submit_then_json():
    answer, requests, _ = await run([
        turn(content="I think it is A."),  # no call, not JSON
        turn(content="still prose"),  # forced submit ignored
        turn(content='{"summary": "json fallback"}'),
    ])
    assert answer.summary == "json fallback"
    assert requests[2]["response_format"] == {"type": "json_object"} and "tools" not in requests[2]


async def test_no_answer_anywhere_is_a_tool_loop_error():
    with pytest.raises(ToolLoopError, match="no report after 3 turns"):
        await run([turn(content="nope")] * 3)


async def test_a_failing_tool_is_a_message_not_an_exception():
    answer, requests, _ = await run([
        turn([tool_call("call_operation", {"source": "grafana", "operation": "x"}, "c1"),
              tool_call("call_operation", "not even json", "c2")]),
        turn([tool_call(SUBMIT, {"summary": "ok"})]),
    ])
    assert answer.summary == "ok"
    tool_messages = [m for m in requests[1]["messages"] if m["role"] == "tool"]
    assert "unknown or missing source" in tool_messages[0]["content"]
    assert "unknown or missing source" in tool_messages[1]["content"]  # bad JSON arguments become {}
