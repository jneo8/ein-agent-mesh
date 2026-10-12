import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from litellm.types.utils import ChatCompletionMessageToolCall, Message

from ein_mesh.bus import InMemoryBus
from ein_mesh.config import GroupingSettings, Topics
from ein_mesh.integrators import derive_group
from ein_mesh.schemas import Alert


@pytest.fixture
def topics() -> Topics:
    return Topics(prefix="t.")


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


STORM = Path(__file__).resolve().parent.parent / "eval" / "scenarios" / "ceph-osd-storm" / "alerts.json"


def storm_payload() -> dict:
    return json.loads(STORM.read_text())


def make_alert(alert_id: str, cluster: str = "dev", ns: str = "ceph", name: str = "CephOSDDown",
               severity: str = "critical", status: str = "firing") -> Alert:
    """An Alert as the gateway would build it: ``id`` is ``fingerprint:startsAt`` (``<alert_id>:t0`` unless
    given in full), ``group_key`` derived from the default GROUP_BY labels."""
    labels = {"alertname": name, "severity": severity, "namespace": ns, "cluster": cluster}
    group_key, group_labels = derive_group(labels, GroupingSettings().group_by)
    alert_id = alert_id if ":" in alert_id else f"{alert_id}:t0"
    return Alert(id=alert_id, cluster=cluster, namespace=ns, alertname=name, severity=severity, status=status,
                 labels=labels, group_key=group_key, group_labels=group_labels)


def tool_call(name: str, arguments, call_id: str = "c1") -> ChatCompletionMessageToolCall:
    """One tool call as a model emits it; ``arguments`` may be anything JSON-encodable, or a raw string."""
    raw = arguments if isinstance(arguments, str) else json.dumps(arguments)
    return ChatCompletionMessageToolCall(id=call_id, type="function", function={"name": name, "arguments": raw})


def turn(calls=None, content=None) -> Message:
    """One assistant turn: tool calls, plain content, or both."""
    return Message(role="assistant", content=content, tool_calls=calls or None)


def scripted_model(turns: list[Message]):
    """A litellm.acompletion stand-in that replays ``turns`` (the last one repeats) and records every request."""
    requests: list[dict] = []

    async def acompletion(**kw):
        requests.append(copy.deepcopy(kw))  # the loop keeps appending to the same message list
        message = turns[min(len(requests) - 1, len(turns) - 1)]
        return SimpleNamespace(choices=[SimpleNamespace(message=message)],
                               usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1))

    return acompletion, requests
