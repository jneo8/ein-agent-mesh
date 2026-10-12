"""A model's conversation with its tools, from a task to a structured answer."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, TypeVar

import litellm
from pydantic import BaseModel, ValidationError

from ein_utcp import Toolbox

log = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

SUBMIT = "submit_report"


class ToolLoopError(Exception):
    """The conversation ended without an answer the schema accepts."""


class ToolLoop:
    """Run a model against a toolbox until it submits an answer of type ``answer``.

    Each turn is one completion. A tool call is routed to the toolbox and its result appended as a tool
    message; a call to ``submit_report`` ends the loop with the parsed answer. The budget is what the
    investigation really costs: ``token_budget`` caps the tokens spent over every turn (prompt and
    completion, as the model reports them), ``time_budget`` the seconds since the loop started. Once
    either is spent, or when a turn calls nothing usable, the model is asked once to submit, with the
    tool choice forced, and failing that once more for a plain JSON answer. Anything else is a
    ``ToolLoopError``.
    """

    def __init__(self, model: str, toolbox: Toolbox, answer: type[T], *, token_budget: int, time_budget: float) -> None:
        self.model = model
        self.toolbox = toolbox
        self.answer = answer
        self.token_budget = token_budget
        self.time_budget = time_budget
        self.tokens = 0
        self.tool_calls = 0
        self.turns = 0
        self.started = time.monotonic()

    @property
    def spent(self) -> bool:
        return self.tokens >= self.token_budget or time.monotonic() - self.started >= self.time_budget

    async def run(self, system: str, user: str) -> T:
        messages: list[dict[str, Any]] = [{"role": "system", "content": system}, {"role": "user", "content": user}]
        tools = self.toolbox.tool_specs() + [self._submit_spec()]
        self.started = time.monotonic()
        while not self.spent:
            message = await self._turn(messages, tools)
            calls = list(getattr(message, "tool_calls", None) or [])
            if not calls:
                answer = self._parse_content(getattr(message, "content", None))
                if answer is not None:
                    return answer
                break
            for call in calls:
                name, arguments = call.function.name, _arguments(call.function.arguments)
                if name == SUBMIT:
                    try:
                        return self.answer.model_validate(arguments)
                    except ValidationError as e:
                        messages.append(_tool_message(call.id, name, f"invalid report: {e.errors()[:3]}; fix the fields and submit again"))
                        continue
                self.tool_calls += 1
                log.info("tool call %d %s %s", self.tool_calls, name, json.dumps(arguments, ensure_ascii=False)[:200])
                messages.append(_tool_message(call.id, name, await self.toolbox.call(name, arguments)))
        return await self._salvage(messages)

    async def _salvage(self, messages: list[dict[str, Any]]) -> T:
        """Two last chances: a forced ``submit_report`` call, then a plain JSON answer."""
        log.info("asking %s to submit after %d tool calls, %d tokens, %.0fs", self.model, self.tool_calls, self.tokens,
                 time.monotonic() - self.started)
        messages.append({"role": "user", "content": f"Stop investigating and submit your findings now with {SUBMIT}."})
        message = await self._turn(messages, [self._submit_spec()],
                                   tool_choice={"type": "function", "function": {"name": SUBMIT}})
        for call in getattr(message, "tool_calls", None) or []:
            if call.function.name == SUBMIT:
                try:
                    return self.answer.model_validate(_arguments(call.function.arguments))
                except ValidationError as e:
                    log.warning("forced submit was invalid: %s", e)
        answer = self._parse_content(getattr(message, "content", None))
        if answer is not None:
            return answer
        messages.append({"role": "user", "content": "Answer with the report as a JSON object and nothing else."})
        message = await self._turn(messages, None, response_format={"type": "json_object"})
        answer = self._parse_content(getattr(message, "content", None))
        if answer is None:
            raise ToolLoopError(f"no report after {self.turns} turns and {self.tool_calls} tool calls")
        return answer

    async def _turn(self, messages: list[dict[str, Any]], tools: list[dict] | None, **kw: Any) -> Any:
        self.turns += 1
        params: dict[str, Any] = {"model": self.model, "messages": messages, "_skip_mcp_handler": True, **kw}
        if tools:
            params.update(tools=tools, tool_choice=kw.get("tool_choice", "auto"))
        response = await litellm.acompletion(**params)
        message = response.choices[0].message
        messages.append(_assistant_message(message))
        usage = getattr(response, "usage", None)
        if usage is not None:
            prompt, completion = getattr(usage, "prompt_tokens", 0) or 0, getattr(usage, "completion_tokens", 0) or 0
            self.tokens += prompt + completion
            log.info("turn %d: %d prompt tokens, %d completion tokens, %d of %d spent", self.turns, prompt, completion,
                     self.tokens, self.token_budget)
        return message

    def _parse_content(self, content: Any) -> T | None:
        if not isinstance(content, str) or not content.strip():
            return None
        try:
            return self.answer.model_validate_json(_strip_fence(content))
        except ValidationError:
            return None

    def _submit_spec(self) -> dict[str, Any]:
        schema = self.answer.model_json_schema()
        schema.pop("title", None)
        return {"type": "function", "function": {
            "name": SUBMIT, "description": "Submit the report. This ends the investigation.", "parameters": schema}}


def _assistant_message(message: Any) -> dict[str, Any]:
    """The assistant turn as the API wants it echoed back, provider fields (thought signatures) included."""
    dump = message.model_dump() if hasattr(message, "model_dump") else dict(vars(message))
    return {k: v for k, v in dump.items() if v is not None and k != "function_call"} | {"role": "assistant"}


def _tool_message(call_id: str, name: str, content: str) -> dict[str, Any]:
    return {"role": "tool", "tool_call_id": call_id, "name": name, "content": content}


def _arguments(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        decoded = json.loads(raw) if raw else {}
    except (TypeError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _strip_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        text = text.rsplit("```", 1)[0]
    return text.strip()
