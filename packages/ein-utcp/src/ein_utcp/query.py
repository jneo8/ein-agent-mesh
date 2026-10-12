"""How a call leaves: what the model may pass, what is checked first, and the transport."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import aiohttp
from utcp_http.http_communication_protocol import HttpCommunicationProtocol

from ein_utcp.tools import Operation

log = logging.getLogger(__name__)

Dispatch = Callable[[str, dict[str, Any]], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class Argument:
    """One parameter of a call as the model supplies it: a name and a string value.

    One ``{name, value}`` pair per parameter, never a nested document: a nested one forces three levels
    of quoting for an inner ``"``, and one level is the level the model already gets right. Values stay
    strings on purpose; see ``coerce_arguments``.
    """

    name: str
    value: str


def arguments_from(raw: Any) -> list[Argument] | None:
    """The model's ``arguments``: a list of ``{name, value}`` or a plain mapping; None when neither."""
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [Argument(str(k), _as_text(v)) for k, v in raw.items()]
    if not isinstance(raw, list):
        return None
    pairs = []
    for item in raw:
        if not isinstance(item, dict) or "name" not in item:
            return None
        pairs.append(Argument(str(item["name"]), _as_text(item.get("value", ""))))
    return pairs


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str) if isinstance(value, list | dict) else str(value)


def _is_array(prop: Any) -> bool:
    declared = prop.get("type") if isinstance(prop, dict) else getattr(prop, "type", None)
    return "array" in declared if isinstance(declared, list) else declared == "array"


def _elements(value: str) -> list[str]:
    """One pair per element is the unambiguous form; a JSON array is accommodated rather than failed."""
    if value.lstrip().startswith("["):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        if isinstance(decoded, list):
            return [e if isinstance(e, str) else json.dumps(e) for e in decoded]
    return [value]


def coerce_arguments(arguments: list[Argument], properties: dict[str, Any] | None) -> dict[str, Any]:
    """Pairs into the mapping the client takes; array parameters collect their elements."""
    schema = properties or {}
    coerced: dict[str, Any] = {}
    for a in arguments:
        if _is_array(schema.get(a.name)):
            coerced.setdefault(a.name, []).extend(_elements(a.value))
            continue
        if a.name in coerced:
            log.warning("duplicate argument %r on a non-array parameter; keeping the last value", a.name)
        coerced[a.name] = a.value
    return coerced


def validate_arguments(operation: str, arguments: dict[str, Any], schema: dict[str, Any] | None) -> dict[str, Any] | None:
    """An error payload for a misspelled or missing parameter, else None. Only names are checked: every
    value is a string by design. An operation that declares no parameters is not judged (many specs
    simply do not list them)."""
    properties = (schema or {}).get("properties") or {}
    if not properties:
        return None
    unknown = sorted(set(arguments) - set(properties))
    missing = sorted(set((schema or {}).get("required") or ()) - set(arguments))
    if not unknown and not missing:
        return None
    problems = []
    if unknown:
        problems.append(f"unknown parameter(s): {', '.join(unknown)}")
    if missing:
        problems.append(f"missing required parameter(s): {', '.join(missing)}")
    return {"error": f"Invalid arguments for {operation!r} - {'; '.join(problems)}.", "parameters": schema}


async def reject_invalid_query(dispatch: Dispatch, validator: str, operation: str,
                               arguments: dict[str, Any]) -> dict[str, Any] | None:
    """An error payload when the server's parser (reached through ``validator``) rejects ``query``, else
    None. None also when there is no query, the call *is* the validator, or the validator cannot answer."""
    query = arguments.get("query")
    if operation == validator or not isinstance(query, str) or not query:
        return None
    try:
        await dispatch(validator, {"query": query})
    except aiohttp.ClientResponseError as e:
        if e.status == 400:
            said = f" Server said: {e.message}." if e.message else ""
            return {"error": f"Invalid query rejected by the server's parser (via {validator!r}): {query!r}.{said}"
                             " Check quoting and bracket matching, correct the query, and call again."}
        log.info("query validation via %s returned HTTP %s; dispatching anyway", validator, e.status)
    except Exception as e:
        log.info("query validation via %s unavailable (%s); dispatching anyway", validator, e)
    return None


class HttpDispatch:
    """Perform an operation over utcp-http's HTTP protocol; the operation's ``transport`` is its call template.

    utcp-http refuses plain HTTP to non-loopback hosts with no escape hatch. A monitoring stack behind an
    in-cluster proxy on a private network is exactly that case, so ``allow_plain_http_for_url`` opts one
    host:port in by patching the private ``utcp_http._security`` module: ``is_secure_url`` is resolved
    through module globals at call time, so replacing the attribute reaches every protocol. The install
    fails closed when the patch point is missing; a drift-guard test fails loudly if an upgrade moves it.
    """

    def __init__(self) -> None:
        self._protocol = HttpCommunicationProtocol()

    async def __call__(self, op: Operation, arguments: dict[str, Any]) -> Any:
        return await self._protocol.call_tool(None, op.name, arguments, op.transport)


def describe_failure(e: BaseException) -> str:
    """One line the model can read for a failed dispatch."""
    if isinstance(e, aiohttp.ClientResponseError):
        return f"HTTP {e.status} {e.message}".strip()
    if isinstance(e, TimeoutError):
        return "timed out waiting for the source"
    return f"{type(e).__name__}: {e}"[:1000]


_PLAIN_HTTP_ALLOWLIST: set[tuple[str, int]] = set()
_original_is_secure_url: Callable[[str], bool] | None = None

_DEFAULT_PORTS = {"http": 80, "https": 443}


def host_port_from_url(url: object) -> tuple[str, int] | None:
    """Extract (host, port) from a URL, filling in the scheme's default port; None without a hostname."""
    try:
        host = getattr(url, "host", None)
        if host is not None:  # yarl.URL exposes host/port/scheme directly
            scheme = getattr(url, "scheme", "") or "https"
            return (host, getattr(url, "port", None) or _DEFAULT_PORTS.get(scheme, 443))
        parsed = urlparse(str(url))
        if not parsed.hostname:
            return None
        return (parsed.hostname, parsed.port or _DEFAULT_PORTS.get(parsed.scheme or "https", 443))
    except Exception:
        return None


def is_loopback_host(host: str) -> bool:
    return host.lower() in {"localhost", "127.0.0.1", "::1", "[::1]"}


def allow_plain_http_for_url(url: object) -> tuple[str, int] | None:
    """Allowlist the URL's host:port for plain HTTP; None (nothing registered) for a URL without a host."""
    key = host_port_from_url(url)
    if key is None:
        return None
    _PLAIN_HTTP_ALLOWLIST.add(key)
    log.info("plain HTTP allowlisted for %s:%s", *key)
    return key


def is_plain_http_allowed(url: object) -> bool:
    """True when ``url`` is plain HTTP to an allowlisted host:port; https and other schemes never match."""
    try:
        scheme = (urlparse(str(url)).scheme or "").lower()
    except ValueError:
        return False
    if scheme != "http":
        return False
    key = host_port_from_url(url)
    return key is not None and key in _PLAIN_HTTP_ALLOWLIST


def install_plain_http_override() -> bool:
    """Idempotently wrap ``utcp_http._security.is_secure_url`` with the allowlist.

    Returns True when the override is (already) installed; False when the patch point is unavailable, in
    which case plain-HTTP URLs keep being rejected.
    """
    global _original_is_secure_url
    if _original_is_secure_url is not None:
        return True
    try:
        from utcp_http import _security
    except ImportError:
        log.error("utcp_http is not importable; plain HTTP cannot be allowed")
        return False
    original = getattr(_security, "is_secure_url", None)
    if not callable(original):
        log.error("utcp_http._security.is_secure_url not found; plain HTTP cannot be allowed (utcp-http drift?)")
        return False

    def _is_secure_url_with_allowlist(url: str) -> bool:
        return original(url) or is_plain_http_allowed(url)

    _security.is_secure_url = _is_secure_url_with_allowlist
    _original_is_secure_url = original
    log.info("utcp_http patched: plain-HTTP allowlist override installed")
    return True


def uninstall_plain_http_override() -> None:
    """Restore the original ``is_secure_url`` (test helper)."""
    global _original_is_secure_url
    if _original_is_secure_url is None:
        return
    from utcp_http import _security

    _security.is_secure_url = _original_is_secure_url
    _original_is_secure_url = None


def clear_plain_http_allowlist() -> None:
    _PLAIN_HTTP_ALLOWLIST.clear()
