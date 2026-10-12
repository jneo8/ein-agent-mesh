"""The system prompts the roles run with, as files a deployment can replace."""

from __future__ import annotations

from pathlib import Path

PROMPTS_DIR = Path(__file__).parent


def load_prompt(role: str, path: str | Path | None = None) -> str:
    """The prompt text for a role: the file at ``path`` when given, else the one shipped as ``<role>.md``.

    A missing or unreadable override is an error at startup, not a silent fall back to the default."""
    source = Path(path) if path else PROMPTS_DIR / f"{role}.md"
    try:
        return source.read_text()
    except OSError as e:
        raise ValueError(f"cannot read the {role} prompt at {source}: {e}") from e
