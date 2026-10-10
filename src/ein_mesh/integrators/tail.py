"""Tail integrator: print records from the given topics as they arrive. No group, so it never moves
anyone's offsets."""

from __future__ import annotations

import json
import sys

from ein_mesh.bus import MessageBus
from ein_mesh.config import Topics


async def tail(bus: MessageBus, topics: Topics, names: list[str], from_start: bool) -> None:
    """``names`` are short names (``incidents.critical``) or full topic names; empty means every
    mesh topic."""
    full = [topics.resolve(n) for n in names] or list(topics.all())
    bus.subscribe(None, *full, auto_offset_reset="earliest" if from_start else "latest")
    await bus.start()
    print(f"tailing {full}", file=sys.stderr)
    try:
        while True:
            for r in await bus.poll(1000):
                print(f"[{r.topic}] key={r.key} {json.dumps(r.value, ensure_ascii=False)}", flush=True)
    finally:
        await bus.stop()
