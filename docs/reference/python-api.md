# Python API

Packages: `ein_mesh.roles` (decision logic), `ein_mesh.integrators` (boundaries), `ein_mesh.bus`
(the transport), `ein_mesh.schemas` (records), `ein_mesh.config` (settings).

## `Role` (`ein_mesh.roles.base`)

Subclass it; implement `inputs`, `handle`, and `restore` if the role has memory.

| Member | Kind | Contract |
| --- | --- | --- |
| `name` | class attr | group id is `KAFKA_GROUP_PREFIX + name` |
| `inputs` | property | topics to consume |
| `restore()` | coroutine | rebuild memory with `bus.read_to_end`; default nothing |
| `handle(records)` | coroutine | one poll, possibly empty; return `True` when everything produced so far is written and offsets may commit |
| `idle()` | method | `True` when nothing is buffered; batching roles override |
| `consumer_options()` | method | extra consumer settings, e.g. `max_poll_interval_ms` |
| `poll_max_records()` | method | per-poll cap |
| `run()` | coroutine | subscribe → start → verify layout → restore → poll / handle / commit, forever |
| `run_until_idle()` | coroutine | test loop: returns when the bus has no unread records and `idle()` |

## `MessageBus` (`ein_mesh.bus.base`)

| Member | Contract |
| --- | --- |
| `start()`, `stop()` | open and close the transport |
| `subscribe(group, *topics, **opts)` | what `poll` reads; `group=None` means no consumer group |
| `poll(timeout_ms, max_records)` | next unread records |
| `commit()` | commit the subscription's offsets |
| `produce(topic, key, value)` | `value` is a Pydantic model or a dict |
| `read_to_end(topic)` | everything on a topic now; the restore step |
| `verify(topics)` | raise unless every topic exists with the configured partitions |
| `now()` | monotonic seconds; the in-memory bus fakes it |
| `pending()` | unread records remain; only the in-memory bus can say yes |

`Record` is `(topic, key, value: dict, offset)`.

| Implementation | Notes |
| --- | --- |
| `KafkaBus(KafkaSettings)` | the only module that imports aiokafka; one idempotent producer, one consumer created at `start()` |
| `InMemoryBus()` | topics are lists; an empty poll advances the fake clock by the timeout; helper `values(topic)` |

## Testing a role

```python
bus, topics = InMemoryBus(), Topics(prefix="t.")
await bus.produce(topics.incidents("critical"), "inc-1", incident)
await Orchestrator(bus, topics).run_until_idle()
assert bus.values(topics.incidents_state)[0]["status"] == "investigating"
```

Unit tests replace `Triage.classify` with a coroutine, or mock `litellm.acompletion` to exercise
prompt building and parsing. No broker, no model.
