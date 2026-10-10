"""``KafkaBus``: the ``MessageBus`` over aiokafka, plus the thin aiokafka helpers it is built from
(producer, consumer, read_to_end, create_topics). The only module that imports Kafka."""

from __future__ import annotations

import json
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from ein_mesh.bus.base import MessageBus, Record
from ein_mesh.config import KafkaSettings, Topics

log = logging.getLogger(__name__)


def dumps(obj) -> bytes:
    if hasattr(obj, "model_dump"):
        obj = obj.model_dump(by_alias=True)
    return json.dumps(obj, ensure_ascii=False).encode()


def loads(raw: bytes | None):
    return None if raw is None else json.loads(raw)


def producer(settings: KafkaSettings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        **settings.client_kwargs(),
        key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
        value_serializer=dumps,
        enable_idempotence=True,
        compression_type="gzip",
    )


def consumer(settings: KafkaSettings, group: str | None, *topics: str, **kw) -> AIOKafkaConsumer:
    opts: dict = dict(
        group_id=f"{settings.group_prefix}{group}" if group else None,
        value_deserializer=loads,
        key_deserializer=lambda k: None if k is None else k.decode(),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    opts.update(kw)
    return AIOKafkaConsumer(*topics, **settings.client_kwargs(), **opts)


async def read_to_end(settings: KafkaSettings, topic: str) -> list:
    """Every record currently on ``topic`` (no group, earliest to the end offsets). For compacted
    state topics this is the restore step; later records for a key override earlier ones."""
    c = consumer(settings, None, topic)
    await c.start()
    out: list = []
    try:
        tps = list(c.assignment())
        end = await c.end_offsets(tps)
        while True:
            positions = [await c.position(tp) for tp in tps]
            if all(pos >= end[tp] for pos, tp in zip(positions, tps)):
                break
            batch = await c.getmany(timeout_ms=2000)
            if not batch:
                break
            for records in batch.values():
                out.extend(records)
    finally:
        await c.stop()
    return out


async def partition_counts(admin: AIOKafkaAdminClient, names: list[str]) -> dict[str, int | None]:
    """Partition count of each named topic on the broker, ``None`` for a topic that does not exist."""
    described = await admin.describe_topics(names)
    found = {t["topic"]: len(t["partitions"]) for t in described if t.get("error_code", 0) == 0 and t["partitions"]}
    return {name: found.get(name) for name in names}


async def verify_layout(admin: AIOKafkaAdminClient, topics: Topics) -> None:
    """Raise ``RuntimeError`` unless every mesh topic exists with ``topics.partitions`` partitions."""
    errors = topics.layout_errors(await partition_counts(admin, list(topics.all())))
    if errors:
        raise RuntimeError("topic layout mismatch:\n  " + "\n  ".join(errors))


async def create_topics(settings: KafkaSettings, topics: Topics) -> list[str]:
    """The v1 init step: make every mesh topic exist with ``topics.partitions`` partitions and
    ``topics.replication`` replicas, then check the layout. Safe to re-run: existing topics are left
    alone, but one with a different partition count makes init fail, because co-partitioning cannot
    be repaired in place."""
    admin = AIOKafkaAdminClient(**settings.client_kwargs())
    await admin.start()
    created: list[str] = []
    try:
        for name, cfg in topics.all().items():
            try:
                await admin.create_topics([
                    NewTopic(name, num_partitions=topics.partitions, replication_factor=topics.replication,
                             topic_configs=cfg)
                ])
                created.append(name)
            except TopicAlreadyExistsError:
                log.info("topic %s already exists", name)
        await verify_layout(admin, topics)
    finally:
        await admin.close()
    return created


class KafkaBus(MessageBus):
    def __init__(self, settings: KafkaSettings) -> None:
        self.settings = settings
        self._producer = producer(settings)
        self._consumer: AIOKafkaConsumer | None = None
        self._sub: tuple[str | None, tuple[str, ...], dict] | None = None

    async def start(self) -> None:
        await self._producer.start()
        if self._sub:
            group, topics, opts = self._sub
            self._consumer = consumer(self.settings, group, *topics, **opts)
            await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        await self._producer.stop()

    async def _produce(self, topic: str, key: str | None, value: dict) -> None:
        await self._producer.send_and_wait(topic, key=key, value=value)

    async def read_to_end(self, topic: str) -> list[Record]:
        return [Record(r.topic, r.key, r.value, r.offset) for r in await read_to_end(self.settings, topic)]

    def subscribe(self, group: str | None, *topics: str, **consumer_options) -> None:
        self._sub = (group, topics, consumer_options)

    async def poll(self, timeout_ms: int, max_records: int | None = None) -> list[Record]:
        assert self._consumer is not None, "subscribe() before start()"
        polled = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        return [Record(r.topic, r.key, r.value, r.offset) for records in polled.values() for r in records]

    async def commit(self) -> None:
        assert self._consumer is not None
        await self._consumer.commit()

    async def verify(self, topics: Topics) -> None:
        admin = AIOKafkaAdminClient(**self.settings.client_kwargs())
        await admin.start()
        try:
            await verify_layout(admin, topics)
        finally:
            await admin.close()
