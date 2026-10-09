"""Thin aiokafka helpers shared by every process."""

from __future__ import annotations

import json
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

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


async def create_topics(settings: KafkaSettings, topics: Topics, partitions: int = 1) -> list[str]:
    """The v1 init step: make every mesh topic exist. Safe to re-run."""
    admin = AIOKafkaAdminClient(**settings.client_kwargs())
    await admin.start()
    created: list[str] = []
    try:
        for name, cfg in topics.all().items():
            try:
                await admin.create_topics(
                    [NewTopic(name, num_partitions=partitions, replication_factor=1, topic_configs=cfg)]
                )
                created.append(name)
            except TopicAlreadyExistsError:
                log.info("topic %s already exists", name)
    finally:
        await admin.close()
    return created
