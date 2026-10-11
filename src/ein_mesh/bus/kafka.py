"""``KafkaBus``: the ``MessageBus`` over aiokafka, plus the thin aiokafka helpers it is built from
(producer, consumer, read_to_end, create_topics). The only module that imports Kafka."""

from __future__ import annotations

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.coordinator.protocol import ConsumerProtocolMemberAssignment
from aiokafka.errors import TopicAuthorizationFailedError, for_code
from aiokafka.structs import TopicPartition

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


DYNAMIC_TOPIC_CONFIG = 1
"""DescribeConfigs ``config_source`` for a value set on the topic itself rather than a broker default."""
TOPIC_ALREADY_EXISTS = 36
UNKNOWN_TOPIC = 3
"""Kafka error codes aiokafka reports per topic instead of raising."""


async def topic_configs(admin: AIOKafkaAdminClient, names: list[str]) -> dict[str, dict[str, str]]:
    """The topic-level config actually set on each named topic (broker defaults excluded). Raises
    the Kafka error for a resource the broker refused, e.g. ``TopicAuthorizationFailedError`` when
    the user lacks DescribeConfigs.

    aiokafka hands back tuples: a resource is ``(error_code, error_message, resource_type,
    resource_name, config_entries)``; an entry is ``(config_name, config_value, read_only,
    is_default | config_source, is_sensitive, ...)`` depending on the protocol version."""
    out: dict[str, dict[str, str]] = {name: {} for name in names}
    responses = await admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, n) for n in names])
    for resp in responses:
        for resource in resp.resources:
            code, message, name, entries = resource[0], resource[1], resource[3], resource[4]
            if code:
                raise for_code(code)(f"describe configs of {name}: {message}")
            for entry in entries:
                key, value, flag = entry[0], entry[1], entry[3]
                dynamic = (not flag) if isinstance(flag, bool) else flag == DYNAMIC_TOPIC_CONFIG
                if dynamic and value is not None:
                    out.setdefault(name, {})[key] = value
    return out


async def reconcile_topic_configs(admin: AIOKafkaAdminClient, topics: Topics, names: list[str]) -> list[str]:
    """Make each existing topic's config equal to ``topics.all()``: keys we want are set, keys we do
    not want fall back to broker defaults. Returns the topics that were changed. Partition counts are
    not touched here; those cannot be reconciled, see ``verify_layout``.

    AlterConfigs replaces the whole topic-level config, which is exactly the intent. Its response
    resources are ``(error_code, error_message, resource_type, resource_name)``."""
    try:
        current = await topic_configs(admin, names)
    except TopicAuthorizationFailedError as e:
        log.warning("topic config check skipped: this user may not DescribeConfigs/AlterConfigs (%s). "
                    "Grant the admin role or run init with admin credentials.", e)
        return []
    changed: list[str] = []
    for name in names:
        drift = topics.config_drift(name, current.get(name, {}))
        if not drift:
            continue
        for key, (old, new) in drift.items():
            log.info("topic %s: %s %s -> %s", name, key, old if old is not None else "(default)",
                     new if new is not None else "(default)")
        responses = await admin.alter_configs([ConfigResource(ConfigResourceType.TOPIC, name, configs=topics.all()[name])])
        for resp in responses:
            for resource in resp.resources:
                if resource[0]:
                    raise for_code(resource[0])(f"alter configs of {resource[3]}: {resource[1]}")
        changed.append(name)
    return changed


async def wait_for_topics(admin: AIOKafkaAdminClient, names: list[str], attempts: int = 20) -> None:
    """CreateTopics returns before every broker's metadata shows the new topics; poll until it does."""
    for _ in range(attempts):
        counts = await partition_counts(admin, names)
        if all(counts.get(n) for n in names):
            return
        await asyncio.sleep(0.5)


async def verify_layout(admin: AIOKafkaAdminClient, topics: Topics) -> None:
    """Raise ``RuntimeError`` unless every mesh topic exists with ``topics.partitions`` partitions."""
    errors = topics.layout_errors(await partition_counts(admin, list(topics.all())))
    if errors:
        raise RuntimeError("topic layout mismatch:\n  " + "\n  ".join(errors))


async def create_topics(settings: KafkaSettings, topics: Topics) -> tuple[list[str], list[str]]:
    """The v1 init step: make every mesh topic exist with ``topics.partitions`` partitions and
    ``topics.replication`` replicas, bring existing topics' config (cleanup policy, retention) in
    line with the layout, then check the partition counts. Returns ``(created, updated)``. Safe to
    re-run. A topic with a different partition count makes init fail, because co-partitioning cannot
    be repaired in place.

    aiokafka's CreateTopics returns per-topic error codes instead of raising, so they are read here."""
    admin = AIOKafkaAdminClient(**settings.client_kwargs())
    await admin.start()
    created: list[str] = []
    existing: list[str] = []
    try:
        for name, cfg in topics.all().items():
            resp = await admin.create_topics([
                NewTopic(name, num_partitions=topics.partitions, replication_factor=topics.replication,
                         topic_configs=cfg)
            ])
            for topic, code, message in resp.topic_errors:
                if code == 0:
                    created.append(topic)
                elif code == TOPIC_ALREADY_EXISTS:
                    existing.append(topic)
                else:
                    raise for_code(code)(f"create topic {topic}: {message}")
        if created:
            await wait_for_topics(admin, created)
        updated = await reconcile_topic_configs(admin, topics, existing) if existing else []
        for name in existing:
            log.info("topic %s already exists%s", name, " (config updated)" if name in updated else "")
        await verify_layout(admin, topics)
    finally:
        await admin.close()
    return created, updated


async def wait_for_topics_gone(admin: AIOKafkaAdminClient, names: list[str], attempts: int = 60) -> None:
    """DeleteTopics only marks topics for deletion; creating one again before the broker has finished
    answers TopicAlreadyExists, so poll until metadata no longer lists any of them."""
    counts: dict[str, int | None] = {}
    for _ in range(attempts):
        counts = await partition_counts(admin, names)
        if not any(counts.get(n) for n in names):
            return
        await asyncio.sleep(0.5)
    raise RuntimeError(f"topics still present after delete: {[n for n in names if counts.get(n)]}")


async def reset_topics(settings: KafkaSettings, topics: Topics, prune: bool = False) -> tuple[list[str], list[str]]:
    """Development only: wipe every mesh topic by deleting and recreating it, returning
    ``(deleted, created)``. Refuses while any role is connected, because a live consumer would keep
    its in-memory position and could skip records written to the fresh topic. Deleting a topic also
    drops the committed offsets of every group for it, so roles restart from the beginning. With
    ``prune``, topics under the prefix that the layout no longer names (an earlier topic model) are
    deleted too and not recreated. Needs ``DELETE`` on the topics, which the Terraform module grants
    through the data-integrator's ``entity-permissions``."""
    active = [g for g in await consumer_groups(settings, settings.group_prefix) if g["members"]]
    if active:
        raise RuntimeError("roles still connected, stop them first: "
                           + ", ".join(f"{g['group']} ({len(g['members'])} members)" for g in active))
    admin = AIOKafkaAdminClient(**settings.client_kwargs())
    await admin.start()
    deleted: list[str] = []
    try:
        names = list(topics.all())
        present = [n for n, c in (await partition_counts(admin, names)).items() if c]
        if prune:
            stale = topics.stale(sorted(await admin.list_topics()))
            if stale:
                log.info("pruning topics outside the layout: %s", stale)
            present += stale
        if present:
            resp = await admin.delete_topics(present)
            for topic, code in resp.topic_error_codes:
                if code == 0:
                    deleted.append(topic)
                elif code != UNKNOWN_TOPIC:
                    raise for_code(code)(f"delete topic {topic}")
            await wait_for_topics_gone(admin, deleted)
            log.info("deleted %s", deleted)
    finally:
        await admin.close()
    created, _updated = await create_topics(settings, topics)
    return deleted, created


async def topic_status(settings: KafkaSettings, topics: Topics) -> list[dict]:
    """One row per mesh topic: partition count against the configured one and how many records it
    holds (end minus beginning offsets, so a compacted topic counts what compaction has kept).
    A topic that does not exist has ``partitions`` ``None``."""
    admin = AIOKafkaAdminClient(**settings.client_kwargs())
    await admin.start()
    c = consumer(settings, None)
    await c.start()
    rows: list[dict] = []
    try:
        counts = await partition_counts(admin, list(topics.all()))
        for name, n in counts.items():
            if not n:
                rows.append({"topic": name, "partitions": None, "records": None, "partitions_ok": False})
                continue
            tps = [TopicPartition(name, p) for p in range(n)]
            begin, end = await c.beginning_offsets(tps), await c.end_offsets(tps)
            rows.append({"topic": name, "partitions": n, "partitions_ok": n == topics.partitions,
                         "records": sum(end[tp] - begin[tp] for tp in tps)})
    finally:
        await c.stop()
        await admin.close()
    return rows


async def consumer_groups(settings: KafkaSettings, prefix: str) -> list[dict]:
    """The roster: every consumer group whose id starts with ``prefix`` (the mesh's roles), with its
    state, members and per-topic lag. A group with no members is a role that stopped but whose
    offsets have not expired yet. Groups the user may not Describe are simply not listed.

    Groups are described one per request to work around aiokafka 0.14: it sends DescribeGroups v3 but
    decodes the reply with the v2 schema, which lacks the per-group ``authorized_operations`` field.
    With a single group the extra bytes trail harmlessly; with two or more the second group decodes
    misaligned and the connection dies with "Buffer underrun decoding string". The reply tuples are
    ``(error_code, group, state, protocol_type, protocol, members, ...)`` and a member is
    ``(member_id, client_id, client_host, metadata, assignment, ...)``. The group-less consumer only
    serves end offsets."""
    admin = AIOKafkaAdminClient(**settings.client_kwargs())
    await admin.start()
    c = consumer(settings, None)
    await c.start()
    out: list[dict] = []
    try:
        ids = sorted(g for g, _proto in await admin.list_consumer_groups() if g.startswith(prefix))
        if not ids:
            return out
        groups: dict[str, tuple] = {}
        for gid in ids:
            for resp in await admin.describe_consumer_groups([gid]):
                for g in resp.groups:
                    groups[g[1]] = g
        for gid in ids:
            g = groups.get(gid)
            members = []
            for m in (g[5] if g else []):
                assigned: dict[str, list[int]] = {}
                if m[4]:
                    for topic, parts in ConsumerProtocolMemberAssignment.decode(m[4]).assignment:
                        assigned[topic] = sorted(parts)
                members.append({"client_id": m[1], "host": m[2].lstrip("/"), "assignment": assigned})
            committed = await admin.list_consumer_group_offsets(gid)
            tps = list(committed)
            ends = await c.end_offsets(tps) if tps else {}
            lag: dict[str, int] = {}
            for tp, om in committed.items():
                lag[tp.topic] = lag.get(tp.topic, 0) + max(0, ends.get(tp, om.offset) - om.offset)
            out.append({"group": gid, "state": g[2] if g else "unknown", "members": members, "lag": lag})
    finally:
        await c.stop()
        await admin.close()
    return out


class KafkaBus(MessageBus):
    """The bus over aiokafka. Both clients are created in ``start()``, not here: aiokafka requires a
    running event loop when a producer or consumer is constructed."""

    def __init__(self, settings: KafkaSettings) -> None:
        self.settings = settings
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._sub: tuple[str | None, tuple[str, ...], dict] | None = None

    async def start(self) -> None:
        self._producer = producer(self.settings)
        await self._producer.start()
        if self._sub:
            group, topics, opts = self._sub
            self._consumer = consumer(self.settings, group, *topics, **opts)
            await self._consumer.start()
            log.info("consuming %s as group %s%s", ", ".join(topics),
                     f"{self.settings.group_prefix}{group}" if group else "(none)",
                     "" if group else ", offsets not committed")

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

    async def _produce(self, topic: str, key: str | None, value: dict) -> None:
        assert self._producer is not None, "start() before produce()"
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
