import asyncio
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from src.application.services.outbox_relay import OutboxRelay
from src.application.usecases.create_ad import CreateAd
from src.application.usecases.delete_ad import DeleteAd
from src.application.usecases.update_ad import UpdateAd
from src.infrastructure.messaging.kafka_broker import KafkaMessageBroker, serialize
from src.infrastructure.persistence.models import AdModel, Base, OutboxModel
from src.infrastructure.persistence.uow import SQLAlchemyUnitOfWork

TRACE_ID = "550e8400-e29b-41d4-a716-446655440000"
FIELDS = dict(
    title="Original", description="description", price=20, category="test", city="test"
)


@pytest.fixture
async def postgres_sessions():
    dsn = os.getenv("MARKETPLACE_AD_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set MARKETPLACE_AD_TEST_POSTGRES_DSN to an isolated PostgreSQL")
    schema = f"ad_outbox_test_{uuid4().hex}"
    engine = create_async_engine(dsn, hide_parameters=True)
    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema))
        try:
            isolated = engine.execution_options(schema_translate_map={None: schema})
            async with isolated.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
            yield async_sessionmaker(isolated, expire_on_commit=False)
        finally:
            async with engine.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
    finally:
        await engine.dispose()


async def snapshot(session):
    ads = await session.execute(
        select(AdModel.id, AdModel.title, AdModel.status).order_by(AdModel.id)
    )
    events = await session.execute(
        select(OutboxModel.event_type, OutboxModel.payload).order_by(OutboxModel.id)
    )
    return ads.all(), events.all()


class CommitFailure(Exception):
    pass


@pytest.mark.parametrize(
    ("usecase", "event"),
    [(CreateAd, "ad.created"), (UpdateAd, "ad.updated"), (DeleteAd, "ad.deleted")],
)
async def test_outbox_and_ad_commit_or_roll_back_together(
    postgres_sessions, usecase, event
):
    args = {"user_id": 1, "trace_id": TRACE_ID}
    if usecase is CreateAd:
        args.update(FIELDS)
    else:
        original = await CreateAd(SQLAlchemyUnitOfWork(postgres_sessions)).execute(
            user_id=1, **FIELDS
        )
        args["ad_id"] = original.id
        if usecase is UpdateAd:
            args.update({**FIELDS, "title": "Updated"})

    async with postgres_sessions() as session:
        before_ads, before_events = await snapshot(session)

    class FailAfterFlush(SQLAlchemyUnitOfWork):
        async def commit(self):
            await self._session.flush()
            ads, events = await snapshot(self._session)
            assert ads != before_ads
            assert events[:-1] == before_events
            assert events[-1][0] == event
            assert events[-1][1]["trace_id"] == TRACE_ID
            raise CommitFailure

    with pytest.raises(CommitFailure):
        await usecase(FailAfterFlush(postgres_sessions)).execute(**args)

    async with postgres_sessions() as session:
        assert await snapshot(session) == (before_ads, before_events)

    result = await usecase(SQLAlchemyUnitOfWork(postgres_sessions)).execute(**args)
    ad_id = args["ad_id"] if result is None else result.id
    async with postgres_sessions() as session:
        ads, events = await snapshot(session)
    expected_title = "Updated" if usecase is UpdateAd else FIELDS["title"]
    expected_status = "archived" if usecase is DeleteAd else "active"
    assert ads == [(ad_id, expected_title, expected_status)]
    assert events == [
        *before_events,
        (event, {"ad_id": ad_id, "trace_id": TRACE_ID}),
    ]


async def test_real_outbox_reads_legacy_unpublished_row(postgres_sessions):
    ad = await CreateAd(SQLAlchemyUnitOfWork(postgres_sessions)).execute(
        user_id=1, **FIELDS
    )
    async with SQLAlchemyUnitOfWork(postgres_sessions) as uow:
        (row,) = await uow.outbox.fetch_unpublished(10)
        assert row.event_type == "ad.created"
        assert row.payload == {"ad_id": ad.id}


@pytest.fixture
def kafka_brokers():
    brokers = os.getenv("MARKETPLACE_AD_TEST_KAFKA_BROKERS")
    if not brokers:
        pytest.skip("set MARKETPLACE_AD_TEST_KAFKA_BROKERS to an isolated broker")
    return brokers


@asynccontextmanager
async def temporary_kafka_topic(brokers):
    topic = f"ad-outbox-test-{uuid4().hex}"
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers, request_timeout_ms=5000)
    try:
        await admin.start()
        created = await admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1)]
        )
        if not any(
            name == topic and code == 0 for name, code, *_ in created.topic_errors
        ):
            raise RuntimeError(f"Failed to create Kafka test topic {topic}")
        try:
            yield topic
        finally:
            try:
                deleted = await admin.delete_topics([topic])
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to delete Kafka test topic {topic}"
                ) from exc
            if deleted.topic_error_codes != [(topic, 0)]:
                raise RuntimeError(
                    f"Failed to delete Kafka test topic {topic}: "
                    f"{deleted.topic_error_codes}"
                )
    finally:
        await admin.close()


@pytest.fixture
async def kafka_topic(kafka_brokers):
    async with temporary_kafka_topic(kafka_brokers) as topic:
        yield kafka_brokers, topic


@pytest.mark.parametrize("fail", [False, True], ids=["success", "exception"])
async def test_kafka_topic_cleanup_on_real_broker(kafka_brokers, fail):
    admin = AIOKafkaAdminClient(
        bootstrap_servers=kafka_brokers, request_timeout_ms=5000
    )
    await admin.start()
    try:
        original_topics = set(await admin.list_topics())
        try:
            async with temporary_kafka_topic(kafka_brokers) as topic:
                async with asyncio.timeout(5):
                    while topic not in await admin.list_topics():
                        await asyncio.sleep(0.1)
                if fail:
                    raise CommitFailure
        except CommitFailure:
            assert fail
        else:
            assert not fail

        async with asyncio.timeout(5):
            while topic in (remaining := set(await admin.list_topics())):
                await asyncio.sleep(0.1)
        assert original_topics <= remaining
    finally:
        await admin.close()


async def test_kafka_topic_cleanup_failure_names_topic_and_closes_client(monkeypatch):
    async def create_topics(topics):
        return SimpleNamespace(topic_errors=[(topics[0].name, 0)])

    async def delete_topics(topics):
        return SimpleNamespace(topic_error_codes=[(topics[0], 29)])

    admin = Mock(
        start=AsyncMock(),
        create_topics=AsyncMock(side_effect=create_topics),
        delete_topics=AsyncMock(side_effect=delete_topics),
        close=AsyncMock(),
    )
    monkeypatch.setattr(f"{__name__}.AIOKafkaAdminClient", lambda **kwargs: admin)

    with pytest.raises(RuntimeError, match=r"Failed to delete Kafka test topic .*29"):
        async with temporary_kafka_topic("broker:9092") as topic:
            assert topic.startswith("ad-outbox-test-")

    admin.delete_topics.assert_awaited_once_with([topic])
    admin.close.assert_awaited_once()


async def test_relay_retries_acknowledged_record_after_outbox_commit_failure(
    postgres_sessions, kafka_topic
):
    brokers, topic = kafka_topic
    ad = await CreateAd(SQLAlchemyUnitOfWork(postgres_sessions)).execute(
        user_id=1, trace_id=TRACE_ID, **FIELDS
    )

    class FailAfterPublish(SQLAlchemyUnitOfWork):
        async def commit(self):
            await self._session.flush()
            own = await self._session.scalar(select(OutboxModel))
            assert own.published_at is not None
            async with SQLAlchemyUnitOfWork(postgres_sessions) as contender:
                # A competing relay skips the locked pending row.
                assert await contender.outbox.fetch_unpublished(1) == []
            raise CommitFailure

    producer = AIOKafkaProducer(
        bootstrap_servers=brokers, value_serializer=serialize, request_timeout_ms=5000
    )
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        auto_offset_reset="earliest",
        group_id=None,
        enable_auto_commit=False,
        request_timeout_ms=5000,
    )
    async with producer, consumer:
        broker = KafkaMessageBroker(producer, topic)
        relay = OutboxRelay(lambda: FailAfterPublish(postgres_sessions), broker)
        with pytest.raises(CommitFailure):
            await relay._process_batch()

        async with postgres_sessions() as session:
            row = await session.scalar(select(OutboxModel))
            assert row.published_at is None
            assert row.payload == {"ad_id": ad.id, "trace_id": TRACE_ID}

        retry = OutboxRelay(lambda: SQLAlchemyUnitOfWork(postgres_sessions), broker)
        assert await retry._process_batch() == 1
        received = []
        while len(received) < 2:
            batch = await consumer.getmany(
                timeout_ms=5000, max_records=2 - len(received)
            )
            assert batch, "expected two broker records after retry"
            for records in batch.values():
                received.extend(records)

    expected = serialize(
        {
            "event": "ad.created",
            "payload": {
                "ad_id": ad.id,
                "trace_id": TRACE_ID,
            },
        }
    )
    assert [record.value for record in received] == [expected, expected]
    assert [record.offset for record in received] == [0, 1]
    async with postgres_sessions() as session:
        assert await session.scalar(select(OutboxModel.published_at)) is not None
