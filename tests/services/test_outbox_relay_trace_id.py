import json
import logging
from io import StringIO
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from src.application.services.outbox_relay import OutboxRelay
from src.infrastructure.messaging.kafka_broker import KafkaMessageBroker, serialize
from src.infrastructure.persistence.outbox_repository import SQLAlchemyOutboxRepository
from src.trace_context import (
    configure_trace_logging,
    get_trace_id,
    reset_trace_id,
    set_trace_id,
)

TRACE_A = "550e8400-e29b-41d4-a716-446655440000"
TRACE_B = "8b738d87-25b4-43bd-9d6b-bbb2c07939ad"
OUTER_TRACE = "ee777944-bb03-4db9-bc67-277b4d7f4973"


@pytest.fixture
def relay_log_output(monkeypatch):
    logger = logging.getLogger("src")
    monkeypatch.setattr(logger, "handlers", [])
    monkeypatch.setattr(logger, "level", logging.NOTSET)
    monkeypatch.setattr(logger, "propagate", True)
    configure_trace_logging()
    stream = StringIO()
    logger.handlers[0].setStream(stream)
    return stream


async def test_relay_preserves_trace_id_in_serialized_kafka_value(fake_uow):
    await fake_uow.outbox.add("ad.created", {"ad_id": 1, "trace_id": TRACE_A})
    sent = []

    async def send_and_wait(topic, value):
        sent.append((topic, json.loads(serialize(value))))

    producer = Mock(send_and_wait=AsyncMock(side_effect=send_and_wait))
    relay = OutboxRelay(lambda: fake_uow, KafkaMessageBroker(producer, "ads"))

    assert await relay._process_batch() == 1
    assert sent == [
        (
            "ads",
            {
                "event": "ad.created",
                "payload": {
                    "ad_id": 1,
                    "trace_id": TRACE_A,
                },
            },
        )
    ]


async def test_relay_reuses_trace_id_after_partial_batch_failure(
    fake_uow, fake_broker, monkeypatch
):
    await fake_uow.outbox.add("ad.created", {"ad_id": 1, "trace_id": TRACE_A})
    await fake_uow.outbox.add("ad.updated", {"ad_id": 2, "trace_id": TRACE_B})
    operations = []
    send = fake_broker.send
    mark = fake_uow.outbox.mark_published
    commit = fake_uow.commit
    fail_once = True

    async def send_with_failure(message):
        nonlocal fail_once
        ad_id = message["payload"]["ad_id"]
        operations.append(("send", ad_id))
        if ad_id == 2 and fail_once:
            fail_once = False
            raise RuntimeError("kafka unavailable")
        await send(message)

    async def mark_published(ids):
        operations.append(("mark", ids))
        await mark(ids)

    async def commit_batch():
        operations.append(("commit", None))
        await commit()

    monkeypatch.setattr(fake_broker, "send", send_with_failure)
    monkeypatch.setattr(fake_uow.outbox, "mark_published", mark_published)
    monkeypatch.setattr(fake_uow, "commit", commit_batch)
    relay = OutboxRelay(lambda: fake_uow, fake_broker)

    with pytest.raises(RuntimeError, match="kafka unavailable"):
        await relay._process_batch()
    assert fake_uow.rolled_back
    assert not fake_uow.committed
    assert [row.id for row in fake_uow.outbox.messages] == [1, 2]
    assert operations == [("send", 1), ("send", 2)]

    assert await relay._process_batch() == 2
    assert operations == [
        ("send", 1),
        ("send", 2),
        ("send", 1),
        ("send", 2),
        ("mark", [1, 2]),
        ("commit", None),
    ]
    assert [value["payload"]["trace_id"] for value in fake_broker.sent] == [
        TRACE_A,
        TRACE_A,
        TRACE_B,
    ]


async def test_relay_generates_uuid_per_legacy_attempt_without_mutating_payload(
    fake_uow, fake_broker, monkeypatch
):
    payload = {"ad_id": 1}
    await fake_uow.outbox.add("ad.created", payload)
    row = fake_uow.outbox.messages[0]
    generated = []
    send = fake_broker.send

    async def send_with_failure(value):
        generated.append(value["payload"].get("trace_id"))
        await send(value)
        raise RuntimeError("retry legacy message")

    monkeypatch.setattr(fake_broker, "send", send_with_failure)
    relay = OutboxRelay(lambda: fake_uow, fake_broker)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="retry legacy message"):
            await relay._process_batch()
        assert row.payload == payload == {"ad_id": 1}
        assert fake_uow.outbox.messages == [row]
        assert not fake_uow.committed

    for value in generated:
        assert value is not None, "Relay must add trace_id to the old message"
        assert UUID(value).version == 4
        assert str(UUID(value)) == value
    assert generated[0] != generated[1]


async def test_relay_isolates_event_context_and_correlates_success_logs(
    fake_uow, fake_broker, monkeypatch, relay_log_output
):
    await fake_uow.outbox.add("ad.created", {"ad_id": 1, "trace_id": TRACE_A})
    await fake_uow.outbox.add("ad.updated", {"ad_id": 2, "trace_id": TRACE_B})
    seen = []
    send = fake_broker.send

    async def observe(value):
        seen.append(get_trace_id())
        await send(value)

    monkeypatch.setattr(fake_broker, "send", observe)
    relay = OutboxRelay(lambda: fake_uow, fake_broker)
    token = set_trace_id(OUTER_TRACE)
    try:
        assert await relay._process_batch() == 2
        assert get_trace_id() == OUTER_TRACE
    finally:
        reset_trace_id(token)

    assert seen == [TRACE_A, TRACE_B]
    lines = relay_log_output.getvalue().splitlines()
    for trace_id in seen:
        assert any(
            line.startswith("INFO ") and f"trace_id={trace_id}" in line
            for line in lines
        ), lines


async def test_relay_logs_failure_in_event_context_and_restores_previous_id(
    fake_uow, fake_broker, monkeypatch, relay_log_output
):
    await fake_uow.outbox.add("ad.created", {"ad_id": 1, "trace_id": TRACE_A})
    send = AsyncMock(side_effect=RuntimeError("kafka unavailable"))
    monkeypatch.setattr(fake_broker, "send", send)
    relay = OutboxRelay(lambda: fake_uow, fake_broker)
    token = set_trace_id(OUTER_TRACE)
    try:
        with pytest.raises(RuntimeError, match="kafka unavailable"):
            await relay._process_batch()
        assert get_trace_id() == OUTER_TRACE
    finally:
        reset_trace_id(token)

    lines = relay_log_output.getvalue().splitlines()
    assert any(
        line.startswith("ERROR ") and f"trace_id={TRACE_A}" in line for line in lines
    ), lines
    assert fake_uow.rolled_back
    assert not fake_uow.committed
    assert len(fake_uow.outbox.messages) == 1


async def test_outbox_selects_pending_rows_in_order_with_postgres_skip_locked():
    result = Mock()
    result.scalars.return_value.all.return_value = []
    session = Mock(execute=AsyncMock(return_value=result))
    repo = SQLAlchemyOutboxRepository(session)

    assert await repo.fetch_unpublished(7) == []
    query = session.execute.await_args.args[0]
    sql = " ".join(
        str(
            query.compile(
                dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
            )
        ).split()
    )
    assert "WHERE outbox.published_at IS NULL ORDER BY outbox.id ASC" in sql
    assert "LIMIT 7 FOR UPDATE SKIP LOCKED" in sql
