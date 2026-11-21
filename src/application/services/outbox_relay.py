import asyncio
import logging
from typing import Callable
from uuid import uuid4

from src.application.ports.message_broker import MessageBroker
from src.application.ports.outbox import OutboxMessage
from src.application.ports.uow import UnitOfWork
from src.trace_context import is_valid_trace_id, reset_trace_id, set_trace_id

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        broker: MessageBroker,
        batch_size: int = 100,
        idle_sleep: float = 1.0,
    ) -> None:
        self._uow_factory = uow_factory
        self._broker = broker
        self._batch_size = batch_size
        self._idle_sleep = idle_sleep

    async def run(self) -> None:
        while True:
            try:
                published = await self._process_batch()
            except Exception as exc:
                logger.error("outbox relay batch failed: %s", type(exc).__name__)
                await asyncio.sleep(self._idle_sleep)
                continue
            if published == 0:
                await asyncio.sleep(self._idle_sleep)

    async def _process_batch(self) -> int:
        async with self._uow_factory() as uow:
            messages = await uow.outbox.fetch_unpublished(self._batch_size)
            if not messages:
                return 0

            for message in messages:
                await self._publish(message)

            await uow.outbox.mark_published([m.id for m in messages])
            await uow.commit()
            logger.info("relayed %d outbox messages", len(messages))
            return len(messages)

    async def _publish(self, message: OutboxMessage) -> None:
        trace_id = message.payload.get("trace_id")
        if not isinstance(trace_id, str) or not is_valid_trace_id(trace_id):
            trace_id = str(uuid4())
        payload = {**message.payload, "trace_id": trace_id}

        token = set_trace_id(trace_id)
        try:
            await self._broker.send({"event": message.event_type, "payload": payload})
        except Exception as exc:
            logger.error(
                "outbox publish failed outbox_id=%s error=%s",
                message.id,
                type(exc).__name__,
            )
            raise
        else:
            logger.info("outbox published outbox_id=%s", message.id)
        finally:
            reset_trace_id(token)
