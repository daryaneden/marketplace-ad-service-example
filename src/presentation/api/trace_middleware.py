import logging
from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.trace_context import (
    TRACE_HEADER,
    is_valid_trace_id,
    reset_trace_id,
    set_trace_id,
)

logger = logging.getLogger(__name__)
_TRACE_HEADER_BYTES = TRACE_HEADER.lower().encode("ascii")


class TraceIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        values = [
            value
            for name, value in scope["headers"]
            if name.lower() == _TRACE_HEADER_BYTES
        ]
        trace_id = None
        if len(values) == 1 and len(values[0]) == 36 and values[0].isascii():
            candidate = values[0].decode("ascii")
            if is_valid_trace_id(candidate):
                trace_id = candidate

        source = "incoming"
        if trace_id is None:
            trace_id = str(uuid4())
            source = "generated"

        token = set_trace_id(trace_id)
        status = None

        async def send_with_trace_id(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                message.setdefault("headers", [])
                MutableHeaders(scope=message)[TRACE_HEADER] = trace_id
                await send(message)
                status = message["status"]
                return
            await send(message)

        try:
            await self.app(scope, receive, send_with_trace_id)
        except Exception as exc:
            logger.error(
                "http request failed method=%r path=%r status=%s "
                "trace_source=%s error=%s",
                scope["method"],
                scope["path"],
                status if status is not None else 500,
                source,
                type(exc).__name__,
            )
            raise
        else:
            logger.info(
                "http request method=%r path=%r status=%s trace_source=%s",
                scope["method"],
                scope["path"],
                status,
                source,
            )
        finally:
            reset_trace_id(token)
