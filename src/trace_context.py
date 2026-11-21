import logging
import re
import sys
from contextvars import ContextVar, Token
from copy import copy
from uuid import UUID

TRACE_HEADER = "X-Trace-Id"
_UUID_PATTERN = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)
_HANDLER_NAME = "marketplace.trace"


def get_trace_id() -> str | None:
    return _trace_id.get()


def set_trace_id(trace_id: str) -> Token[str | None]:
    return _trace_id.set(trace_id)


def reset_trace_id(token: Token[str | None]) -> None:
    _trace_id.reset(token)


def is_valid_trace_id(value: str) -> bool:
    return (
        len(value) == 36
        and _UUID_PATTERN.fullmatch(value) is not None
        and str(UUID(value)) == value
    )


class _TraceIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = get_trace_id() or "-"
        return True


def configure_trace_logging() -> None:
    logger = logging.getLogger("src")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if any(handler.name == _HANDLER_NAME for handler in logger.handlers):
        return

    handler = logging.StreamHandler()
    handler.name = _HANDLER_NAME
    handler.addFilter(_TraceIdFilter())
    handler.setFormatter(
        logging.Formatter("%(levelname)s %(name)s trace_id=%(trace_id)s %(message)s")
    )
    logger.addHandler(handler)


class _UvicornASGIErrorFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> logging.LogRecord:
        # The trace middleware already logs the exception type and request ID.
        if (
            record.name != "uvicorn.error"
            or record.msg != "Exception in ASGI application\n"
        ):
            return record

        # Replace only the console handler's record. Driver details can contain data.
        safe_record = copy(record)
        safe_record.exc_info = None
        safe_record.exc_text = None
        return safe_record


def configure_uvicorn_logging() -> None:
    # All API entrypoints import bin.api after Uvicorn configures its loggers.
    logging.getLogger("uvicorn.access").disabled = True

    logger = logging.getLogger("uvicorn.error")
    while logger is not None:
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler) and handler.stream in (
                sys.stdout,
                sys.stderr,
            ):
                if not any(
                    isinstance(item, _UvicornASGIErrorFilter)
                    for item in handler.filters
                ):
                    handler.addFilter(_UvicornASGIErrorFilter())
        if not logger.propagate:
            break
        logger = logger.parent
