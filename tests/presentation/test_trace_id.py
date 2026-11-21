import asyncio
import logging
import runpy
from collections import Counter
from contextvars import copy_context
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, MockTransport, Request, Response

from src.fastapi import create_app
from src.infrastructure.http.auth_client import AuthServiceUserProfileService
from src.presentation.api.dependencies import (
    get_create_ad,
    get_delete_ad,
    get_update_ad,
    get_user_profile_service,
)
from src.presentation.api.trace_middleware import TraceIdMiddleware
from src.trace_context import get_trace_id, reset_trace_id, set_trace_id

TRACE_HEADER = "X-Trace-Id"
TRACE_A = "550e8400-e29b-41d4-a716-446655440000"
TRACE_B = "8b738d87-25b4-43bd-9d6b-bbb2c07939ad"
OUTER_TRACE = "ee777944-bb03-4db9-bc67-277b4d7f4973"
probe_logger = logging.getLogger("src.trace_test")


@pytest.fixture
def traced_app(app: FastAPI) -> FastAPI:
    application = create_app()
    application.dependency_overrides.update(app.dependency_overrides)
    return application


@pytest.fixture
async def trace_client(traced_app: FastAPI):
    transport = ASGITransport(app=traced_app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def configure_api_logging(monkeypatch):
    # Isolate the application logger and restore its process-wide state.
    app_logger = logging.getLogger("src")
    monkeypatch.setattr(app_logger, "handlers", [])
    monkeypatch.setattr(app_logger, "level", logging.NOTSET)
    monkeypatch.setattr(app_logger, "propagate", True)

    def configure():
        # Exercise the import branch of bin.api:app without starting Uvicorn.
        return runpy.run_module("bin.api", run_name="trace_test_api")["app"]

    return configure


def response_trace(response):
    values = response.headers.get_list(TRACE_HEADER)
    assert len(values) == 1, f"Expected one {TRACE_HEADER}, got {values!r}"
    return values[0]


def assert_uuid4(value):
    parsed = UUID(value)
    assert parsed.version == 4
    assert str(parsed) == value


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("/openapi.json", 200),
        ("/__trace-test/missing", 404),
        ("/ads?limit=0", 422),
    ],
    ids=["normal", "not-found", "validation-error"],
)
async def test_preserves_valid_trace_id(trace_client, path, status):
    response = await trace_client.get(path, headers={"x-TrAcE-iD": TRACE_A})

    assert response.status_code == status, response.text
    assert response_trace(response) == TRACE_A


async def test_requests_without_trace_id_get_distinct_uuids(trace_client):
    first = await trace_client.get("/openapi.json")
    second = await trace_client.get("/openapi.json")

    assert first.status_code == second.status_code == 200
    first_id, second_id = response_trace(first), response_trace(second)
    assert_uuid4(first_id)
    assert_uuid4(second_id)
    assert first_id != second_id


@pytest.mark.parametrize(
    "values",
    [
        [b""],
        [b"not-a-uuid"],
        [TRACE_A.upper().encode()],
        [TRACE_A.replace("e", "E", 1).encode()],
        [b"\xff" + TRACE_A.encode()[1:]],
        [TRACE_A.encode() + b"a" * 1024],
        [TRACE_A.encode(), TRACE_A.encode()],
        [TRACE_A.encode(), TRACE_B.encode()],
        [f"{TRACE_A}, {TRACE_B}".encode()],
    ],
    ids=[
        "empty",
        "malformed",
        "uppercase",
        "mixed-case",
        "non-ascii",
        "too-long",
        "identical-duplicates",
        "different-duplicates",
        "comma-joined",
    ],
)
async def test_replaces_invalid_trace_header(trace_client, values):
    names = (b"x-trace-id", b"X-Trace-ID")
    response = await trace_client.get("/openapi.json", headers=list(zip(names, values)))

    assert response.status_code == 200
    generated = response_trace(response)
    assert_uuid4(generated)
    assert generated.encode() not in values
    assert generated not in {TRACE_A, TRACE_B}


async def test_unhandled_error_logs_trace_id(
    traced_app, trace_client, configure_api_logging, capsys
):
    configure_api_logging()

    @traced_app.get("/__trace-test/crash")
    async def crash():
        raise RuntimeError("trace-test-unhandled")

    response = await trace_client.get(
        "/__trace-test/crash", headers={TRACE_HEADER: TRACE_A}
    )
    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert response.status_code == 500
    assert TRACE_HEADER not in response.headers
    assert any(
        TRACE_A in line and ("ERROR" in line or "trace-test-unhandled" in line)
        for line in output.splitlines()
    ), f"Expected correlated application error in application output, got {output!r}"


async def test_overlapping_requests_keep_separate_contexts(traced_app, trace_client):
    entered = asyncio.Event()
    release = asyncio.Event()
    before = {}
    after = {}

    @traced_app.get("/__trace-test/overlap/{index}")
    async def overlap(index: int):
        before[index] = tuple(copy_context().values())
        if len(before) == 2:
            entered.set()
        await release.wait()
        after[index] = tuple(copy_context().values())
        return {"index": index}

    async with asyncio.timeout(5), asyncio.TaskGroup() as tasks:
        first = tasks.create_task(
            trace_client.get("/__trace-test/overlap/1", headers={TRACE_HEADER: TRACE_A})
        )
        second = tasks.create_task(
            trace_client.get("/__trace-test/overlap/2", headers={TRACE_HEADER: TRACE_B})
        )
        await entered.wait()
        release.set()

    for index, trace_id, other_id, task in (
        (1, TRACE_A, TRACE_B, first),
        (2, TRACE_B, TRACE_A, second),
    ):
        assert task.result().status_code == 200
        assert trace_id in before[index] and trace_id in after[index], (
            f"Request {index} lacks its own trace_id in the task context"
        )
        assert other_id not in before[index] and other_id not in after[index]
        assert response_trace(task.result()) == trace_id


@pytest.mark.parametrize("outcome", ["success", "exception", "cancelled"])
async def test_restores_previous_context_in_same_task(
    traced_app, trace_client, outcome
):
    observed = []
    active_task = asyncio.current_task()

    @traced_app.get("/__trace-test/context")
    async def context(result: str = "success"):
        observed.append((asyncio.current_task(), copy_context()))
        if result == "cancelled":
            raise asyncio.CancelledError
        if result == "exception":
            raise RuntimeError("trace-test-context")
        return {"ok": True}

    original = copy_context()
    discovery = await trace_client.get(
        "/__trace-test/context", headers={TRACE_HEADER: TRACE_A}
    )
    assert discovery.status_code == 200
    variables = [var for var, value in observed[0][1].items() if value == TRACE_A]
    assert len(variables) == 1, "The real HTTP app must set a ContextVar for trace_id"
    variable = variables[0]
    assert variable.get(None) == original.get(variable)

    token = variable.set(OUTER_TRACE)
    try:
        request = trace_client.get(
            "/__trace-test/context",
            params={"result": outcome},
            headers={TRACE_HEADER: TRACE_B},
        )
        if outcome == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await request
        else:
            response = await request
            assert response.status_code == (500 if outcome == "exception" else 200)
        assert observed[0][0] is observed[1][0] is active_task
        assert observed[1][1][variable] == TRACE_B
        assert variable.get() == OUTER_TRACE
    finally:
        variable.reset(token)


def test_api_logging_setup_is_idempotent_and_logs_outside_operation(
    configure_api_logging, capsys
):
    first = configure_api_logging()
    app_logger = logging.getLogger("src")
    handlers = tuple(app_logger.handlers)

    second = configure_api_logging()
    assert first is not second
    probe_logger.info("trace-test-outside-operation")
    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert len(handlers) == 1, "API startup must configure one application handler"
    assert tuple(app_logger.handlers) == handlers
    lines = [
        line for line in output.splitlines() if "trace-test-outside-operation" in line
    ]
    assert len(lines) == 1
    assert "trace_id=-" in lines[0]


async def test_application_log_in_request_has_trace_id(
    traced_app, trace_client, configure_api_logging, capsys
):
    configure_api_logging()

    @traced_app.get("/__trace-test/log")
    async def log():
        probe_logger.info("trace-test-inside-operation")
        return {"ok": True}

    response = await trace_client.get(
        "/__trace-test/log?secret=trace-test-query-secret",
        headers={TRACE_HEADER: TRACE_A},
    )
    captured = capsys.readouterr()
    output = captured.out + captured.err
    lines = [
        line for line in output.splitlines() if "trace-test-inside-operation" in line
    ]

    assert response.status_code == 200
    assert len(lines) == 1, f"Expected one application log line, got {lines!r}"
    assert f"trace_id={TRACE_A}" in lines[0]
    request_lines = [
        line for line in output.splitlines() if "path='/__trace-test/log'" in line
    ]
    assert len(request_lines) == 1
    assert "method='GET'" in request_lines[0]
    assert "status=200" in request_lines[0]
    assert "trace_source=incoming" in request_lines[0]
    assert "trace-test-query-secret" not in output


@pytest.mark.parametrize("fanout", [False, True], ids=["detail", "parallel-list"])
async def test_ad_sends_trace_header_to_auth(
    traced_app, trace_client, fake_uow, fanout
):
    for user_id in (1, 2):
        await fake_uow.ads.create(
            user_id=user_id,
            title=f"Ad {user_id}",
            description="Test ad",
            price=123,
            category="test",
            city="test",
        )
    requested = []
    both_started = asyncio.Event()

    async def auth_response(request: Request) -> Response:
        requested.append((request.url.path, request.headers.get_list(TRACE_HEADER)))
        if fanout:
            if len(requested) == 2:
                both_started.set()
            await both_started.wait()
        user_id = int(request.url.path.rsplit("/", 1)[-1])
        return Response(200, json={"user_id": user_id, "name": f"User {user_id}"})

    async with AsyncClient(transport=MockTransport(auth_response)) as upstream:
        original_headers = upstream.headers.copy()
        adapter = AuthServiceUserProfileService(upstream, "http://auth")
        traced_app.dependency_overrides[get_user_profile_service] = lambda: adapter
        async with asyncio.timeout(5):
            response = await trace_client.get(
                "/ads" if fanout else "/ads/1",
                headers={TRACE_HEADER: TRACE_A},
            )

        assert upstream.headers == original_headers
        assert await adapter.user(1) is not None

    assert requested.pop() == ("/internal/users/1", [])
    assert response.status_code == 200, response.text
    expected = (
        ["/internal/users/1", "/internal/users/2"] if fanout else ["/internal/users/1"]
    )
    assert Counter(path for path, _ in requested) == Counter(expected)
    assert all(values == [TRACE_A] for _, values in requested), requested


@pytest.mark.parametrize(
    ("method", "path", "dependency", "status"),
    [
        ("POST", "/ads", get_create_ad, 201),
        ("PUT", "/ads/1", get_update_ad, 200),
        ("DELETE", "/ads/1", get_delete_ad, 204),
    ],
    ids=["create", "update", "delete"],
)
async def test_ad_mutation_passes_trace_id_by_keyword(
    traced_app, trace_client, auth_headers, fake_uow, method, path, dependency, status
):
    data = {
        "title": "Ad",
        "description": "Test ad",
        "price": 123,
        "category": "test",
        "city": "test",
    }
    ad = await fake_uow.ads.create(user_id=1, **data)
    execute = AsyncMock(return_value=ad)
    traced_app.dependency_overrides[dependency] = lambda: SimpleNamespace(
        execute=execute
    )

    response = await trace_client.request(
        method,
        path,
        headers={**auth_headers, TRACE_HEADER: TRACE_A},
        json=data if method != "DELETE" else None,
    )

    assert response.status_code == status, response.text
    assert response_trace(response) == TRACE_A
    execute.assert_awaited_once()
    assert execute.await_args.kwargs.get("trace_id") == TRACE_A


@pytest.mark.parametrize("include_headers", [False, True], ids=["minimal", "ordinary"])
async def test_asgi_response_start_supports_optional_headers(include_headers):
    async def app(scope, receive, send):
        assert get_trace_id() == TRACE_A
        start = {"type": "http.response.start", "status": 204}
        if include_headers:
            start["headers"] = [
                (b"x-trace-id", b"old"),
                (b"x-trace-id", b"duplicate"),
                (b"x-test", b"preserved"),
            ]
        await send(start)
        await send({"type": "http.response.body", "body": b""})

    token = set_trace_id(OUTER_TRACE)
    try:
        transport = ASGITransport(app=TraceIdMiddleware(app))
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/", headers={TRACE_HEADER: TRACE_A})
        assert get_trace_id() == OUTER_TRACE
    finally:
        reset_trace_id(token)

    assert response.status_code == 204
    assert response.content == b""
    assert response_trace(response) == TRACE_A
    if include_headers:
        assert response.headers["x-test"] == "preserved"
