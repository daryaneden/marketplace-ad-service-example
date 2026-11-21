from inspect import Parameter, signature

import pytest

from src.application.usecases.create_ad import CreateAd
from src.application.usecases.delete_ad import DeleteAd
from src.application.usecases.update_ad import UpdateAd
from tests.conftest import FakeUnitOfWork

TRACE_ID = "550e8400-e29b-41d4-a716-446655440000"
AD_FIELDS = {
    "title": "Test ad",
    "description": "Test description",
    "price": 100,
    "category": "test",
    "city": "test",
}


@pytest.mark.parametrize("action", ["create", "update", "delete"])
@pytest.mark.parametrize("with_trace", [True, False], ids=["traced", "legacy"])
async def test_mutation_writes_outbox_payload(
    fake_uow: FakeUnitOfWork, action, with_trace
):
    if action == "create":
        execute = CreateAd(fake_uow).execute
        kwargs = {"user_id": 1, **AD_FIELDS}
    else:
        ad = await fake_uow.ads.create(user_id=1, **AD_FIELDS)
        kwargs = {"ad_id": ad.id, "user_id": 1}
        if action == "update":
            execute = UpdateAd(fake_uow).execute
            kwargs.update({**AD_FIELDS, "title": "Updated"})
        else:
            execute = DeleteAd(fake_uow).execute

    expected = {"ad_id": 1}
    if with_trace:
        parameter = signature(execute).parameters.get("trace_id")
        assert parameter is not None, f"{action} must accept trace_id"
        assert parameter.kind == Parameter.KEYWORD_ONLY
        kwargs["trace_id"] = TRACE_ID
        expected["trace_id"] = TRACE_ID

    await execute(**kwargs)

    assert fake_uow.committed
    (message,) = fake_uow.outbox.messages
    assert message.event_type == f"ad.{action}d"
    assert message.payload == expected
