from src.application.exceptions import AdNotFoundError, ForbiddenError
from src.application.ports.uow import UnitOfWork
from src.application.ports.usecases import DeleteAdPort
from src.domain.entities import AdStatus


class DeleteAd(DeleteAdPort):
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(
        self, ad_id: int, user_id: int, *, trace_id: str | None = None
    ) -> None:
        async with self._uow as uow:
            ad = await uow.ads.get_by_id(ad_id)
            if ad is None or ad.status == AdStatus.ARCHIVED:
                raise AdNotFoundError
            if ad.user_id != user_id:
                raise ForbiddenError
            ad.archive()
            await self._uow.ads.save(ad)
            payload: dict[str, int | str] = {"ad_id": ad.id}
            if trace_id is not None:
                payload["trace_id"] = trace_id
            await uow.outbox.add("ad.deleted", payload)
            await uow.commit()
