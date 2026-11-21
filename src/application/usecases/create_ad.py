from src.application.ports.uow import UnitOfWork
from src.application.ports.usecases import CreateAdPort
from src.domain.entities import Ad


class CreateAd(CreateAdPort):
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(
        self,
        user_id: int,
        title: str,
        description: str,
        price: int,
        category: str,
        city: str,
        *,
        trace_id: str | None = None,
    ) -> Ad:
        async with self._uow:
            ad = await self._uow.ads.create(
                user_id=user_id,
                title=title,
                description=description,
                price=price,
                category=category,
                city=city,
            )
            payload: dict[str, int | str] = {"ad_id": ad.id}
            if trace_id is not None:
                payload["trace_id"] = trace_id
            await self._uow.outbox.add("ad.created", payload)
            await self._uow.commit()
        return ad
