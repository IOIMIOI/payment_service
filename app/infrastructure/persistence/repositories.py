"""SQL queries and ORM/domain mapping live at the persistence boundary."""

from copy import deepcopy
from dataclasses import asdict, fields

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import entities
from app.infrastructure.persistence import models


def _entity(entity_type, row):
    if row is None:
        return None
    return entity_type(
        **{field.name: deepcopy(getattr(row, field.name)) for field in fields(entity_type)}
    )


async def _save(session, model, identity, entity):
    row = await session.get(model, identity)
    if row is None:
        raise RuntimeError(f"Cannot update missing {model.__name__}: {identity}")
    for name, value in asdict(entity).items():
        setattr(row, name, value)
    await session.flush()


class Payments:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, payment_id: str, *, lock: bool = False) -> entities.Payment | None:
        query = select(models.Payment).where(models.Payment.id == payment_id)
        if lock:
            query = query.with_for_update()
        return _entity(entities.Payment, await self.session.scalar(query))

    async def by_key(self, key: str) -> entities.Payment | None:
        row = await self.session.scalar(
            select(models.Payment).where(models.Payment.idempotency_key == key)
        )
        return _entity(entities.Payment, row)

    async def add(self, payment: entities.Payment) -> None:
        self.session.add(models.Payment(**asdict(payment)))
        # Insert before dependent work/outbox rows, including under concurrent keys.
        await self.session.flush()

    async def save(self, payment: entities.Payment) -> None:
        await _save(self.session, models.Payment, payment.id, payment)


class WorkItems:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, event_id: str) -> entities.WorkItem | None:
        row = await self.session.scalar(
            select(models.WorkItem).where(models.WorkItem.event_id == event_id).with_for_update()
        )
        return _entity(entities.WorkItem, row)

    async def webhook_for(self, payment_id: str) -> entities.WorkItem | None:
        row = await self.session.scalar(
            select(models.WorkItem).where(
                models.WorkItem.payment_id == payment_id,
                models.WorkItem.kind == "webhook",
            )
        )
        return _entity(entities.WorkItem, row)

    async def dead(self) -> list[entities.WorkItem]:
        rows = await self.session.scalars(
            select(models.WorkItem)
            .where(
                models.WorkItem.state == "dead",
            )
            .order_by(models.WorkItem.created_at)
        )
        return [_entity(entities.WorkItem, row) for row in rows]

    async def add(self, item: entities.WorkItem) -> None:
        self.session.add(models.WorkItem(**asdict(item)))
        await self.session.flush()

    async def save(self, item: entities.WorkItem) -> None:
        await _save(self.session, models.WorkItem, item.event_id, item)


class Events:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, event: entities.Outbox) -> None:
        self.session.add(models.Outbox(**asdict(event)))
        await self.session.flush()

    async def pending(self, limit: int) -> list[entities.Outbox]:
        rows = await self.session.scalars(
            select(models.Outbox)
            .where(models.Outbox.published_at.is_(None))
            .order_by(models.Outbox.created_at, models.Outbox.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [_entity(entities.Outbox, row) for row in rows]

    async def save(self, event: entities.Outbox) -> None:
        await _save(self.session, models.Outbox, event.id, event)


class ProviderOperations:
    """Storage owned by the emulator, outside the payment application's UoW."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, key: str) -> models.ProviderOperation | None:
        return await self.session.get(models.ProviderOperation, key)

    async def add(self, operation: models.ProviderOperation) -> None:
        self.session.add(operation)
        await self.session.flush()
