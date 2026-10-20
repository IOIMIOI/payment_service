"""Recovery uses the same state rules and transaction boundary as processing."""

from app.application.ports import UowFactory


async def list_dead(uow_factory: UowFactory) -> list[dict]:
    async with uow_factory() as uow:
        items = await uow.work_items.dead()
        return [
            {
                "event_id": item.event_id,
                "payment_id": item.payment_id,
                "stage": item.kind,
                "cycle": item.cycle,
                "attempts": item.attempts,
                "last_error": item.last_error,
            }
            for item in items
        ]


async def replay(event_id: str, uow_factory: UowFactory) -> dict:
    async with uow_factory() as uow:
        item = await uow.work_items.get(event_id)
        if item is None:
            raise ValueError(f"Событие {event_id} не найдено")
        item.replay()
        await uow.work_items.save(item)
        await uow.outbox.add(item.event())
        return {"event_id": item.event_id, "stage": item.kind, "cycle": item.cycle}
