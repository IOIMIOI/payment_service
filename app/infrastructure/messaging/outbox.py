"""Publish with confirms while holding the outbox transaction and row locks."""

from app.infrastructure.messaging.broker import QUEUES
from app.infrastructure.persistence.uow import SqlAlchemyUnitOfWork
from app.shared.time import utcnow


async def publish_once(session_factory, broker, *, batch_size: int = 100) -> int:
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        rows = await uow.outbox.pending(batch_size)
        for row in rows:
            await broker.publish(
                {"event_id": row.event_id, "cycle": row.cycle},
                queue=QUEUES[row.destination],
                message_id=row.event_id,
                correlation_id=row.event_id,
                persist=True,
                mandatory=True,
                timeout=10,
            )
            # Между broker confirm и COMMIT возможен сбой. Мы намеренно допускаем
            # повтор с прежним event_id: исключить его общей транзакцией Rabbit/SQL
            # нельзя, поэтому безопасную обработку гарантирует идемпотентный consumer.
            row.published_at = utcnow()
            await uow.outbox.save(row)
        return len(rows)
