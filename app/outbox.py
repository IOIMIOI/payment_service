"""Публикация outbox с подтверждением RabbitMQ; безопасна при повторной отправке."""

import asyncio
import logging

from sqlalchemy import select

from app.broker import QUEUES, create_broker, declare_topology
from app.config import get_settings
from app.db import Session, engine
from app.models import Outbox, utcnow

logger = logging.getLogger(__name__)


async def publish_once(session_factory, broker, *, batch_size: int = 100) -> int:
    async with session_factory() as session, session.begin():
        rows = (
            await session.scalars(
                select(Outbox)
                .where(Outbox.published_at.is_(None))
                .order_by(Outbox.created_at, Outbox.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
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
        return len(rows)


async def run() -> None:
    settings = get_settings()
    try:
        while True:
            try:
                async with create_broker() as broker:
                    await declare_topology(broker)
                    while True:
                        await publish_once(Session, broker)
                        await asyncio.sleep(settings.outbox_poll_interval)
            except Exception:
                # При недоступном брокере транзакция откатывается; события остаются
                # в outbox и автоматически отправятся после восстановления связи.
                logger.exception("Outbox publication failed; retrying")
                await asyncio.sleep(max(1.0, settings.outbox_poll_interval))
    finally:
        await engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
