"""Публикация outbox с подтверждением RabbitMQ; безопасна при повторной отправке."""

import asyncio
import logging

from app.config import get_settings
from app.infrastructure.messaging.broker import create_broker, declare_topology
from app.infrastructure.messaging.outbox import publish_once
from app.infrastructure.persistence.db import Session, engine

logger = logging.getLogger(__name__)


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
