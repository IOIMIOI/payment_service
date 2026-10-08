"""Один процесс consumer; оба этапа используют один Processor."""

import asyncio
import logging

from faststream import FastStream
from faststream.exceptions import NackMessage, RejectMessage
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.broker import PAYMENT_QUEUE, QUEUES, WEBHOOK_QUEUE, create_broker, declare_topology
from app.db import Session, engine
from app.provider import HTTPProvider
from app.webhook import HTTPWebhook
from app.worker import Processor

logger = logging.getLogger(__name__)


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    event_id: str = Field(min_length=1, max_length=36)
    cycle: int = Field(ge=0)


broker = create_broker()
processor = Processor(Session, engine, HTTPProvider(), HTTPWebhook())
app = FastStream(broker)


@broker.subscriber(QUEUES[PAYMENT_QUEUE])
@broker.subscriber(QUEUES[WEBHOOK_QUEUE])
async def handle(body: dict) -> None:
    try:
        event = Event.model_validate(body)
    except ValidationError as exc:
        logger.error("Invalid event; moving to transport DLQ: %s", exc)
        raise RejectMessage(requeue=False) from exc
    try:
        # Дополнение к исходному «обработчику, делающему всё»: логические этапы
        # разделены, чтобы ошибка webhook не запускала повторную оплату.
        # ACK произойдёт после фиксации результата/состояния DLQ в БД.
        await processor.handle(event.event_id, event.cycle)
    except Exception as exc:
        # Сбой БД не является отказом провайдера и не должен терять сообщение.
        # Бизнес-попытки и их лимит хранит Processor; AMQP redelivery их не сбрасывает.
        logger.exception("Consumer infrastructure failure; requeue event %s", event.event_id)
        await asyncio.sleep(1)
        raise NackMessage(requeue=True) from exc


@app.on_startup
async def prepare_topology() -> None:
    await broker.connect()
    await declare_topology(broker)


@app.after_shutdown
async def dispose_database() -> None:
    await engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
