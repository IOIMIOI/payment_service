from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import TestRabbitBroker as RabbitTestBroker

from app import consumer
from app.broker import PAYMENT_QUEUE, QUEUES, WEBHOOK_QUEUE


@pytest.mark.parametrize("queue_name", [PAYMENT_QUEUE, WEBHOOK_QUEUE])
async def test_registered_subscriptions_dispatch_same_processor(monkeypatch, queue_name):
    dispatch = AsyncMock()
    monkeypatch.setattr(consumer, "processor", SimpleNamespace(handle=dispatch))
    event_id = str(uuid4())

    # Настоящая регистрация FastStream, сериализация и обработчик; соединение
    # RabbitMQ заменено TestRabbitBroker, поэтому локальный брокер не нужен.
    async with RabbitTestBroker(consumer.broker) as broker:
        await broker.publish({"event_id": event_id, "cycle": 2}, queue=QUEUES[queue_name])

    dispatch.assert_awaited_once_with(event_id, 2)


async def test_infrastructure_error_requests_requeue(monkeypatch):
    dispatch = AsyncMock(side_effect=ConnectionError("database unavailable"))
    pause = AsyncMock()
    monkeypatch.setattr(consumer, "processor", SimpleNamespace(handle=dispatch))
    monkeypatch.setattr(consumer.asyncio, "sleep", pause)
    event_id = str(uuid4())
    with pytest.raises(NackMessage) as raised:
        await consumer.handle({"event_id": event_id, "cycle": 0})
    assert raised.value.extra_options == {"requeue": True}
    pause.assert_awaited_once_with(1)
    dispatch.assert_awaited_once_with(event_id, 0)


@pytest.mark.parametrize("body", [{"event_id": "x"}, {"event_id": "x", "cycle": -1}])
async def test_invalid_envelope_is_rejected_before_processor(monkeypatch, body):
    dispatch = AsyncMock()
    monkeypatch.setattr(consumer, "processor", SimpleNamespace(handle=dispatch))
    with pytest.raises(RejectMessage) as raised:
        await consumer.handle(body)
    assert raised.value.extra_options == {"requeue": False}
    dispatch.assert_not_awaited()
