"""Opt-in проверки реальных PostgreSQL/RabbitMQ в собственных schema/queue.

Запуск: TEST_DATABASE_URL=... TEST_RABBITMQ_URL=... pytest -m integration.
Общие таблицы приложения и рабочие очереди эти тесты не очищают.
"""

import asyncio
import json
import os
import re
from functools import partial
from uuid import uuid4

import aio_pika
import httpx
import pytest
import pytest_asyncio
from faststream.rabbit import RabbitQueue
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.application.payments import PaymentService
from app.application.processor import EventBusy, Processor
from app.bootstrap import get_payment_service
from app.config import settings
from app.emulator.provider import Emulator
from app.entrypoints.api.routes import app
from app.infrastructure.messaging.broker import PAYMENT_QUEUE, QUEUES, create_broker
from app.infrastructure.messaging.outbox import publish_once
from app.infrastructure.persistence.db import event_lock
from app.infrastructure.persistence.models import Base, Outbox, Payment, ProviderOperation, WorkItem
from app.infrastructure.persistence.uow import SqlAlchemyUnitOfWork
from app.shared.json import canonical_json, decode_json

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def postgres():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to run real PostgreSQL checks")
    if not url.startswith("postgresql+asyncpg://"):
        pytest.fail("TEST_DATABASE_URL must use postgresql+asyncpg")

    # Имя генерируется внутри теста и не берётся из внешней конфигурации.
    # DROP ниже удаляет только эту новую схему, никогда public/таблицы приложения.
    schema = "test_" + uuid4().hex
    assert re.fullmatch(r"test_[0-9a-f]{32}", schema)
    admin = create_async_engine(url)
    engines = []
    created = False
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        created = True
        for _ in range(2):
            engines.append(
                create_async_engine(
                    url,
                    connect_args={"server_settings": {"search_path": schema}},
                    json_serializer=canonical_json,
                    json_deserializer=decode_json,
                )
            )
        async with engines[0].begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        factories = [async_sessionmaker(engine, expire_on_commit=False) for engine in engines]
        yield engines, factories
    finally:
        for engine in engines:
            await engine.dispose()
        if created:
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest_asyncio.fixture
async def postgres_api(postgres):
    _, factories = postgres

    def service():
        return PaymentService(partial(SqlAlchemyUnitOfWork, factories[0]))

    original = dict(app.dependency_overrides)
    app.dependency_overrides[get_payment_service] = service
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://integration.test",
            headers={"X-API-Key": settings.api_key, "Idempotency-Key": uuid4().hex},
        ) as client:
            yield client
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(original)


def request_body():
    return {
        "amount": "100.00",
        "currency": "RUB",
        "description": "integration test",
        "metadata": {"nested": {"b": 2, "a": 1}},
        "webhook_url": "http://webhook-sink:8002/hook",
    }


@pytest.mark.parametrize("character", ["\x00", "\ud800", "\udfff"])
@pytest.mark.parametrize(
    "location", ["description", "metadata_value", "metadata_key", "unknown_key"]
)
async def test_postgres_rejects_unsupported_text_with_422(
    postgres,
    postgres_api,
    character,
    location,
):
    body = request_body()
    if location == "description":
        body["description"] = "hello" + character + "world"
    elif location == "metadata_value":
        body["metadata"] = {"nested": [{"value": "hello" + character}]}
    elif location == "metadata_key":
        body["metadata"] = {"nested": [{"bad" + character: "value"}]}
    else:
        body["unknown" + character] = "bad key in validation error"

    # ensure_ascii доставляет именно JSON escape, а не ошибку UTF-8 в HTTP-клиенте.
    response = await postgres_api.post(
        "/api/v1/payments",
        content=json.dumps(body, ensure_ascii=True),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json()["detail"]
    _, factories = postgres
    async with factories[1]() as session:
        for model in (Payment, WorkItem, Outbox):
            assert await session.scalar(select(func.count()).select_from(model)) == 0

    # Отклонённый запрос не занимает ключ: исправленный создаёт единственный платёж.
    valid = await postgres_api.post("/api/v1/payments", json=request_body())
    assert valid.status_code == 202


async def test_postgres_accepts_valid_unicode_and_surrogate_pairs(postgres_api):
    body = request_body()
    text = "Оплата \U0001f600 e\u0301\n\t"
    body["description"] = text
    body["metadata"] = {"\U0001f600": [text]}
    # В JSON emoji представлено корректной парой surrogate escapes.
    response = await postgres_api.post(
        "/api/v1/payments",
        content=json.dumps(body, ensure_ascii=True),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 202
    replay = await postgres_api.post("/api/v1/payments", json=body)
    assert replay.status_code == 200
    assert replay.json()["payment_id"] == response.json()["payment_id"]
    detail = await postgres_api.get("/api/v1/payments/" + response.json()["payment_id"])
    assert detail.status_code == 200
    assert detail.json()["description"] == text
    assert detail.json()["metadata"] == body["metadata"]


async def test_postgres_rejects_nul_in_idempotency_header_and_payment_path(postgres, postgres_api):
    response = await postgres_api.post(
        "/api/v1/payments",
        json=request_body(),
        headers={"Idempotency-Key": "bad\x00key"},
    )
    assert response.status_code == 422
    response = await postgres_api.get("/api/v1/payments/bad%00id")
    assert response.status_code == 422
    _, factories = postgres
    async with factories[1]() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0


async def test_postgres_concurrent_idempotency_constraint(postgres, postgres_api):
    responses = await asyncio.gather(
        *(postgres_api.post("/api/v1/payments", json=request_body()) for _ in range(8))
    )
    assert sorted(response.status_code for response in responses) == [200] * 7 + [202]
    assert len({response.json()["payment_id"] for response in responses}) == 1
    _, factories = postgres
    async with factories[1]() as session:
        for model in (Payment, WorkItem, Outbox):
            assert await session.scalar(select(func.count()).select_from(model)) == 1


async def test_postgres_provider_lock_works_across_engines(postgres):
    engines, factories = postgres
    invoked = []

    async def operation_delay(delay):
        await asyncio.sleep(0.05)

    def outcome():
        invoked.append(1)
        return 0.1

    services = [
        Emulator(factory, engine, sleep=operation_delay, random_value=outcome)
        for engine, factory in zip(engines, factories, strict=True)
    ]
    key = "integration-" + uuid4().hex
    responses = await asyncio.gather(
        *(services[index % 2].charge(key, "100.00", "RUB") for index in range(8))
    )
    assert len(invoked) == 1
    assert all(response == responses[0] for response in responses)
    async with factories[1]() as session:
        assert await session.scalar(select(func.count()).select_from(ProviderOperation)) == 1


async def test_postgres_worker_lock_prevents_concurrent_duplicate_charge(postgres, postgres_api):
    response = await postgres_api.post("/api/v1/payments", json=request_body())
    assert response.status_code == 202
    engines, factories = postgres
    async with factories[0]() as session:
        event_id = (await session.scalars(select(WorkItem))).one().event_id

    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    class SlowProvider:
        async def charge(self, key, amount, currency):
            calls.append(key)
            started.set()
            await release.wait()
            return {"status": "succeeded", "processed_at": "2026-10-08T12:00:00+00:00"}

    class NoWebhookAtThisStage:
        async def send(self, url, payload):
            pytest.fail("Payment event must schedule, not execute, the webhook stage")

    provider = SlowProvider()
    processors = [
        Processor(
            partial(SqlAlchemyUnitOfWork, factory),
            partial(event_lock, engine),
            provider,
            NoWebhookAtThisStage(),
        )
        for engine, factory in zip(engines, factories, strict=True)
    ]
    first = asyncio.create_task(processors[0].handle(event_id, 0))
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        with pytest.raises(EventBusy):
            await asyncio.wait_for(processors[1].handle(event_id, 0), timeout=5)
    finally:
        release.set()
        await asyncio.wait_for(first, timeout=5)

    # Повторная доставка после завершения возвращается без нового вызова оплаты.
    await processors[1].handle(event_id, 0)
    assert len(calls) == 1
    async with factories[1]() as session:
        payment = (await session.scalars(select(Payment))).one()
        original = await session.get(WorkItem, event_id)
        assert payment.status == "succeeded"
        assert original.state == "done" and original.attempts == 1
        assert await session.scalar(select(func.count()).select_from(WorkItem)) == 2
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 2


async def test_rabbit_outbox_confirm_gap_republishes_same_identity(
    postgres,
    postgres_api,
    monkeypatch,
):
    url = os.getenv("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Set TEST_RABBITMQ_URL to run real RabbitMQ checks")
    response = await postgres_api.post("/api/v1/payments", json=request_body())
    assert response.status_code == 202
    _, factories = postgres
    factory = factories[0]

    # Production publisher работает без изменений; подменяется только имя
    # назначения. Тест не читает, не очищает и не удаляет рабочую payments.new.
    queue_name = "test.payments." + uuid4().hex
    declaration = RabbitQueue(queue_name, durable=True)
    monkeypatch.setitem(QUEUES, PAYMENT_QUEUE, declaration)
    connection = await aio_pika.connect_robust(url, timeout=10)
    queue = None
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)
        async with create_broker(url) as broker:
            await broker.declare_queue(declaration)

            class CrashAfterConfirm:
                async def publish(self, *args, **kwargs):
                    await broker.publish(*args, **kwargs)
                    raise RuntimeError("lost SQL commit after broker confirm")

            with pytest.raises(RuntimeError, match="lost SQL commit"):
                await publish_once(factory, CrashAfterConfirm())
            async with factory() as session:
                row = (await session.scalars(select(Outbox))).one()
                assert row.published_at is None
                event_id = row.event_id

            assert await publish_once(factory, broker) == 1
            assert await publish_once(factory, broker) == 0

        first = await queue.get(timeout=10)
        second = await queue.get(timeout=10)
        assert first is not None and second is not None
        expected = {"event_id": event_id, "cycle": 0}
        assert json.loads(first.body) == json.loads(second.body) == expected
        assert first.message_id == second.message_id == event_id
        assert first.correlation_id == second.correlation_id == event_id
        assert first.delivery_mode == second.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        await first.ack()

        # Брокер также сохраняет идентификатор при реальной повторной доставке.
        await second.reject(requeue=True)
        redelivered = await queue.get(timeout=10)
        assert redelivered is not None and redelivered.redelivered
        assert redelivered.message_id == event_id
        assert json.loads(redelivered.body) == expected
        await redelivered.ack()

        async with factory() as session:
            assert (await session.scalars(select(Outbox))).one().published_at is not None
    finally:
        if queue is not None:
            await queue.delete(if_unused=False, if_empty=False)
        await connection.close()
