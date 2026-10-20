"""Проверки HTTP-контракта, гонки ключей и атомарности платежа/outbox."""

import asyncio
from decimal import Decimal
from functools import partial

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import event, func, select

from app.application.payments import PaymentService
from app.bootstrap import get_payment_service
from app.config import settings
from app.entrypoints.api.routes import app
from app.infrastructure.persistence.models import Outbox, Payment, WorkItem, utcnow
from app.infrastructure.persistence.uow import SqlAlchemyUnitOfWork
from app.shared.json import decode_json


@pytest_asyncio.fixture
async def api_client(session_factory):
    def service():
        return PaymentService(partial(SqlAlchemyUnitOfWork, session_factory))

    app.dependency_overrides[get_payment_service] = service
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://api.test",
            headers={"X-API-Key": settings.api_key, "Idempotency-Key": "request-1"},
        ) as client:
            yield client
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def payment_body():
    return {
        "amount": "100.00",
        "currency": "RUB",
        "description": "Заказ 42",
        "metadata": {"order": 42, "nested": {"b": 2, "a": 1}},
        "webhook_url": "http://webhook-sink:8002/hook",
    }


async def test_create_persists_payment_key_event_and_outbox(
    api_client, payment_body, session_factory
):
    response = await api_client.post("/api/v1/payments", json=payment_body)
    assert response.status_code == 202
    assert response.json()["status"] == "pending"
    async with session_factory() as session:
        payment = await session.get(Payment, response.json()["payment_id"])
        work = (await session.scalars(select(WorkItem))).one()
        outbox = (await session.scalars(select(Outbox))).one()
        assert payment.idempotency_key == "request-1"
        assert payment.amount == Decimal("100.00")
        assert work.payment_id == payment.id
        assert work.kind == "payment"
        assert outbox.event_id == work.event_id
        assert outbox.destination == "payments.new"
        assert outbox.published_at is None


async def test_replay_returns_current_status_and_same_identifier(
    api_client, payment_body, session_factory
):
    first = await api_client.post("/api/v1/payments", json=payment_body)
    payment_id = first.json()["payment_id"]
    async with session_factory() as session, session.begin():
        payment = await session.get(Payment, payment_id)
        payment.status, payment.processed_at = "succeeded", utcnow()
    replay = dict(reversed(list(payment_body.items())))
    replay["amount"] = 100.0
    replay["metadata"] = {"nested": {"a": 1.0, "b": 2}, "order": 42}
    response = await api_client.post("/api/v1/payments", json=replay)
    assert response.status_code == 200
    assert response.json()["payment_id"] == payment_id
    assert response.json()["status"] == "succeeded"
    assert response.json()["created_at"] == first.json()["created_at"]
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", "100.01"),
        ("currency", "USD"),
        ("description", "Другой заказ"),
        ("metadata", {"order": 43}),
        ("webhook_url", "http://webhook-sink:8002/other"),
    ],
)
async def test_every_changed_field_conflicts(
    api_client, payment_body, field, value, session_factory
):
    assert (await api_client.post("/api/v1/payments", json=payment_body)).status_code == 202
    response = await api_client.post("/api/v1/payments", json={**payment_body, field: value})
    assert response.status_code == 409
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_concurrent_equal_requests_create_one_payment_and_one_event(
    api_client, payment_body, session_factory
):
    responses = await asyncio.gather(
        *[api_client.post("/api/v1/payments", json=payment_body) for _ in range(2)]
    )
    assert sorted(response.status_code for response in responses) == [200, 202]
    assert len({response.json()["payment_id"] for response in responses}) == 1
    async with session_factory() as session:
        for model in (Payment, WorkItem, Outbox):
            assert await session.scalar(select(func.count()).select_from(model)) == 1


async def test_concurrent_different_requests_conflict(api_client, payment_body, session_factory):
    responses = await asyncio.gather(
        api_client.post("/api/v1/payments", json=payment_body),
        api_client.post("/api/v1/payments", json={**payment_body, "description": "changed"}),
    )
    assert sorted(response.status_code for response in responses) == [202, 409]
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1


async def test_failure_saving_outbox_rolls_back_everything(
    api_client, payment_body, engine, session_factory
):
    # Сбой между INSERT платежа и commit всей транзакции не оставляет сирот.
    def fail_outbox(connection, cursor, statement, parameters, context, executemany):
        if statement.upper().startswith("INSERT INTO OUTBOX"):
            raise RuntimeError("simulated database failure")

    event.listen(engine.sync_engine, "before_cursor_execute", fail_outbox)
    try:
        with pytest.raises(RuntimeError, match="simulated database failure"):
            await api_client.post("/api/v1/payments", json=payment_body)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", fail_outbox)
    async with session_factory() as session:
        for model in (Payment, WorkItem, Outbox):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_get_payment_and_missing_payment(api_client, payment_body):
    created = await api_client.post("/api/v1/payments", json=payment_body)
    response = await api_client.get("/api/v1/payments/" + created.json()["payment_id"])
    assert response.status_code == 200
    detail = response.json()
    for field in ("amount", "currency", "description", "metadata", "webhook_url"):
        assert detail[field] == payment_body[field]
    assert detail["processed_at"] is None
    assert (await api_client.get("/api/v1/payments/missing")).status_code == 404


async def test_all_api_endpoints_require_key(api_client, payment_body):
    for key in (None, "wrong-key"):
        headers = {"Idempotency-Key": "unauthorized"}
        if key:
            headers["X-API-Key"] = key
        api_client.headers.clear()
        assert (
            await api_client.post("/api/v1/payments", json=payment_body, headers=headers)
        ).status_code == 401
        assert (
            await api_client.get("/api/v1/payments/missing", headers=headers)
        ).status_code == 401


async def test_required_key_and_strict_body_validation(api_client, payment_body):
    api_client.headers.pop("Idempotency-Key")
    assert (await api_client.post("/api/v1/payments", json=payment_body)).status_code == 422
    api_client.headers["Idempotency-Key"] = "new"
    assert (
        await api_client.post("/api/v1/payments", json={**payment_body, "ignored": 1})
    ).status_code == 422
    assert (await api_client.post("/api/v1/payments", content="not json")).status_code == 422


async def test_raw_json_amount_does_not_lose_precision(api_client):
    response = await api_client.post(
        "/api/v1/payments",
        content=(
            '{"amount":100.000000000000001,"currency":"RUB","webhook_url":"http://example.com/hook"}'
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


async def test_metadata_precision_survives_database_and_replay(api_client):
    body = '{"amount":1,"currency":"RUB","metadata":{"n":0.12345678901234567890123456789},"webhook_url":"http://example.com/hook"}'
    first = await api_client.post(
        "/api/v1/payments", content=body, headers={"Content-Type": "application/json"}
    )
    assert first.status_code == 202
    detail = await api_client.get("/api/v1/payments/" + first.json()["payment_id"])
    assert decode_json(detail.content)["metadata"] == decode_json(body)["metadata"]
    assert (
        await api_client.post(
            "/api/v1/payments", content=body, headers={"Content-Type": "application/json"}
        )
    ).status_code == 200
    changed = body.replace("12345678901234567890123456789", "12345678901234567890123456788")
    assert (
        await api_client.post(
            "/api/v1/payments", content=changed, headers={"Content-Type": "application/json"}
        )
    ).status_code == 409


@pytest.mark.parametrize("amount", ["1e9999", "1e-9999", "1e99999999999999999999"])
async def test_extreme_json_amount_has_validation_response_not_server_error(api_client, amount):
    response = await api_client.post(
        "/api/v1/payments",
        content=(
            '{"amount":' + amount + ',"currency":"RUB","webhook_url":"http://example.com/hook"}'
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


def test_openapi_preserves_typed_body_and_replay_responses():
    operation = app.openapi()["paths"]["/api/v1/payments"]["post"]
    assert operation["requestBody"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "/PaymentCreate"
    )
    for status in ("200", "202", "401", "409", "422"):
        assert status in operation["responses"]
    assert operation["security"] == [{"APIKeyHeader": []}]
