import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import event, func, select

from app.models import Outbox, Payment, ProviderOperation, WorkItem
from app.provider import Emulator, ProviderTemporaryError
from app.recover import replay
from app.webhook import WebhookDeliveryError
from app.worker import EventBusy, Processor


class Clock:
    def __init__(self):
        self.time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.delays = []

    def now(self):
        return self.time

    async def sleep(self, seconds):
        self.delays.append(seconds)
        self.time += timedelta(seconds=seconds)


class Provider:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes or ["succeeded"])
        self.calls = []

    async def charge(self, key, amount, currency):
        self.calls.append((key, amount, currency))
        outcome = self.outcomes.pop(0)
        if outcome == "temporary":
            raise ProviderTemporaryError("temporary provider failure")
        return {"status": outcome, "processed_at": "2026-01-01T00:00:00+00:00"}


class Webhook:
    def __init__(self, failures=0):
        self.failures = failures
        self.calls = []

    async def send(self, url, payload):
        self.calls.append((url, deepcopy(payload)))
        if len(self.calls) <= self.failures:
            raise WebhookDeliveryError("HTTP 500")


async def seed(sessions):
    payment_id, event_id = str(uuid4()), str(uuid4())
    async with sessions() as session, session.begin():
        session.add(
            Payment(
                id=payment_id,
                idempotency_key=uuid4().hex,
                request_canonical="{}",
                provider_key="payment:" + payment_id,
                amount=Decimal("100.00"),
                currency="RUB",
                description="test",
                metadata_json={},
                webhook_url="http://receiver/hook",
                status="pending",
            )
        )
        await session.flush()
        session.add(
            WorkItem(
                event_id=event_id,
                payment_id=payment_id,
                kind="payment",
                payload={"payment_id": payment_id},
            )
        )
        await session.flush()
        session.add(Outbox(id=str(uuid4()), event_id=event_id, destination="payments.new", cycle=0))
    return payment_id, event_id


async def read(sessions, model, identity):
    async with sessions() as session:
        return await session.get(model, identity)


async def webhook_event(sessions, payment_id):
    async with sessions() as session:
        return await session.scalar(
            select(WorkItem).where(WorkItem.payment_id == payment_id, WorkItem.kind == "webhook")
        )


def processor(sessions, engine, provider=None, webhook=None, clock=None):
    clock = clock or Clock()
    return Processor(
        sessions,
        engine,
        provider or Provider(),
        webhook or Webhook(),
        sleep=clock.sleep,
        now=clock.now,
    )


@pytest.mark.parametrize("status", ["succeeded", "failed"])
async def test_final_outcome_is_monotonic_and_duplicate_does_not_charge(
    session_factory, engine, status
):
    payment_id, event_id = await seed(session_factory)
    provider, webhook = Provider(status), Webhook()
    worker = processor(session_factory, engine, provider, webhook)
    await worker.handle(event_id, 0)
    await worker.handle(event_id, 0)
    restarted = processor(session_factory, engine, provider, webhook)
    await restarted.handle(event_id, 0)
    payment = await read(session_factory, Payment, payment_id)
    item = await read(session_factory, WorkItem, event_id)
    assert payment.status == status and payment.processed_at is not None
    assert item.state == "done" and item.attempts == 1
    assert len(provider.calls) == 1
    notification = await webhook_event(session_factory, payment_id)
    assert notification.payload == {
        "event_id": notification.event_id,
        "payment_id": payment_id,
        "status": status,
        "amount": "100.00",
        "currency": "RUB",
        "processed_at": "2026-01-01T00:00:00+00:00",
    }
    await restarted.handle(notification.event_id, 0)
    await restarted.handle(notification.event_id, 0)
    assert len(webhook.calls) == 1 and len(provider.calls) == 1
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 2


async def test_three_temporary_failures_dlq_pending_delays_one_two(session_factory, engine):
    payment_id, event_id = await seed(session_factory)
    clock, provider = Clock(), Provider("temporary", "temporary", "temporary")
    await processor(session_factory, engine, provider, clock=clock).handle(event_id, 0)
    item = await read(session_factory, WorkItem, event_id)
    assert item.state == "dead" and item.attempts == 3
    assert clock.delays == [1.0, 2.0]
    assert len(set(provider.calls)) == 1 and len(provider.calls) == 3
    assert (await read(session_factory, Payment, payment_id)).status == "pending"
    assert await webhook_event(session_factory, payment_id) is None
    async with session_factory() as session:
        dlq = (
            await session.scalars(select(Outbox).where(Outbox.destination == "payments.new.dlq"))
        ).one()
        assert dlq.event_id == event_id and dlq.cycle == 0


async def test_temporary_then_success_same_key(session_factory, engine):
    payment_id, event_id = await seed(session_factory)
    provider, clock = Provider("temporary", "temporary", "succeeded"), Clock()
    await processor(session_factory, engine, provider, clock=clock).handle(event_id, 0)
    assert (await read(session_factory, Payment, payment_id)).status == "succeeded"
    assert clock.delays == [1.0, 2.0]
    assert len(set(provider.calls)) == 1


async def test_webhook_exhaustion_preserves_payment_and_payload(session_factory, engine):
    payment_id, event_id = await seed(session_factory)
    provider, webhook, clock = Provider(), Webhook(3), Clock()
    worker = processor(session_factory, engine, provider, webhook, clock)
    await worker.handle(event_id, 0)
    notification = await webhook_event(session_factory, payment_id)
    original = deepcopy(notification.payload)
    await worker.handle(notification.event_id, 0)
    await worker.handle(notification.event_id, 0)
    dead = await read(session_factory, WorkItem, notification.event_id)
    assert dead.state == "dead" and dead.attempts == 3
    assert clock.delays == [1.0, 2.0]
    assert len(webhook.calls) == 3 and all(payload == original for _, payload in webhook.calls)
    assert len(provider.calls) == 1
    assert (await read(session_factory, Payment, payment_id)).status == "succeeded"
    async with session_factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(Outbox)
                .where(Outbox.destination == "payments.webhook.dlq")
            )
            == 1
        )

    await replay(notification.event_id, session_factory)
    await worker.handle(notification.event_id, 0)  # старое сообщение не тратит новый цикл
    assert len(webhook.calls) == 3
    await worker.handle(notification.event_id, 1)
    restored = await read(session_factory, WorkItem, notification.event_id)
    assert restored.state == "done" and restored.attempts == 1 and restored.cycle == 1
    assert webhook.calls[-1][1] == original and len(provider.calls) == 1


async def test_manual_payment_replay_new_bounded_cycle_same_identity(session_factory, engine):
    payment_id, event_id = await seed(session_factory)
    provider = Provider(*(["temporary"] * 6))
    worker = processor(session_factory, engine, provider)
    await worker.handle(event_id, 0)
    original = await read(session_factory, WorkItem, event_id)
    await replay(event_id, session_factory)
    await worker.handle(event_id, 0)
    assert len(provider.calls) == 3
    await worker.handle(event_id, 1)
    restored = await read(session_factory, WorkItem, event_id)
    assert restored.event_id == original.event_id and restored.payload == original.payload
    assert restored.state == "dead" and restored.cycle == 1 and restored.attempts == 3
    assert len(provider.calls) == 6 and len(set(provider.calls)) == 1
    assert (await read(session_factory, Payment, payment_id)).status == "pending"


async def test_consumer_crash_after_provider_commit_reuses_final_result(
    session_factory, engine, monkeypatch
):
    payment_id, event_id = await seed(session_factory)
    operations, clock = [], Clock()

    def outcome():
        operations.append(1)
        return 0.2

    async def no_delay(_):
        pass

    provider = Emulator(session_factory, engine, random_value=outcome, sleep=no_delay)
    worker = processor(session_factory, engine, provider, clock=clock)

    async def crash(*_):
        raise RuntimeError("consumer killed after provider commit")

    monkeypatch.setattr(worker, "_complete_payment", crash)
    with pytest.raises(RuntimeError, match="consumer killed"):
        await worker.handle(event_id, 0)
    assert (await read(session_factory, Payment, payment_id)).status == "pending"
    assert (await read(session_factory, WorkItem, event_id)).attempts == 1
    # Новый объект обработчика имитирует потерю всей памяти consumer.
    await processor(session_factory, engine, provider, clock=clock).handle(event_id, 0)
    payment = await read(session_factory, Payment, payment_id)
    stored = await read(session_factory, ProviderOperation, payment.provider_key)
    assert payment.status == stored.status == "succeeded"
    assert payment.processed_at == stored.processed_at
    assert len(operations) == 1
    assert (await read(session_factory, WorkItem, event_id)).attempts == 2


async def test_restart_does_not_reset_attempts_or_deadline(session_factory, engine, monkeypatch):
    payment_id, event_id = await seed(session_factory)
    provider, clock = Provider("temporary", "temporary", "temporary"), Clock()
    worker = processor(session_factory, engine, provider, clock=clock)
    record_failure = worker._failure

    async def crash_after_failure(*args):
        await record_failure(*args)
        raise RuntimeError("restart")

    monkeypatch.setattr(worker, "_failure", crash_after_failure)
    with pytest.raises(RuntimeError, match="restart"):
        await worker.handle(event_id, 0)
    assert (await read(session_factory, WorkItem, event_id)).attempts == 1
    await processor(session_factory, engine, provider, clock=clock).handle(event_id, 0)
    assert len(provider.calls) == 3 and clock.delays == [1.0, 2.0]
    assert (await read(session_factory, WorkItem, event_id)).state == "dead"
    assert (await read(session_factory, Payment, payment_id)).status == "pending"


async def test_uncertain_third_call_goes_dlq_then_manual_lookup(
    session_factory, engine, monkeypatch
):
    payment_id, event_id = await seed(session_factory)
    async with session_factory() as session, session.begin():
        item = await session.get(WorkItem, event_id)
        item.attempts = 2
    operations = []

    async def no_delay(_):
        pass

    def outcome():
        operations.append(1)
        return 0.1

    provider = Emulator(session_factory, engine, sleep=no_delay, random_value=outcome)
    worker = processor(session_factory, engine, provider)

    async def crash(*_):
        raise RuntimeError("commit lost")

    monkeypatch.setattr(worker, "_complete_payment", crash)
    with pytest.raises(RuntimeError):
        await worker.handle(event_id, 0)
    worker = processor(session_factory, engine, provider)
    await worker.handle(event_id, 0)
    assert (await read(session_factory, WorkItem, event_id)).state == "dead"
    assert (await read(session_factory, Payment, payment_id)).status == "pending"
    assert len(operations) == 1
    await replay(event_id, session_factory)
    await worker.handle(event_id, 1)
    assert (await read(session_factory, Payment, payment_id)).status == "succeeded"
    assert len(operations) == 1


async def test_concurrent_duplicate_cannot_reserve_second_call(session_factory, engine):
    _, event_id = await seed(session_factory)
    entered, release = asyncio.Event(), asyncio.Event()

    class SlowProvider(Provider):
        async def charge(self, *args):
            entered.set()
            await release.wait()
            return await super().charge(*args)

    provider = SlowProvider()
    first = processor(session_factory, engine, provider)
    second = processor(session_factory, engine, provider)
    task = asyncio.create_task(first.handle(event_id, 0))
    await entered.wait()
    try:
        with pytest.raises(EventBusy):
            await second.handle(event_id, 0)
    finally:
        release.set()
        await task
    assert len(provider.calls) == 1


async def test_final_status_and_webhook_are_atomic_after_sql_failure(session_factory, engine):
    payment_id, event_id = await seed(session_factory)
    operations = []

    async def no_delay(_):
        pass

    def outcome():
        operations.append(1)
        return 0.1

    provider = Emulator(session_factory, engine, sleep=no_delay, random_value=outcome)

    def fail_webhook_outbox(connection, cursor, statement, parameters, context, executemany):
        if statement.upper().startswith("INSERT INTO OUTBOX"):
            raise RuntimeError("webhook outbox SQL failure")

    event.listen(engine.sync_engine, "before_cursor_execute", fail_webhook_outbox)
    try:
        with pytest.raises(RuntimeError, match="webhook outbox SQL failure"):
            await processor(session_factory, engine, provider).handle(event_id, 0)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", fail_webhook_outbox)
    assert (await read(session_factory, Payment, payment_id)).status == "pending"
    assert (await read(session_factory, WorkItem, event_id)).state == "ready"
    assert await webhook_event(session_factory, payment_id) is None
    await processor(session_factory, engine, provider).handle(event_id, 0)
    assert (await read(session_factory, Payment, payment_id)).status == "succeeded"
    assert await webhook_event(session_factory, payment_id) is not None
    assert len(operations) == 1


async def test_webhook_crash_replays_same_notification_without_payment(
    session_factory, engine, monkeypatch
):
    payment_id, event_id = await seed(session_factory)
    provider, webhook = Provider(), Webhook()
    worker = processor(session_factory, engine, provider, webhook)
    await worker.handle(event_id, 0)
    notification = await webhook_event(session_factory, payment_id)

    async def crash(*_):
        raise RuntimeError("crash after webhook response")

    monkeypatch.setattr(worker, "_complete_webhook", crash)
    with pytest.raises(RuntimeError):
        await worker.handle(notification.event_id, 0)
    await processor(session_factory, engine, provider, webhook).handle(notification.event_id, 0)
    assert len(webhook.calls) == 2 and webhook.calls[0] == webhook.calls[1]
    assert len(provider.calls) == 1
    assert (await read(session_factory, WorkItem, notification.event_id)).attempts == 2
    assert (await read(session_factory, Payment, payment_id)).status == "succeeded"
