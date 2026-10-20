"""Exercise use cases through their ports, without SQLAlchemy, HTTP or RabbitMQ."""

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.application.payments import CreatePayment, IdempotencyConflict, PaymentService
from app.application.ports import StorageConflict, WebhookDeliveryError
from app.application.processor import Processor
from app.application.recovery import replay


class MemoryRepository:
    def __init__(self, rows):
        self.rows = rows

    async def get(self, identity, *, lock=False):
        return self.rows.get(identity)

    async def by_key(self, key):
        return next((p for p in self.rows.values() if p.idempotency_key == key), None)

    async def add(self, entity):
        if hasattr(entity, "idempotency_key") and await self.by_key(entity.idempotency_key):
            raise StorageConflict("Duplicate key")
        await self.save(entity)

    async def save(self, entity):
        identity = entity.id if hasattr(entity, "id") else entity.event_id
        self.rows[identity] = entity

    async def webhook_for(self, payment_id):
        return next(
            (w for w in self.rows.values() if w.payment_id == payment_id and w.kind == "webhook"),
            None,
        )


class MemoryStore:
    def __init__(self):
        self.rows = {"payments": {}, "work_items": {}, "outbox": {}}
        self.reject_outbox = False

    @asynccontextmanager
    async def uow(self):
        transaction = deepcopy(self.rows)
        unit = SimpleNamespace(
            **{name: MemoryRepository(rows) for name, rows in transaction.items()}
        )
        if self.reject_outbox:

            async def fail(_):
                raise RuntimeError("Outbox unavailable")

            unit.outbox.add = fail
        yield unit
        self.rows = transaction


@asynccontextmanager
async def unlocked(_):
    yield True


def command(**changes):
    return CreatePayment(
        amount=Decimal("100.00"),
        currency="RUB",
        webhook_url="http://client.test/hook",
        **changes,
    )


async def test_create_replay_conflict_and_read_without_database():
    store = MemoryStore()
    service = PaymentService(store.uow)
    first = await service.create(command(), "order-1")
    repeated = await service.create(command(), "order-1")
    assert first.created and not repeated.created
    assert first.payment == repeated.payment == await service.get(first.payment.id)
    assert len(store.rows["payments"]) == len(store.rows["outbox"]) == 1
    with pytest.raises(IdempotencyConflict):
        await service.create(command(description="changed"), "order-1")
    assert len(store.rows["work_items"]) == 1


async def test_failed_outbox_does_not_leave_payment_or_work():
    store = MemoryStore()
    store.reject_outbox = True
    with pytest.raises(RuntimeError, match="Outbox unavailable"):
        await PaymentService(store.uow).create(command(), "order-1")
    assert not any(store.rows.values())


async def test_webhook_recovery_never_recharges_and_ignores_old_cycle():
    store = MemoryStore()
    result = await PaymentService(store.uow).create(command(), "order-1")
    event_id = next(iter(store.rows["work_items"]))
    charges, notifications = [], []
    available = False
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    clock = [timestamp]

    async def charge(key, amount, currency):
        charges.append(key)
        return {"status": "succeeded", "processed_at": timestamp.isoformat()}

    async def send(url, payload):
        notifications.append(deepcopy(payload))
        if not available:
            raise WebhookDeliveryError("Unavailable")

    async def sleep(seconds):
        from datetime import timedelta

        clock[0] += timedelta(seconds=seconds)

    worker = Processor(
        store.uow,
        unlocked,
        SimpleNamespace(charge=charge),
        SimpleNamespace(send=send),
        now=lambda: clock[0],
        sleep=sleep,
    )
    await worker.handle(event_id, 0)
    webhook = next(w for w in store.rows["work_items"].values() if w.kind == "webhook")
    await worker.handle(webhook.event_id, 0)
    assert store.rows["work_items"][webhook.event_id].state == "dead"
    await replay(webhook.event_id, store.uow)
    available = True
    await worker.handle(webhook.event_id, 0)
    assert len(notifications) == 3
    await worker.handle(webhook.event_id, 1)
    assert len(charges) == 1
    assert len(notifications) == 4
    assert all(payload == notifications[0] for payload in notifications)
    assert store.rows["payments"][result.payment.id].status == "succeeded"
    assert store.rows["work_items"][webhook.event_id].state == "done"
