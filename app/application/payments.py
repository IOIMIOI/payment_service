"""Create/read payment use cases shared by any transport."""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import uuid4

from app.application.ports import StorageConflict, UowFactory
from app.domain.entities import Payment, WorkItem
from app.shared.json import canonical_json


class IdempotencyConflict(Exception):
    pass


class PaymentNotFound(Exception):
    pass


@dataclass(frozen=True, kw_only=True)
class CreatePayment:
    amount: Decimal
    currency: str
    webhook_url: str
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def canonical(self) -> str:
        return canonical_json(
            {
                "amount": self.amount,
                "currency": self.currency,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": self.webhook_url,
            }
        )


@dataclass(frozen=True)
class CreatedPayment:
    payment: Payment
    created: bool


class PaymentService:
    def __init__(self, uow: UowFactory):
        self.uow = uow

    async def create(self, command: CreatePayment, key: str) -> CreatedPayment:
        canonical = command.canonical()
        payment_id = str(uuid4())
        payment = Payment(
            id=payment_id,
            idempotency_key=key,
            request_canonical=canonical,
            provider_key=f"payment:{payment_id}",
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            metadata_json=command.metadata,
            webhook_url=command.webhook_url,
        )
        item = WorkItem(
            event_id=str(uuid4()),
            payment_id=payment.id,
            kind="payment",
            payload={"payment_id": payment.id},
        )
        try:
            # The unique INSERT arbitrates concurrent requests. No preflight SELECT.
            async with self.uow() as uow:
                await uow.payments.add(payment)
                await uow.work_items.add(item)
                await uow.outbox.add(item.event())
        except StorageConflict:
            # A fresh transaction reads the winner after the failed INSERT rolled back.
            async with self.uow() as uow:
                existing = await uow.payments.by_key(key)
            if existing is None:
                raise  # Do not disguise other constraint errors as an idempotent replay.
            if existing.request_canonical != canonical:
                raise IdempotencyConflict("Idempotency-Key was used with different data")
            return CreatedPayment(existing, False)
        return CreatedPayment(payment, True)

    async def get(self, payment_id: str) -> Payment:
        async with self.uow() as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id)
        return payment
