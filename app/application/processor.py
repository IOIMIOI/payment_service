"""Один обработчик с независимыми сохраняемыми этапами оплаты и webhook."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

from app.application.ports import (
    EventLock,
    Provider,
    ProviderTemporaryError,
    UnitOfWork,
    UowFactory,
    Webhook,
    WebhookDeliveryError,
)
from app.domain.entities import Payment, WorkItem
from app.shared.time import utcnow

logger = logging.getLogger(__name__)


class EventBusy(Exception):
    """Другая доставка уже обрабатывает событие; брокер должен повторить позже."""


@dataclass(frozen=True)
class Attempt:
    kind: str
    number: int
    provider_key: str
    amount: str
    currency: str
    webhook_url: str
    payload: dict


class Processor:
    def __init__(
        self,
        uow: UowFactory,
        event_lock: EventLock,
        provider: Provider,
        webhook: Webhook,
        *,
        sleep=asyncio.sleep,
        now=utcnow,
    ):
        self.uow = uow
        self.event_lock = event_lock
        self.provider = provider
        self.webhook = webhook
        self.sleep = sleep
        self.now = now

    async def handle(self, event_id: str, cycle: int) -> None:
        async with self.event_lock("event:" + event_id) as acquired:
            if not acquired:
                raise EventBusy(event_id)
            while True:
                attempt = await self._reserve(event_id, cycle)
                if attempt is None:
                    return
                if isinstance(attempt, float):
                    await self.sleep(attempt)
                    continue
                logger.info(
                    "Processing event=%s cycle=%s stage=%s attempt=%s",
                    event_id,
                    cycle,
                    attempt.kind,
                    attempt.number,
                )
                try:
                    if attempt.kind == "payment":
                        result = await self.provider.charge(
                            attempt.provider_key, attempt.amount, attempt.currency
                        )
                        await self._complete_payment(event_id, cycle, result)
                    else:
                        await self.webhook.send(attempt.webhook_url, attempt.payload)
                        await self._complete_webhook(event_id, cycle)
                except (ProviderTemporaryError, WebhookDeliveryError) as exc:
                    await self._failure(event_id, cycle, str(exc))
                    continue
                return

    async def _item(self, uow: UnitOfWork, event_id: str, cycle: int) -> WorkItem | None:
        item = await uow.work_items.get(event_id)
        # Old messages cannot consume attempts from a recovered cycle.
        return item if item is not None and item.accepts(cycle) else None

    async def _reserve(self, event_id: str, cycle: int) -> Attempt | float | None:
        async with self.uow() as uow:
            item = await self._item(uow, event_id, cycle)
            if item is None:
                return None
            payment = await uow.payments.get(item.payment_id)
            if payment is None:
                raise RuntimeError("WorkItem references missing payment")
            if item.kind == "payment" and payment.status != "pending":
                item.complete()
                await uow.work_items.save(item)
                await self._ensure_webhook(uow, payment)
                return None
            if item.exhausted:
                # Crash после резервирования третьей попытки может оставить
                # неопределённый результат. Новый автоматический вызов запрещён:
                # pending + DLQ; ручное восстановление использует прежний ключ.
                await self._dead(uow, item)
                return None
            delay = item.delay(self.now())
            if delay > 0:
                return delay
            item.reserve(self.now())
            await uow.work_items.save(item)
            return Attempt(
                item.kind,
                item.attempts,
                payment.provider_key,
                format(payment.amount, ".2f"),
                payment.currency,
                payment.webhook_url,
                item.payload,
            )

    async def _dead(self, uow: UnitOfWork, item: WorkItem) -> None:
        item.mark_dead()
        await uow.work_items.save(item)
        # DLQ publication and dead state must commit atomically.
        await uow.outbox.add(item.event())

    async def _failure(self, event_id: str, cycle: int, error: str) -> None:
        async with self.uow() as uow:
            item = await self._item(uow, event_id, cycle)
            if item is None:
                return
            item.fail(error, self.now())
            await uow.work_items.save(item)
            if item.state == "dead":
                await uow.outbox.add(item.event())

    async def _ensure_webhook(self, uow: UnitOfWork, payment: Payment) -> None:
        if await uow.work_items.webhook_for(payment.id) is not None:
            return
        # Snapshot once, in the same transaction as the final payment status.
        notification = payment.webhook()
        await uow.work_items.add(notification)
        await uow.outbox.add(notification.event())

    async def _complete_payment(self, event_id: str, cycle: int, result: dict) -> None:
        if result["status"] not in {"succeeded", "failed"}:
            raise ValueError("Provider must return a final status")
        processed_at = datetime.fromisoformat(result["processed_at"])
        if processed_at.tzinfo is None:
            raise ValueError("Provider timestamp must have a timezone")
        async with self.uow() as uow:
            item = await self._item(uow, event_id, cycle)
            if item is None:
                return
            payment = await uow.payments.get(item.payment_id, lock=True)
            if payment is None:
                raise RuntimeError("WorkItem references missing payment")
            payment.complete(result["status"], processed_at)
            item.complete()
            await uow.payments.save(payment)
            await uow.work_items.save(item)
            await self._ensure_webhook(uow, payment)

    async def _complete_webhook(self, event_id: str, cycle: int) -> None:
        async with self.uow() as uow:
            item = await self._item(uow, event_id, cycle)
            if item is not None:
                item.complete()
                await uow.work_items.save(item)
