"""Один обработчик с независимыми сохраняемыми этапами оплаты и webhook."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import select

from app.db import event_lock
from app.models import Outbox, Payment, WorkItem, utcnow
from app.provider import ProviderTemporaryError
from app.schemas import iso_utc
from app.webhook import WebhookDeliveryError

logger = logging.getLogger(__name__)
MAX_ATTEMPTS = 3
DESTINATIONS = {"payment": "payments.new", "webhook": "payments.webhook"}


def aware(value: datetime) -> datetime:
    return value.replace(tzinfo=value.tzinfo or timezone.utc)


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
        self, session_factory, engine, provider, webhook, *, sleep=asyncio.sleep, now=utcnow
    ):
        self.sessions = session_factory
        self.engine = engine
        self.provider = provider
        self.webhook = webhook
        self.sleep = sleep
        self.now = now

    async def handle(self, event_id: str, cycle: int) -> None:
        async with event_lock(self.engine, "event:" + event_id) as acquired:
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

    async def _item(self, session, event_id: str, cycle: int):
        item = await session.scalar(
            select(WorkItem).where(WorkItem.event_id == event_id).with_for_update()
        )
        # Номер цикла — fencing token: старое сообщение до ручного восстановления
        # не расходует попытки нового цикла. event_id при этом не меняется.
        if item is None or item.state != "ready" or item.cycle != cycle:
            return None
        return item

    async def _reserve(self, event_id: str, cycle: int) -> Attempt | float | None:
        async with self.sessions() as session, session.begin():
            item = await self._item(session, event_id, cycle)
            if item is None:
                return None
            payment = await session.get(Payment, item.payment_id)
            if payment is None:
                raise RuntimeError("WorkItem references missing payment")
            if item.kind == "payment" and payment.status != "pending":
                item.state = "done"
                await self._ensure_webhook(session, payment)
                return None
            if item.attempts >= MAX_ATTEMPTS:
                # Crash после резервирования третьей попытки может оставить
                # неопределённый результат. Новый автоматический вызов запрещён:
                # pending + DLQ; ручное восстановление использует прежний ключ.
                self._dead(session, item)
                return None
            if item.not_before is not None:
                delay = (aware(item.not_before) - aware(self.now())).total_seconds()
                if delay > 0:
                    return float(delay)
            item.attempts += 1
            # Техническое решение: попытка расходуется ДО сетевого вызова.
            # Рестарт не сбрасывает лимит; даже неопределённый вызов учитывается.
            # Если процесс погиб, сохранённая дата также запрещает немедленный retry.
            delay = 2 ** (item.attempts - 1)
            item.not_before = self.now() + timedelta(seconds=delay)
            item.last_error = "Attempt reserved; result not recorded (possible interrupted call)"
            return Attempt(
                item.kind,
                item.attempts,
                payment.provider_key,
                format(payment.amount, ".2f"),
                payment.currency,
                payment.webhook_url,
                item.payload,
            )

    def _dead(self, session, item: WorkItem) -> None:
        item.state = "dead"
        item.not_before = None
        # DLQ тоже через outbox: недоступность RabbitMQ не теряет проблемный этап.
        session.add(
            Outbox(
                id=str(uuid4()),
                event_id=item.event_id,
                destination=DESTINATIONS[item.kind] + ".dlq",
                cycle=item.cycle,
            )
        )

    async def _failure(self, event_id: str, cycle: int, error: str) -> None:
        async with self.sessions() as session, session.begin():
            item = await self._item(session, event_id, cycle)
            if item is None:
                return
            item.last_error = error[:2000]
            if item.attempts >= MAX_ATTEMPTS:
                self._dead(session, item)
            else:
                # 3 попытки всего, задержки после ошибок ровно 1 и 2 секунды.
                item.not_before = self.now() + timedelta(seconds=2 ** (item.attempts - 1))

    async def _ensure_webhook(self, session, payment: Payment) -> None:
        existing = await session.scalar(
            select(WorkItem).where(
                WorkItem.payment_id == payment.id,
                WorkItem.kind == "webhook",
            )
        )
        if existing is not None:
            return
        event_id = str(uuid4())
        # Payload создаётся один раз В ТОЙ ЖЕ транзакции, что финальный статус.
        # Нельзя пересобирать processed_at/event_id при повторах уведомления.
        session.add(
            WorkItem(
                event_id=event_id,
                payment_id=payment.id,
                kind="webhook",
                state="ready",
                payload={
                    "event_id": event_id,
                    "payment_id": payment.id,
                    "status": payment.status,
                    "amount": format(payment.amount, ".2f"),
                    "currency": payment.currency,
                    "processed_at": iso_utc(payment.processed_at),
                },
                attempts=0,
                cycle=0,
            )
        )
        await session.flush()
        session.add(
            Outbox(id=str(uuid4()), event_id=event_id, destination="payments.webhook", cycle=0)
        )

    async def _complete_payment(self, event_id: str, cycle: int, result: dict) -> None:
        if result["status"] not in {"succeeded", "failed"}:
            raise ValueError("Provider must return a final status")
        processed_at = datetime.fromisoformat(result["processed_at"])
        if processed_at.tzinfo is None:
            raise ValueError("Provider timestamp must have a timezone")
        async with self.sessions() as session, session.begin():
            item = await self._item(session, event_id, cycle)
            if item is None:
                return
            payment = await session.scalar(
                select(Payment).where(Payment.id == item.payment_id).with_for_update()
            )
            # Окончательный результат монотонен: дубль не переписывает факт оплаты.
            if payment.status == "pending":
                payment.status = result["status"]
                payment.processed_at = processed_at
            item.state = "done"
            item.last_error = None
            item.not_before = None
            await self._ensure_webhook(session, payment)

    async def _complete_webhook(self, event_id: str, cycle: int) -> None:
        async with self.sessions() as session, session.begin():
            item = await self._item(session, event_id, cycle)
            if item is not None:
                item.state = "done"
                item.last_error = None
                item.not_before = None
