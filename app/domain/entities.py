"""Payment and delivery rules, independent of HTTP, queues and persistence."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from app.shared.time import aware, iso_utc, utcnow

MAX_ATTEMPTS = 3
DESTINATIONS = {"payment": "payments.new", "webhook": "payments.webhook"}


@dataclass(kw_only=True)
class Payment:
    id: str
    idempotency_key: str
    request_canonical: str
    provider_key: str
    amount: Decimal
    currency: str
    webhook_url: str
    description: str = ""
    metadata_json: dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    created_at: datetime = field(default_factory=utcnow)
    processed_at: datetime | None = None

    def complete(self, status: str, processed_at: datetime) -> None:
        if status not in {"succeeded", "failed"}:
            raise ValueError("Provider must return a final status")
        if processed_at.tzinfo is None:
            raise ValueError("Provider timestamp must have a timezone")
        # A duplicate delivery must never rewrite a final payment outcome.
        if self.status == "pending":
            self.status = status
            self.processed_at = processed_at

    def webhook(self) -> "WorkItem":
        if self.status not in {"succeeded", "failed"} or self.processed_at is None:
            raise ValueError("Only a completed payment can produce a webhook")
        event_id = str(uuid4())
        return WorkItem(
            event_id=event_id,
            payment_id=self.id,
            kind="webhook",
            payload={
                "event_id": event_id,
                "payment_id": self.id,
                "status": self.status,
                "amount": format(self.amount, ".2f"),
                "currency": self.currency,
                "processed_at": iso_utc(self.processed_at),
            },
        )


@dataclass(kw_only=True)
class WorkItem:
    event_id: str
    payment_id: str
    kind: str
    payload: dict[str, Any]
    state: str = "ready"
    attempts: int = 0
    cycle: int = 0
    not_before: datetime | None = None
    last_error: str | None = None
    created_at: datetime = field(default_factory=utcnow)

    def accepts(self, cycle: int) -> bool:
        return self.state == "ready" and self.cycle == cycle

    @property
    def exhausted(self) -> bool:
        return self.attempts >= MAX_ATTEMPTS

    def delay(self, now: datetime) -> float:
        return (
            max(0.0, (aware(self.not_before) - aware(now)).total_seconds())
            if (self.not_before is not None)
            else 0.0
        )

    def reserve(self, now: datetime) -> None:
        if self.state != "ready" or self.exhausted or self.delay(now):
            raise ValueError("Attempt is not ready")
        # Reserve before external I/O: a crash consumes an uncertain attempt.
        self.attempts += 1
        self.not_before = now + timedelta(seconds=2 ** (self.attempts - 1))
        self.last_error = "Attempt reserved; result not recorded (possible interrupted call)"

    def fail(self, error: str, now: datetime) -> None:
        self.last_error = error[:2000]
        if self.exhausted:
            self.mark_dead()
        else:
            self.not_before = now + timedelta(seconds=2 ** (self.attempts - 1))

    def mark_dead(self) -> None:
        self.state = "dead"
        self.not_before = None

    def complete(self) -> None:
        self.state = "done"
        self.last_error = None
        self.not_before = None

    def replay(self) -> None:
        if self.state != "dead":
            raise ValueError(f"Восстанавливать можно только dead, сейчас: {self.state}")
        self.cycle += 1
        self.attempts = 0
        self.state = "ready"
        self.not_before = None
        self.last_error = None

    def event(self) -> "Outbox":
        destination = DESTINATIONS[self.kind] + (".dlq" if self.state == "dead" else "")
        return Outbox(event_id=self.event_id, destination=destination, cycle=self.cycle)


@dataclass(kw_only=True)
class Outbox:
    event_id: str
    destination: str
    cycle: int
    id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=utcnow)
    published_at: datetime | None = None
