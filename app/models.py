"""Общие таблицы сервиса и независимого эмулятора провайдера."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        UniqueConstraint("provider_key", name="uq_payments_provider_key"),
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    # Уникальность обеспечивает БД: проверка SELECT перед INSERT не защищает от гонки.
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_canonical: Mapped[str] = mapped_column(Text, nullable=False)
    provider_key: Mapped[str] = mapped_column(String(100), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkItem(Base):
    __tablename__ = "work_items"
    __table_args__ = (
        UniqueConstraint("payment_id", "kind", name="uq_work_items_payment_kind"),
        CheckConstraint("kind IN ('payment', 'webhook')", name="ck_work_items_kind"),
        CheckConstraint("state IN ('ready', 'done', 'dead')", name="ck_work_items_state"),
        CheckConstraint("attempts BETWEEN 0 AND 3", name="ck_work_items_attempts"),
        CheckConstraint("cycle >= 0", name="ck_work_items_cycle"),
    )

    # Дополнение к ТЗ: отдельные этапы сохраняют событие и счетчики после рестарта.
    # Повтор webhook не должен повторять списание, поэтому состояния разделены.
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    payment_id: Mapped[str] = mapped_column(ForeignKey("payments.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="ready")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cycle: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    not_before: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint(
            "event_id", "destination", "cycle", name="uq_outbox_event_destination_cycle"
        ),
        Index("ix_outbox_unpublished", "published_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    # ID строки outbox отличается от неизменного ID доставляемого события.
    event_id: Mapped[str] = mapped_column(ForeignKey("work_items.event_id"), nullable=False)
    destination: Mapped[str] = mapped_column(String(64), nullable=False)
    cycle: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProviderOperation(Base):
    __tablename__ = "provider_operations"
    __table_args__ = (
        CheckConstraint(
            "status IN ('succeeded', 'failed')", name="ck_provider_operations_final_status"
        ),
    )

    # Эмулятор коммитит результат независимо от consumer: падение consumer не
    # стирает уже проведенную операцию. Временные ошибки сюда не записываются.
    provider_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    request_canonical: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
