"""Начальная схема с независимыми этапами и сохранением результата провайдера.

Revision ID: 0001_initial
"""

import sqlalchemy as sa

from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_canonical", sa.Text(), nullable=False),
        sa.Column("provider_key", sa.String(100), nullable=False),
        sa.Column("amount", sa.Numeric(20, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        sa.UniqueConstraint("provider_key", name="uq_payments_provider_key"),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"
        ),
    )
    # В исходном ТЗ только payments/outbox; work_items нужны для неизменяемого
    # payload, постоянного event_id и счетчиков попыток, переживающих рестарт.
    op.create_table(
        "work_items",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("payment_id", sa.String(36), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("cycle", sa.Integer(), nullable=False),
        sa.Column("not_before", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("payment_id", "kind", name="uq_work_items_payment_kind"),
        sa.CheckConstraint("kind IN ('payment', 'webhook')", name="ck_work_items_kind"),
        sa.CheckConstraint("state IN ('ready', 'done', 'dead')", name="ck_work_items_state"),
        sa.CheckConstraint("attempts BETWEEN 0 AND 3", name="ck_work_items_attempts"),
        sa.CheckConstraint("cycle >= 0", name="ck_work_items_cycle"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("event_id", sa.String(36), sa.ForeignKey("work_items.event_id"), nullable=False),
        sa.Column("destination", sa.String(64), nullable=False),
        sa.Column("cycle", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "event_id", "destination", "cycle", name="uq_outbox_event_destination_cycle"
        ),
    )
    op.create_index("ix_outbox_unpublished", "outbox", ["published_at", "created_at"])
    # Дополнительная таблица эмулятора моделирует хранилище внешнего провайдера.
    # Его собственная транзакция переживает сбой основного обработчика.
    op.create_table(
        "provider_operations",
        sa.Column("provider_key", sa.String(100), primary_key=True),
        sa.Column("request_canonical", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed')", name="ck_provider_operations_final_status"
        ),
    )


def downgrade() -> None:
    op.drop_table("provider_operations")
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("work_items")
    op.drop_table("payments")
