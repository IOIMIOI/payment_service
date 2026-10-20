from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.domain.entities import Payment, WorkItem


def test_reserved_attempt_survives_crash_and_exhaustion_requires_replay():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    item = WorkItem(event_id="event", payment_id="payment", kind="payment", payload={})
    for number in range(1, 4):
        item.reserve(now)
        assert item.attempts == number
        assert item.delay(now) == 2 ** (number - 1)
        with pytest.raises(ValueError, match="not ready"):
            item.reserve(now)
        now += timedelta(seconds=2 ** (number - 1))
    assert item.exhausted
    with pytest.raises(ValueError):
        item.reserve(now)
    item.mark_dead()
    assert item.event().destination == "payments.new.dlq"
    item.replay()
    assert item.accepts(1) and not item.accepts(0)
    assert item.attempts == 0 and item.event_id == "event"


def test_payment_final_result_is_monotonic_and_webhook_requires_completion():
    payment = Payment(
        id="payment",
        idempotency_key="key",
        provider_key="provider",
        request_canonical="{}",
        amount=Decimal("1.00"),
        currency="RUB",
        webhook_url="http://client.test/hook",
    )
    with pytest.raises(ValueError, match="completed"):
        payment.webhook()
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    payment.complete("succeeded", timestamp)
    payment.complete("failed", timestamp + timedelta(hours=1))
    assert payment.status == "succeeded" and payment.processed_at == timestamp
    assert payment.webhook().payload["processed_at"] == timestamp.isoformat()
