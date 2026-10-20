from decimal import Decimal

import pytest
from sqlalchemy import event, func, select

from app.domain.entities import Payment
from app.infrastructure.persistence import models
from app.infrastructure.persistence.repositories import Payments
from app.infrastructure.persistence.uow import SqlAlchemyUnitOfWork


def payment():
    return Payment(
        id="payment",
        idempotency_key="key",
        provider_key="provider",
        request_canonical="{}",
        amount=Decimal("1.00"),
        currency="RUB",
        webhook_url="http://client.test/hook",
        metadata_json={"nested": {"value": "original"}},
    )


async def test_domain_json_is_detached_and_only_explicit_save_persists(session_factory):
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.payments.add(payment())
    async with session_factory() as session, session.begin():
        row = await session.get(models.Payment, "payment")
        repo = Payments(session)
        entity = await repo.get("payment")
        assert isinstance(entity, Payment)
        entity.metadata_json["nested"]["value"] = "changed"
        assert row.metadata_json["nested"]["value"] == "original"
        await repo.save(entity)
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        saved = await uow.payments.get("payment")
        assert saved.metadata_json["nested"]["value"] == "changed"


async def test_commit_failure_rolls_back_even_flushed_records(session_factory):
    def fail_commit(session):
        raise RuntimeError("Commit unavailable")

    unit = SqlAlchemyUnitOfWork(session_factory)
    with pytest.raises(RuntimeError, match="Commit unavailable"):
        async with unit:
            await unit.payments.add(payment())
            event.listen(unit.session.sync_session, "before_commit", fail_commit)
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(models.Payment)) == 0
    # The failed transaction releases its resources; the same key can be retried.
    async with SqlAlchemyUnitOfWork(session_factory) as retry:
        await retry.payments.add(payment())
