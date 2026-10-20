from decimal import Decimal
from functools import partial
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.application.recovery import list_dead, replay
from app.infrastructure.persistence.models import Base, Outbox, Payment, WorkItem, utcnow
from app.infrastructure.persistence.uow import SqlAlchemyUnitOfWork


@pytest_asyncio.fixture
async def recovery_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'recovery.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def seed_dead(factory, kind):
    payment_id, event_id = str(uuid4()), str(uuid4())
    payload = {"payment_id": payment_id}
    if kind == "webhook":
        payload.update(
            event_id=event_id,
            status="succeeded",
            amount="100.00",
            currency="RUB",
            processed_at=utcnow().isoformat(),
        )
    async with factory() as session, session.begin():
        session.add(
            Payment(
                id=payment_id,
                idempotency_key=payment_id,
                request_canonical="{}",
                provider_key="stable-provider-key",
                amount=Decimal("100"),
                currency="RUB",
                webhook_url="http://client.test/webhook",
                status="succeeded" if kind == "webhook" else "pending",
            )
        )
        await session.flush()
        session.add(
            WorkItem(
                event_id=event_id,
                payment_id=payment_id,
                kind=kind,
                payload=payload,
                state="dead",
                attempts=3,
                cycle=0,
                not_before=utcnow(),
                last_error="unavailable",
            )
        )
    return payment_id, event_id, payload


@pytest.mark.parametrize(
    "kind,destination", [("payment", "payments.new"), ("webhook", "payments.webhook")]
)
async def test_manual_recovery_starts_bounded_cycle_preserving_identity(
    recovery_db, kind, destination
):
    payment_id, event_id, payload = await seed_dead(recovery_db, kind)
    dead = await list_dead(partial(SqlAlchemyUnitOfWork, recovery_db))
    assert dead == [
        {
            "event_id": event_id,
            "payment_id": payment_id,
            "stage": kind,
            "cycle": 0,
            "attempts": 3,
            "last_error": "unavailable",
        }
    ]

    result = await replay(event_id, partial(SqlAlchemyUnitOfWork, recovery_db))
    assert result == {"event_id": event_id, "stage": kind, "cycle": 1}
    assert await list_dead(partial(SqlAlchemyUnitOfWork, recovery_db)) == []
    async with recovery_db() as session:
        item = await session.get(WorkItem, event_id)
        assert item.payload == payload
        assert item.attempts == 0
        assert item.state == "ready"
        assert item.not_before is None
        assert item.last_error is None
        payment = await session.get(Payment, payment_id)
        assert payment.provider_key == "stable-provider-key"
        assert payment.status == ("succeeded" if kind == "webhook" else "pending")
        rows = (await session.scalars(select(Outbox))).all()
        assert len(rows) == 1
        assert (rows[0].event_id, rows[0].cycle, rows[0].destination) == (event_id, 1, destination)


async def test_recovery_requires_existing_dead_event(recovery_db):
    _, event_id, _ = await seed_dead(recovery_db, "webhook")
    with pytest.raises(ValueError, match="не найдено"):
        await replay(str(uuid4()), partial(SqlAlchemyUnitOfWork, recovery_db))
    await replay(event_id, partial(SqlAlchemyUnitOfWork, recovery_db))
    with pytest.raises(ValueError, match="только dead"):
        await replay(event_id, partial(SqlAlchemyUnitOfWork, recovery_db))
    async with recovery_db() as session:
        assert len((await session.scalars(select(Outbox))).all()) == 1
