from decimal import Decimal
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import Base, Outbox, Payment, WorkItem
from app.outbox import publish_once


@pytest_asyncio.fixture
async def outbox_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def seed(factory, *, destination="payments.new"):
    payment_id, event_id, outbox_id = (str(uuid4()) for _ in range(3))
    async with factory() as session, session.begin():
        session.add(
            Payment(
                id=payment_id,
                idempotency_key=payment_id,
                request_canonical="{}",
                provider_key="provider:" + payment_id,
                amount=Decimal("100"),
                currency="RUB",
                webhook_url="http://client.test/webhook",
            )
        )
        await session.flush()
        session.add(
            WorkItem(
                event_id=event_id,
                payment_id=payment_id,
                kind="payment",
                payload={"payment_id": payment_id},
            )
        )
        await session.flush()
        session.add(Outbox(id=outbox_id, event_id=event_id, destination=destination))
    return event_id, outbox_id


class FakeBroker:
    def __init__(self, *, fail=False):
        self.calls = []
        self.fail = fail

    async def publish(self, body, **kwargs):
        self.calls.append((body, kwargs))
        if self.fail:
            raise ConnectionError("broker unavailable or confirmation lost")


async def test_broker_outage_keeps_event_until_recovery(outbox_db):
    event_id, outbox_id = await seed(outbox_db)
    broker = FakeBroker(fail=True)
    with pytest.raises(ConnectionError):
        await publish_once(outbox_db, broker)
    async with outbox_db() as session:
        assert (await session.get(Outbox, outbox_id)).published_at is None

    broker.fail = False
    assert await publish_once(outbox_db, broker) == 1
    assert await publish_once(outbox_db, broker) == 0
    async with outbox_db() as session:
        assert (await session.get(Outbox, outbox_id)).published_at is not None

    body, options = broker.calls[-1]
    assert body == {"event_id": event_id, "cycle": 0}
    assert options["message_id"] == event_id
    assert options["persist"] is True
    assert options["mandatory"] is True
    assert options["queue"].name == "payments.new"


async def test_lost_confirmation_republishes_identical_identity(outbox_db):
    await seed(outbox_db)
    broker = FakeBroker(fail=True)
    # Брокер мог принять сообщение до обрыва соединения. Следующая публикация
    # обязана сохранить идентификаторы, чтобы consumer распознал дубликат.
    with pytest.raises(ConnectionError):
        await publish_once(outbox_db, broker)
    broker.fail = False
    await publish_once(outbox_db, broker)
    first, second = broker.calls
    assert first[0] == second[0]
    assert first[1]["message_id"] == second[1]["message_id"]
    async with outbox_db() as session:
        assert len((await session.scalars(select(Outbox))).all()) == 1


@pytest.mark.parametrize(
    "destination", ["payments.webhook", "payments.new.dlq", "payments.webhook.dlq"]
)
async def test_each_stage_and_dlq_uses_the_same_durable_publisher(outbox_db, destination):
    event_id, _ = await seed(outbox_db, destination=destination)
    broker = FakeBroker()
    assert await publish_once(outbox_db, broker) == 1
    body, options = broker.calls[0]
    assert options["queue"].name == destination
    assert options["queue"].durable is True
    assert options["message_id"] == body["event_id"] == event_id
