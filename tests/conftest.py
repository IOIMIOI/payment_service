import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import Base
from app.schemas import canonical_json, decode_json


@pytest_asyncio.fixture
async def engine(tmp_path):
    database = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'test.db'}",
        json_serializer=canonical_json,
        json_deserializer=decode_json,
        connect_args={"timeout": 20},
    )

    @event.listens_for(database.sync_engine, "connect")
    def enable_foreign_keys(connection, _):
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield database
    await database.dispose()


@pytest_asyncio.fixture
async def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False)
