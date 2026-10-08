"""Соединения и межпроцессная сериализация обработки одного события."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from weakref import WeakKeyDictionary

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.config import settings
from app.schemas import canonical_json, decode_json

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    json_serializer=canonical_json,
    json_deserializer=decode_json,
)
Session = async_sessionmaker(engine, expire_on_commit=False)
_test_locks: WeakKeyDictionary = WeakKeyDictionary()


@asynccontextmanager
async def event_lock(database: AsyncEngine, key: str):
    if database.dialect.name == "postgresql":
        lock_id = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big", signed=True)
        # Блокировка отдельной транзакции живёт через коммиты бизнес-сессий:
        # счётчик попытки фиксируется ДО сети, а дубль не запускает второй вызов.
        # xact-lock освобождается rollback/закрытием соединения даже при падении;
        # session-level lock мог бы случайно остаться на соединении из пула.
        async with database.connect() as connection, connection.begin():
            acquired = await connection.scalar(
                text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": lock_id}
            )
            yield bool(acquired)
    elif database.dialect.name == "sqlite":
        # SQLite — только быстрые тесты. В production защиту между процессами
        # обеспечивает PostgreSQL; локальный asyncio.Lock её не заменяет.
        locks = _test_locks.setdefault(database, {})
        lock = locks.setdefault(key, asyncio.Lock())
        if lock.locked():
            yield False
            return
        async with lock:
            yield True
    else:
        raise RuntimeError("Only PostgreSQL is supported in production")
