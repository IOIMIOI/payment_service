"""Отдельный HTTP-эмулятор провайдера с собственной фиксацией результата."""

from __future__ import annotations

import asyncio
import hmac
import json
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from app.application.ports import ProviderTemporaryError
from app.config import get_settings
from app.infrastructure.persistence.db import Session, engine, event_lock
from app.infrastructure.persistence.models import ProviderOperation, utcnow
from app.infrastructure.persistence.repositories import ProviderOperations


class ProviderConflictError(Exception):
    """Провайдер уже видел ключ с другим содержимым операции."""


def _canonical_request(amount: str, currency: str) -> str:
    value = Decimal(str(amount))
    if not value.is_finite() or value <= 0 or value != value.quantize(Decimal("0.01")):
        raise ValueError("amount must be positive and have at most two decimal places")
    if currency not in {"RUB", "USD", "EUR"}:
        raise ValueError("unsupported currency")
    return json.dumps(
        {"amount": format(value, ".2f"), "currency": currency},
        sort_keys=True,
        separators=(",", ":"),
    )


def _response(operation: ProviderOperation) -> dict[str, str]:
    processed_at = operation.processed_at
    # SQLite в тестах не сохраняет timezone=True; HTTP-контракт всегда содержит UTC.
    if processed_at.tzinfo is None:
        processed_at = processed_at.replace(tzinfo=timezone.utc)
    return {"status": operation.status, "processed_at": processed_at.isoformat()}


class Emulator:
    def __init__(
        self,
        session_factory,
        engine,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_value: Callable[[], float] = random.random,
        delay_min: float = 2,
        delay_max: float = 5,
    ) -> None:
        self.session_factory = session_factory
        self.engine = engine
        self.sleep = sleep
        self.random_value = random_value
        self.delay_min = delay_min
        self.delay_max = delay_max

    async def charge(self, key: str, amount: str, currency: str) -> dict[str, str]:
        canonical = _canonical_request(amount, currency)
        while True:
            # Блокировка действует между процессами провайдера. Повтор сначала читает
            # зафиксированный результат, поэтому не запускает новую случайную операцию.
            async with event_lock(self.engine, "provider:" + key) as acquired:
                if acquired:
                    async with self.session_factory() as session:
                        operations = ProviderOperations(session)
                        existing = await operations.get(key)
                        if existing is not None:
                            if existing.request_canonical != canonical:
                                raise ProviderConflictError(
                                    "Idempotency-Key belongs to another operation"
                                )
                            return _response(existing)

                        await self.sleep(random.uniform(self.delay_min, self.delay_max))
                        outcome = self.random_value()
                        # Дополнение заменяет 90/10 из исходного ТЗ на 90/5/5:
                        # временный сбой не является окончательным отказом и не сохраняется.
                        if 0.90 <= outcome < 0.95:
                            raise ProviderTemporaryError("temporary provider failure")
                        operation = ProviderOperation(
                            provider_key=key,
                            request_canonical=canonical,
                            status="succeeded" if outcome < 0.90 else "failed",
                            processed_at=utcnow(),
                        )
                        await operations.add(operation)
                        # Провайдер фиксирует результат ДО HTTP-ответа в отдельной
                        # транзакции. Падение consumer после оплаты не стирает этот факт.
                        await session.commit()
                        return _response(operation)
            # Второй HTTP-запрос ожидает первого, а не превращает конкуренцию в отказ.
            await asyncio.sleep(0.05)


class ChargeRequest(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=20, decimal_places=2)
    currency: Literal["RUB", "USD", "EUR"]


class ChargeResponse(BaseModel):
    status: Literal["succeeded", "failed"]
    processed_at: datetime


def create_app(emulator: Emulator | None = None, api_key: str | None = None) -> FastAPI:
    settings = get_settings()
    expected_key = api_key if api_key is not None else settings.provider_api_key
    service = (
        emulator
        if emulator is not None
        else Emulator(
            Session,
            engine,
            delay_min=settings.provider_delay_min,
            delay_max=settings.provider_delay_max,
        )
    )

    async def authenticate(x_api_key: Annotated[str | None, Header()] = None) -> None:
        if x_api_key is None or not hmac.compare_digest(x_api_key.encode(), expected_key.encode()):
            raise HTTPException(status_code=401, detail="Invalid API key")

    provider_app = FastAPI(
        title="Durable payment provider emulator",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        dependencies=[Depends(authenticate)],
    )

    @provider_app.post("/charge", response_model=ChargeResponse)
    async def charge(
        request: ChargeRequest,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=100)],
    ) -> dict[str, str]:
        try:
            return await service.charge(idempotency_key, str(request.amount), request.currency)
        except ProviderConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ProviderTemporaryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    return provider_app


app = create_app()
