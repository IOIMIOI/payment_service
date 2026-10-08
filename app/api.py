"""HTTP API: прием платежа и outbox в одной транзакции."""

import secrets
from collections.abc import AsyncIterator
from typing import Annotated, Any
from uuid import uuid4

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from fastapi.security import APIKeyHeader
from pydantic import AfterValidator
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import JSONResponse

from app.config import settings
from app.db import Session
from app.models import Outbox, Payment, WorkItem, utcnow
from app.schemas import (
    APIError,
    PaymentCreate,
    PaymentDetail,
    PaymentSummary,
    canonical_json,
    decode_json,
    iso_utc,
    validate_storage_text,
)


class ExactJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return canonical_json(content).encode("utf-8")


class DecimalJSONRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request) -> Response:
            if request.method == "POST":
                body = await request.body()
                if body:
                    try:
                        request._json = decode_json(body)
                    except (ValueError, UnicodeDecodeError) as exc:
                        raise RequestValidationError(
                            [
                                {
                                    "type": "json_invalid",
                                    "loc": ("body",),
                                    "msg": "Invalid JSON",
                                    "input": None,
                                }
                            ]
                        ) from exc
            return await original(request)

        return handler


async def get_session() -> AsyncIterator[AsyncSession]:
    async with Session() as session:
        yield session


key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(key: Annotated[str | None, Depends(key_header)]) -> None:
    if key is None or not secrets.compare_digest(key.encode(), settings.api_key.encode()):
        raise HTTPException(status_code=401, detail="Invalid API key")


# Все доступные HTTP endpoints защищены ключом. Документация отключена, чтобы
# /docs и /openapi.json не нарушали требование исходного ТЗ об аутентификации.
app = FastAPI(
    title="Asynchronous payment processing", docs_url=None, redoc_url=None, openapi_url=None
)
router = APIRouter(
    route_class=DecimalJSONRoute,
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": APIError, "description": "Missing or invalid API key"}},
)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> Response:
    # Стандартный encoder пытается преобразовать Decimal из rejected input в
    # float/int; например 1e9999 может превратить ожидаемый 422 в ошибку сервера.
    # Для клиента достаточно пути поля и причины без отражения всего запроса.
    return ExactJSONResponse(
        {
            "detail": [
                {"type": error["type"], "loc": list(error["loc"]), "msg": error["msg"]}
                for error in exc.errors()
            ]
        },
        status_code=422,
    )


def payment_summary(payment: Payment) -> dict[str, Any]:
    return {
        "payment_id": payment.id,
        "status": payment.status,
        "created_at": iso_utc(payment.created_at),
    }


@router.post(
    "/api/v1/payments",
    status_code=202,
    response_model=PaymentSummary,
    responses={
        200: {"model": PaymentSummary, "description": "Existing payment with current status"},
        409: {"model": APIError, "description": "Same key with different normalized request"},
    },
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255),
        AfterValidator(validate_storage_text),
    ],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    if not idempotency_key.strip():
        raise HTTPException(status_code=422, detail="Idempotency-Key must not be blank")
    canonical = body.canonical()
    payment_id, event_id = str(uuid4()), str(uuid4())
    payment = Payment(
        id=payment_id,
        idempotency_key=idempotency_key,
        request_canonical=canonical,
        provider_key=f"payment:{payment_id}",
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata_json=body.metadata,
        webhook_url=str(body.webhook_url),
        status="pending",
        created_at=utcnow(),
    )
    try:
        # Никаких обращений к провайдеру/брокеру до commit. Уникальный INSERT
        # сериализует конкурирующие одинаковые ключи на уровне PostgreSQL.
        async with session.begin():
            session.add(payment)
            await session.flush()
            session.add(
                WorkItem(
                    event_id=event_id,
                    payment_id=payment_id,
                    kind="payment",
                    payload={"payment_id": payment_id},
                    state="ready",
                    attempts=0,
                    cycle=0,
                )
            )
            await session.flush()
            session.add(
                Outbox(
                    id=str(uuid4()),
                    event_id=event_id,
                    destination="payments.new",
                    cycle=0,
                )
            )
    except IntegrityError:
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:
            raise  # Другая ошибка целостности не маскируется под идемпотентность.
        if existing.request_canonical != canonical:
            raise HTTPException(
                status_code=409, detail="Idempotency-Key was used with different data"
            )
        # Исходное ТЗ указывало только 202. Уточнение требует 200 и актуальный
        # статус существующего платежа, а не закэшированный ответ первого POST.
        return ExactJSONResponse(payment_summary(existing), status_code=200)
    return ExactJSONResponse(payment_summary(payment), status_code=202)


@router.get(
    "/api/v1/payments/{payment_id}",
    response_model=PaymentDetail,
    responses={404: {"model": APIError, "description": "Payment not found"}},
)
async def get_payment(
    payment_id: Annotated[str, AfterValidator(validate_storage_text)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return ExactJSONResponse(
        {
            **payment_summary(payment),
            "amount": format(payment.amount, ".2f"),
            "currency": payment.currency,
            "description": payment.description,
            "metadata": payment.metadata_json,
            "webhook_url": payment.webhook_url,
            "processed_at": iso_utc(payment.processed_at),
        }
    )


app.include_router(router)
