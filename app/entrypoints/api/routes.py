"""HTTP API: прием платежа и outbox в одной транзакции."""

import secrets
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.security import APIKeyHeader
from pydantic import AfterValidator

from app.application.payments import IdempotencyConflict, PaymentNotFound, PaymentService
from app.bootstrap import get_payment_service
from app.config import settings
from app.domain.entities import Payment
from app.entrypoints.api.json import DecimalJSONRoute, ExactJSONResponse
from app.entrypoints.api.schemas import (
    APIError,
    PaymentCreate,
    PaymentDetail,
    PaymentSummary,
    validate_storage_text,
)
from app.shared.time import iso_utc

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
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> Response:
    if not idempotency_key.strip():
        raise HTTPException(status_code=422, detail="Idempotency-Key must not be blank")
    try:
        result = await service.create(body.command(), idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ExactJSONResponse(
        payment_summary(result.payment),
        status_code=202 if result.created else 200,
    )


@router.get(
    "/api/v1/payments/{payment_id}",
    response_model=PaymentDetail,
    responses={404: {"model": APIError, "description": "Payment not found"}},
)
async def get_payment(
    payment_id: Annotated[str, AfterValidator(validate_storage_text)],
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> Response:
    try:
        payment = await service.get(payment_id)
    except PaymentNotFound as exc:
        raise HTTPException(status_code=404, detail="Payment not found") from exc
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
