"""Нормализация запроса без округления денежных и JSON-значений."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator, model_validator

from app.application.payments import CreatePayment
from app.shared.json import canonical_json


def validate_storage_text(value: str) -> str:
    # PostgreSQL TEXT не хранит NUL, а UTF-8 не кодирует непарные суррогаты.
    # Отклоняем их с 422 до SQL, а не удаляем: изменение строки могло бы
    # незаметно изменить данные и семантику сравнения Idempotency-Key.
    if "\x00" in value or any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError("Strings must not contain NUL or unpaired Unicode surrogates")
    return value


def validate_json_text(value: Any) -> None:
    if isinstance(value, str):
        validate_storage_text(value)
    elif isinstance(value, dict):
        for key, item in value.items():
            validate_json_text(key)
            validate_json_text(item)
    elif isinstance(value, list):
        for item in value:
            validate_json_text(item)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl

    @model_validator(mode="before")
    @classmethod
    def validate_request_text(cls, value: Any) -> Any:
        # Проверка до полевых валидаторов охватывает metadata любой вложенности
        # и неизвестные ключи. Иначе суррогат из ключа попадёт в error.loc,
        # и сама сериализация ответа 422 завершится ошибкой кодирования.
        # Корректную JSON-пару \ud83d\ude00 парсер уже превратил в Unicode emoji.
        validate_json_text(value)
        return value

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, value: Any) -> Decimal:
        if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
            raise ValueError("amount must be a decimal number")
        try:
            amount = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError("amount must be a decimal number") from exc
        if not amount.is_finite() or amount <= 0:
            raise ValueError("amount must be finite and strictly positive")
        if amount > Decimal("999999999999999999.99"):
            raise ValueError("amount exceeds the supported NUMERIC(20,2) range")
        _, digits, exponent = amount.as_tuple()
        if exponent < -2 and any(digits[max(0, len(digits) + exponent + 2) :]):
            raise ValueError("amount must not require more than two decimal places")
        # Проверка выше выполнена до quantize: 1.001 запрещен, а 1.2300 допустим.
        return amount.quantize(Decimal("0.01"))

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value: Any) -> Any:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        canonical_json(value)
        return value

    def command(self) -> CreatePayment:
        return CreatePayment(
            amount=self.amount,
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            webhook_url=str(self.webhook_url),
        )

    def canonical(self) -> str:
        return self.command().canonical()


class PaymentSummary(BaseModel):
    """Документируем ответы, сохраняя Decimal-aware сериализацию в HTTP-слое."""

    payment_id: str
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetail(PaymentSummary):
    amount: str = Field(pattern=r"^\d+\.\d{2}$", examples=["100.00"])
    currency: Literal["RUB", "USD", "EUR"]
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    processed_at: datetime | None


class APIError(BaseModel):
    detail: str
