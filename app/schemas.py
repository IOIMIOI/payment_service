"""Нормализация запроса без округления денежных и JSON-значений."""

import json
import math
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, InvalidOperation
from typing import Any, Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator, model_validator


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


def _reject_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON number: {value}")


def decode_json(value: str | bytes) -> Any:
    # Стандартный float округляет уже при разборе JSON, еще до валидации суммы.
    # Decimal также сохраняет различия произвольных чисел внутри metadata.
    try:
        return json.loads(value, parse_float=Decimal, parse_constant=_reject_constant)
    except DecimalException as exc:
        raise ValueError("JSON number exponent exceeds the supported range") from exc


def _decimal_token(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("JSON numbers must be finite")
    if not value:
        return "0"
    # normalize() зависит от precision текущего decimal.Context и может округлить.
    # Работа с tuple сохраняет все цифры и делает 1, 1.0 и 1e0 эквивалентными.
    sign, digits, exponent = value.as_tuple()
    raw = "".join(str(digit) for digit in digits)
    raw = raw.lstrip("0") or "0"
    trimmed = raw.rstrip("0")
    exponent += len(raw) - len(trimmed)
    raw = trimmed
    prefix = "-" if sign else ""
    # Экспоненциальная запись не выделяет гигантскую строку для metadata=1e9999.
    return prefix + raw + (f"e{exponent}" if exponent else "")


def canonical_json(value: Any) -> str:
    """Точная, детерминированная сериализация JSON для БД и идемпотентности."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return _decimal_token(value)
    if isinstance(value, int):
        return _decimal_token(Decimal(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return _decimal_token(Decimal(str(value)))
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ",".join(canonical_json(item) for item in value) + "]"
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings")
        return (
            "{"
            + ",".join(
                canonical_json(key) + ":" + canonical_json(value[key]) for key in sorted(value)
            )
            + "}"
        )
    raise ValueError(f"Unsupported JSON value: {type(value).__name__}")


def iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    # SQLite в быстрых тестах теряет tzinfo; PostgreSQL хранит timestamptz.
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat()


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

    def canonical(self) -> str:
        # Дополнение к исходному ТЗ: сравниваются ВСЕ поля, включая webhook и
        # вложенные metadata. Отсутствующие необязательные поля равны их default.
        return canonical_json(
            {
                "amount": self.amount,
                "currency": self.currency,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": str(self.webhook_url),
            }
        )


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
