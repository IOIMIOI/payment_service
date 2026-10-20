"""Exact JSON codec shared by HTTP and database adapters."""

import json
import math
from decimal import Decimal, DecimalException
from typing import Any


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
