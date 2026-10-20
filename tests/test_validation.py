"""Границы денежной точности и семантика сравнения всего запроса."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.entrypoints.api.schemas import PaymentCreate
from app.shared.json import canonical_json, decode_json


def make_request(amount="100.00", **changes):
    data = {"amount": amount, "currency": "RUB", "webhook_url": "https://example.com/hook"}
    data.update(changes)
    return PaymentCreate.model_validate(data)


@pytest.mark.parametrize(
    "amount", ["0", "-0.01", "-100", "NaN", "Infinity", "-Infinity", True, None]
)
def test_amount_must_be_positive_finite_decimal(amount):
    with pytest.raises(ValidationError):
        make_request(amount)


@pytest.mark.parametrize(
    "amount", ["1.001", "0.001", "100.000000000000001", "1e-9999", "999999999999999999.991"]
)
def test_extra_precision_is_rejected_without_rounding(amount):
    with pytest.raises(ValidationError):
        make_request(amount)


@pytest.mark.parametrize("amount", [100, 100.0, "100.00", "100.0000", "1e2", Decimal("1.000e2")])
def test_equivalent_amounts_have_identical_canonical_request(amount):
    assert make_request(amount).canonical() == make_request("100.00").canonical()


def test_redundant_zero_precision_is_accepted():
    assert make_request("1.230000").amount == Decimal("1.23")
    assert make_request("0.0100").amount == Decimal("0.01")


def test_database_numeric_range_is_checked_before_insert():
    assert make_request("999999999999999999.99").amount == Decimal("999999999999999999.99")
    with pytest.raises(ValidationError):
        make_request("1000000000000000000.00")


def test_json_numbers_are_parsed_without_float_rounding():
    raw = decode_json(
        '{"amount":100.000000000000001,"currency":"RUB","webhook_url":"http://example.com"}'
    )
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(raw)


def test_nested_json_order_and_numeric_spelling_are_normalized():
    first = make_request(metadata=decode_json('{"b":[1,1.0,{"a":true,"b":null}],"a":1000.00}'))
    other = make_request(metadata=decode_json('{"a":1e3,"b":[1.00,1,{"b":null,"a":true}]}'))
    assert first.canonical() == other.canonical()


def test_metadata_order_types_and_precise_numeric_changes_are_significant():
    assert (
        make_request(metadata={"x": [1, 2]}).canonical()
        != make_request(metadata={"x": [2, 1]}).canonical()
    )
    assert (
        make_request(metadata={"x": True}).canonical()
        != make_request(metadata={"x": 1}).canonical()
    )
    assert (
        make_request(metadata={"x": "1"}).canonical() != make_request(metadata={"x": 1}).canonical()
    )
    first = make_request(metadata=decode_json('{"x":0.12345678901234567890123456789}'))
    other = make_request(metadata=decode_json('{"x":0.12345678901234567890123456788}'))
    assert first.canonical() != other.canonical()


def test_canonical_serializer_roundtrips_decimal_without_context_rounding():
    data = decode_json('{"x":123456789012345678901234567890.1200,"small":1e-9999,"zero":-0.0}')
    assert decode_json(canonical_json(data)) == data


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_nonstandard_json_constants_are_forbidden(constant):
    with pytest.raises(ValueError):
        decode_json('{"x":' + constant + "}")


def test_unknown_fields_and_invalid_currency_are_rejected():
    with pytest.raises(ValidationError):
        make_request(unknown="silently ignored fields would break idempotency")
    with pytest.raises(ValidationError):
        make_request(currency="JPY")


def test_documented_defaults_and_normalized_urls_currency():
    assert (
        make_request(currency=" rub ", webhook_url="HTTP://EXAMPLE.COM").canonical()
        == make_request(
            currency="RUB",
            webhook_url="http://example.com/",
            description="",
            metadata={},
        ).canonical()
    )


@pytest.mark.parametrize("character", ["\x00", "\ud800", "\udfff"])
@pytest.mark.parametrize("location", ["description", "metadata_value", "metadata_key", "url"])
def test_unsupported_text_is_rejected_before_storage(character, location):
    if location == "description":
        changes = {"description": "hello" + character + "world"}
    elif location == "metadata_value":
        changes = {"metadata": {"nested": [{"value": "hello" + character}]}}
    elif location == "metadata_key":
        changes = {"metadata": {"nested": [{"bad" + character: "value"}]}}
    else:
        changes = {"webhook_url": "http://example.com/" + character}
    with pytest.raises(ValidationError):
        make_request(**changes)


def test_valid_unicode_and_escaped_surrogate_pair_are_preserved():
    text = "Оплата \U0001f600 e\u0301\n\t"
    data = decode_json(
        '{"amount":1,"currency":"RUB","description":"\\ud83d\\ude00",'
        '"metadata":{"\\ud83d\\ude00":"\\ud83d\\ude00"},'
        '"webhook_url":"http://example.com/hook"}'
    )
    request = PaymentCreate.model_validate(data)
    assert request.description == "\U0001f600"
    assert request.metadata == {"\U0001f600": "\U0001f600"}
    assert make_request(description=text, metadata={text: text}).description == text
