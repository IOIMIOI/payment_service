import asyncio
import json

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import Base, ProviderOperation
from app.provider import (
    Emulator,
    HTTPProvider,
    ProviderConflictError,
    ProviderTemporaryError,
    create_app,
)


@pytest_asyncio.fixture
async def provider_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'provider.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield engine, factory
    await engine.dispose()


async def no_wait(delay):
    await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sample", "expected"),
    [
        (0, "succeeded"),
        (0.899999, "succeeded"),
        (0.90, None),
        (0.949999, None),
        (0.95, "failed"),
        (0.999999, "failed"),
    ],
)
async def test_probability_boundaries_and_only_final_results_persist(provider_db, sample, expected):
    engine, factory = provider_db
    service = Emulator(factory, engine, sleep=no_wait, random_value=lambda: sample)
    if expected is None:
        with pytest.raises(ProviderTemporaryError):
            await service.charge("payment-key", "100.0", "RUB")
    else:
        assert (await service.charge("payment-key", "100.0", "RUB"))["status"] == expected
    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(ProviderOperation))
        assert count == (0 if expected is None else 1)


@pytest.mark.asyncio
async def test_lost_consumer_result_is_recovered_from_durable_provider_state(provider_db):
    engine, factory = provider_db
    first_service = Emulator(factory, engine, sleep=no_wait, random_value=lambda: 0.99)
    original = await first_service.charge("stable-key", "100.0", "RUB")

    # Consumer потерял ответ и перезапустился; новый экземпляр эмулятора читает БД.
    def unexpected_operation():
        pytest.fail("A completed payment must not be simulated again")

    restarted = Emulator(factory, engine, sleep=no_wait, random_value=unexpected_operation)
    assert await restarted.charge("stable-key", "100.00", "RUB") == original
    assert original["status"] == "failed"
    with pytest.raises(ProviderConflictError):
        await restarted.charge("stable-key", "101.00", "RUB")


@pytest.mark.asyncio
async def test_concurrent_provider_calls_create_one_operation(provider_db):
    engine, factory = provider_db
    samples = []
    delays = []

    async def simulated_delay(delay):
        delays.append(delay)
        await asyncio.sleep(0.01)

    def outcome():
        samples.append(1)
        return 0.1

    services = [
        Emulator(factory, engine, sleep=simulated_delay, random_value=outcome) for _ in range(8)
    ]
    responses = await asyncio.gather(
        *(service.charge("one-operation", "10", "EUR") for service in services)
    )
    assert all(result == responses[0] for result in responses)
    assert len(samples) == len(delays) == 1
    assert 2 <= delays[0] <= 5
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(ProviderOperation)) == 1


@pytest.mark.asyncio
async def test_temporary_failure_does_not_freeze_result(provider_db):
    engine, factory = provider_db
    outcomes = iter([0.92, 0.01])
    service = Emulator(factory, engine, sleep=no_wait, random_value=lambda: next(outcomes))
    with pytest.raises(ProviderTemporaryError):
        await service.charge("retry-key", "10", "USD")
    original = await service.charge("retry-key", "10.0", "USD")
    assert original["status"] == "succeeded"
    assert await service.charge("retry-key", "10.00", "USD") == original


@pytest.mark.asyncio
async def test_provider_http_authentication_and_idempotency(provider_db):
    engine, factory = provider_db
    service = Emulator(factory, engine, sleep=no_wait, random_value=lambda: 0.01)
    application = create_app(service, api_key="provider-secret")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://provider"
    ) as client:
        body = {"amount": "100.00", "currency": "RUB"}
        assert (
            await client.post("/charge", json=body, headers={"Idempotency-Key": "key"})
        ).status_code == 401
        headers = {"X-API-Key": "provider-secret", "Idempotency-Key": "key"}
        original = await client.post("/charge", json=body, headers=headers)
        assert original.status_code == 200
        assert (await client.post("/charge", json=body, headers=headers)).json() == original.json()
        assert (
            await client.post("/charge", json={**body, "currency": "USD"}, headers=headers)
        ).status_code == 409
        assert (await client.get("/openapi.json")).status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", ["0", "-1", "0.001", "100.001", "NaN", "Infinity"])
async def test_provider_rejects_invalid_amounts(provider_db, amount):
    engine, factory = provider_db
    application = create_app(Emulator(factory, engine, sleep=no_wait), api_key="secret")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://provider"
    ) as client:
        response = await client.post(
            "/charge",
            json={"amount": amount, "currency": "RUB"},
            headers={"X-API-Key": "secret", "Idempotency-Key": "key"},
        )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_http_adapter_preserves_provider_key_and_final_decline():
    requests = []
    response_data = {"status": "failed", "processed_at": "2026-10-08T12:00:00+00:00"}

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json=response_data)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        provider = HTTPProvider("http://provider", "secret", client=client)
        assert await provider.charge("constant", "10.00", "USD") == response_data
        assert await provider.charge("constant", "10.00", "USD") == response_data
    assert len(requests) == 2
    assert all(request.headers["Idempotency-Key"] == "constant" for request in requests)
    assert all(request.headers["X-API-Key"] == "secret" for request in requests)
    assert json.loads(requests[0].content) == {"amount": "10.00", "currency": "USD"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "body"),
    [
        (503, {}),
        (409, {}),
        (200, {}),
        (200, {"status": "pending", "processed_at": "2026-10-08T12:00:00Z"}),
        (200, {"status": "succeeded", "processed_at": "2026-10-08T12:00:00"}),
    ],
)
async def test_http_adapter_retries_uncertain_or_invalid_results(status_code, body):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status_code, json=body))
    ) as client:
        provider = HTTPProvider("http://provider", "secret", client=client)
        with pytest.raises(ProviderTemporaryError):
            await provider.charge("constant", "10.00", "USD")


@pytest.mark.asyncio
async def test_http_adapter_total_timeout_is_bounded():
    async def slow_response(request):
        await asyncio.sleep(1)
        return httpx.Response(
            200, json={"status": "succeeded", "processed_at": "2026-10-08T12:00:00Z"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_response)) as client:
        provider = HTTPProvider("http://provider", "secret", client=client, timeout=0.01)
        with pytest.raises(ProviderTemporaryError):
            await provider.charge("constant", "10.00", "USD")
