"""HTTP-контракт одной попытки доставки; счётчики/повторы проверяет worker."""

import asyncio
import json

import httpx
import pytest

from app.webhook import HTTPWebhook, WebhookDeliveryError

pytestmark = pytest.mark.asyncio


def notification():
    return {
        "event_id": "7b1c53c8-793e-408a-9541-0d584e09a51d",
        "payment_id": "e0e7aa99-1079-45b0-8944-dcaea0884023",
        "status": "succeeded",
        "amount": "100.00",
        "currency": "RUB",
        "processed_at": "2026-10-08T12:00:00+00:00",
    }


@pytest.mark.parametrize("status", [200, 201, 202, 204, 206, 299])
async def test_every_2xx_confirms_delivery(status):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status)),
    ) as client:
        await HTTPWebhook(client=client).send("http://receiver/hook", notification())


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_redirect_is_failed_delivery_and_is_not_followed(status):
    requests = []

    def redirect(request):
        requests.append(request)
        return httpx.Response(status, headers={"Location": "http://other/redirected"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(redirect),
        follow_redirects=True,
    ) as client:
        with pytest.raises(WebhookDeliveryError, match=f"HTTP {status}"):
            await HTTPWebhook(client=client).send("http://receiver/hook", notification())
    assert len(requests) == 1
    assert str(requests[0].url) == "http://receiver/hook"


@pytest.mark.parametrize("status", [400, 401, 404, 408, 429, 500, 503])
async def test_non_2xx_raises_delivery_error(status):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status)),
    ) as client:
        with pytest.raises(WebhookDeliveryError, match=f"HTTP {status}"):
            await HTTPWebhook(client=client).send("http://receiver/hook", notification())


async def test_repeated_posts_preserve_exact_body_and_default_timeout():
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(204)

    payload = notification()
    original = dict(payload)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        adapter = HTTPWebhook(client=client)
        assert adapter.timeout == 5.0
        await adapter.send("http://receiver/hook", payload)
        await adapter.send("http://receiver/hook", payload)
    assert payload == original
    assert requests[0].content == requests[1].content
    assert json.loads(requests[0].content) == original
    for request in requests:
        assert request.method == "POST"
        assert request.headers["Content-Type"] == "application/json"
        assert request.extensions["timeout"] == {
            "connect": 5.0,
            "read": 5.0,
            "write": 5.0,
            "pool": 5.0,
        }


async def test_total_deadline_bounds_a_stalled_attempt():
    async def never_finishes_in_time(request):
        await asyncio.sleep(10)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(never_finishes_in_time)) as client:
        with pytest.raises(WebhookDeliveryError):
            await asyncio.wait_for(
                HTTPWebhook(client=client, timeout=0.01).send(
                    "http://receiver/hook", notification()
                ),
                timeout=0.5,
            )


async def test_total_deadline_also_covers_response_body():
    class SlowBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"first bytes"
            await asyncio.sleep(10)
            yield b"last bytes"

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=SlowBody())),
    ) as client:
        with pytest.raises(WebhookDeliveryError):
            await asyncio.wait_for(
                HTTPWebhook(client=client, timeout=0.01).send(
                    "http://receiver/hook", notification()
                ),
                timeout=0.5,
            )


@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ReadTimeout])
async def test_transport_failure_can_be_retried_by_worker(error_type):
    def broken_network(request):
        raise error_type("connection failed", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(broken_network)) as client:
        with pytest.raises(WebhookDeliveryError):
            await HTTPWebhook(client=client).send("http://receiver/hook", notification())
