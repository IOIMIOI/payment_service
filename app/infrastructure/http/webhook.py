"""Доставка неизменяемого уведомления; платёж этот модуль не обрабатывает."""

import asyncio

import httpx

from app.application.ports import WebhookDeliveryError
from app.shared.json import canonical_json


class HTTPWebhook:
    def __init__(self, *, client: httpx.AsyncClient | None = None, timeout: float = 5.0):
        self.client = client
        self.timeout = timeout

    async def send(self, url: str, payload: dict) -> None:
        async def post(client: httpx.AsyncClient) -> None:
            response = await client.post(
                url,
                content=canonical_json(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
                follow_redirects=False,
            )
            # Уточнение ТЗ: любой 2xx, включая 202/204; редирект не подтверждает
            # доставку по заданному адресу и не выполняется автоматически.
            if not 200 <= response.status_code < 300:
                raise WebhookDeliveryError(f"HTTP {response.status_code}")

        try:
            # httpx timeout отдельно ограничивает connect/read/write/pool.
            # Внешний deadline обеспечивает именно 5 секунд на ВСЮ попытку.
            async with asyncio.timeout(self.timeout):
                if self.client is not None:
                    await post(self.client)
                else:
                    async with httpx.AsyncClient() as client:
                        await post(client)
        except (httpx.HTTPError, TimeoutError) as exc:
            raise WebhookDeliveryError(f"Webhook unavailable: {exc}") from exc
