"""HTTP adapter for the payment provider; independent of the emulator server."""

import asyncio
from datetime import datetime
from typing import Literal

import httpx
from pydantic import BaseModel

from app.application.ports import ProviderTemporaryError
from app.config import get_settings


class ChargeResponse(BaseModel):
    status: Literal["succeeded", "failed"]
    processed_at: datetime


class HTTPProvider:
    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 15.0,
    ) -> None:
        settings = get_settings()
        self.url = (url if url is not None else settings.provider_url).rstrip("/")
        self.api_key = api_key if api_key is not None else settings.provider_api_key
        self.client = client
        self.timeout = timeout

    async def charge(self, key: str, amount: str, currency: str) -> dict[str, str]:
        async def send(client: httpx.AsyncClient) -> dict[str, str]:
            response = await client.post(
                self.url + "/charge",
                headers={"X-API-Key": self.api_key, "Idempotency-Key": key},
                json={"amount": amount, "currency": currency},
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = ChargeResponse.model_validate(response.json())
            if data.processed_at.tzinfo is None:
                raise ValueError("provider processed_at must have a timezone")
            return {"status": data.status, "processed_at": data.processed_at.isoformat()}

        try:
            # Потеря HTTP-ответа не доказывает отсутствие оплаты. Любая ошибка
            # транспорта повторяется с тем же ключом, а не помечает платёж failed.
            async with asyncio.timeout(self.timeout):
                if self.client is not None:
                    return await send(self.client)
                async with httpx.AsyncClient() as client:
                    return await send(client)
        except (httpx.HTTPError, ValueError, TimeoutError) as exc:
            raise ProviderTemporaryError(f"provider response unavailable: {exc}") from exc
