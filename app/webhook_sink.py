"""Приёмник для примеров: контейнерный URL http://webhook-sink:8002/webhooks."""

import logging

from fastapi import FastAPI, Request, Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(title="Development webhook receiver", docs_url=None, redoc_url=None, openapi_url=None)


@app.post("/webhooks", status_code=204)
async def receive(request: Request) -> Response:
    # Это внешняя клиентская заглушка, а не эндпоинт платёжного API.
    # При повторах клиент должен дедуплицировать бизнес-эффекты по event_id.
    logger.info("Webhook received: %s", await request.json())
    return Response(status_code=204)
