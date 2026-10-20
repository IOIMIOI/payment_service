"""Decimal-preserving JSON input and output at the HTTP boundary."""

from typing import Any

from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.responses import JSONResponse

from app.shared.json import canonical_json, decode_json


class ExactJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return canonical_json(content).encode("utf-8")


class DecimalJSONRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request) -> Response:
            if request.method == "POST":
                body = await request.body()
                if body:
                    try:
                        request._json = decode_json(body)
                    except (ValueError, UnicodeDecodeError) as exc:
                        raise RequestValidationError(
                            [
                                {
                                    "type": "json_invalid",
                                    "loc": ("body",),
                                    "msg": "Invalid JSON",
                                    "input": None,
                                }
                            ]
                        ) from exc
            return await original(request)

        return handler
