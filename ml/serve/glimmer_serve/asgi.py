"""A minimal ASGI app around endpoint.handle: one POST route, no framework,
no request logging."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from .endpoint import MAX_BODY_BYTES, ChatBackend, Settings, handle

Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]


def make_app(settings: Settings, backend: ChatBackend, path: str = "/infer"):
    async def app(scope: dict[str, Any], receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return
        if scope.get("method") != "POST" or scope.get("path") != path:
            await _reply(send, 404, b'{"error":"not found"}')
            return
        body = bytearray()
        while True:
            msg = await receive()
            body += msg.get("body", b"")
            if len(body) > MAX_BODY_BYTES:
                await _reply(send, 413, b'{"error":"request too large"}')
                return
            if not msg.get("more_body"):
                break
        auth = None
        for k, v in scope.get("headers", []):
            if k.lower() == b"authorization":
                auth = v.decode("latin-1")
        status, out = await handle(settings, backend, auth, bytes(body))
        await _reply(send, status, out)

    return app


async def _reply(send: Send, status: int, body: bytes) -> None:
    await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"application/json")]})
    await send({"type": "http.response.body", "body": body})
