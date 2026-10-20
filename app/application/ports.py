"""Only the capabilities used by the application; no framework dependencies."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol

from app.domain.entities import Outbox, Payment, WorkItem


class StorageConflict(Exception):
    """A database constraint rejected a transaction."""


class ProviderTemporaryError(Exception):
    """No definitive response; retry with the same provider key."""


class WebhookDeliveryError(Exception):
    pass


class Payments(Protocol):
    async def get(self, payment_id: str, *, lock: bool = False) -> Payment | None: ...
    async def by_key(self, key: str) -> Payment | None: ...
    async def add(self, payment: Payment) -> None: ...
    async def save(self, payment: Payment) -> None: ...


class WorkItems(Protocol):
    async def get(self, event_id: str) -> WorkItem | None: ...
    async def webhook_for(self, payment_id: str) -> WorkItem | None: ...
    async def dead(self) -> list[WorkItem]: ...
    async def add(self, item: WorkItem) -> None: ...
    async def save(self, item: WorkItem) -> None: ...


class Events(Protocol):
    async def add(self, event: Outbox) -> None: ...


class UnitOfWork(Protocol):
    payments: Payments
    work_items: WorkItems
    outbox: Events

    async def __aenter__(self) -> "UnitOfWork": ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...


UowFactory = Callable[[], UnitOfWork]
EventLock = Callable[[str], AbstractAsyncContextManager[bool]]


class Provider(Protocol):
    async def charge(self, key: str, amount: str, currency: str) -> dict[str, str]: ...


class Webhook(Protocol):
    async def send(self, url: str, payload: dict) -> None: ...
