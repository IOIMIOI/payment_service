"""Composition root: concrete adapters are wired here, never in use cases."""

from functools import partial

from app.application.payments import PaymentService
from app.application.processor import Processor
from app.infrastructure.http.provider import HTTPProvider
from app.infrastructure.http.webhook import HTTPWebhook
from app.infrastructure.persistence.db import Session, engine, event_lock
from app.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

uow_factory = partial(SqlAlchemyUnitOfWork, Session)


def get_payment_service() -> PaymentService:
    return PaymentService(uow_factory)


def create_processor() -> Processor:
    return Processor(
        uow_factory,
        partial(event_lock, engine),
        HTTPProvider(),
        HTTPWebhook(),
    )
