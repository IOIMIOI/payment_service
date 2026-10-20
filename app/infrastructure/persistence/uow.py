"""One transaction for payment, work state and outbox changes."""

from sqlalchemy.exc import IntegrityError

from app.application.ports import StorageConflict
from app.infrastructure.persistence.repositories import Events, Payments, WorkItems


class SqlAlchemyUnitOfWork:
    """Commit on successful context exit; roll back and close on every failure."""

    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def __aenter__(self):
        self.session = self.session_factory()
        try:
            await self.session.begin()
        except BaseException:
            await self.session.close()
            raise
        self.payments = Payments(self.session)
        self.work_items = WorkItems(self.session)
        self.outbox = Events(self.session)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                try:
                    await self.session.commit()
                except BaseException:
                    await self.session.rollback()
                    raise
            else:
                await self.session.rollback()
                if isinstance(exc, IntegrityError):
                    raise StorageConflict("Database constraint rejected the transaction") from exc
        except IntegrityError as error:
            raise StorageConflict("Database constraint rejected the transaction") from error
        finally:
            await self.session.close()
