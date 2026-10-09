from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def add(self, event: Outbox) -> None:
        self.session.add(event)

    async def get_pending_batch(self, limit: int, due_at: datetime) -> list[Outbox]:
        events = await self.session.scalars(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.next_attempt_at <= due_at)
            .order_by(Outbox.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(events)
