import asyncio
import logging
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.time import utcnow
from app.domain.ports import EventPublisher
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        settings: Settings,
    ):
        self.session_factory = session_factory
        self.publisher = publisher
        self.settings = settings

    async def publish_batch(self) -> int:
        published = 0
        async with self.session_factory() as session, session.begin():
            events = await OutboxRepository(session).get_pending_batch(
                self.settings.outbox_batch_size,
                utcnow(),
            )
            for event in events:
                event.publish_attempts += 1
                try:
                    await self.publisher.publish(event.payload, event.routing_key, event.id)
                except Exception as exc:
                    event.last_error = str(exc)[:2000]
                    event.next_attempt_at = utcnow() + timedelta(
                        seconds=min(60, 2 ** min(event.publish_attempts - 1, 6))
                    )
                    logger.warning("Outbox publication failed event_id=%s: %s", event.id, exc)
                else:
                    event.published_at = utcnow()
                    event.last_error = None
                    published += 1
        return published

    async def run(self) -> None:
        while True:
            try:
                await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed; unpublished rows will be retried")
            await asyncio.sleep(self.settings.outbox_poll_interval_seconds)
