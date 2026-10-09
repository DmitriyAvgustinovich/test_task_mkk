import asyncio
import logging
from contextlib import asynccontextmanager, suppress

import httpx
from faststream import AckPolicy, FastStream

from app.consumers.payments import PaymentConsumer
from app.core.config import Settings, get_settings
from app.db.session import create_database
from app.integrations.gateway import PaymentGateway
from app.integrations.rabbitmq import (
    MAIN_QUEUE,
    PAYMENTS_EXCHANGE,
    RabbitEventPublisher,
    create_broker,
    declare_topology,
)
from app.integrations.webhooks import HttpWebhookSender
from app.services.outbox import OutboxRelay
from app.services.processing import PaymentProcessor


def create_worker(settings: Settings | None = None) -> FastStream:
    settings = settings or get_settings()
    engine, sessions = create_database(settings.database_url)
    broker = create_broker(settings)
    client = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds, follow_redirects=False)
    processor = PaymentProcessor(sessions, PaymentGateway(settings), HttpWebhookSender(client))
    consumer = PaymentConsumer(processor)
    relay = OutboxRelay(sessions, RabbitEventPublisher(broker), settings)
    relay_task: asyncio.Task | None = None

    @asynccontextmanager
    async def lifespan():
        try:
            async with client:
                yield
        finally:
            await engine.dispose()

    app = FastStream(broker, lifespan=lifespan)
    broker.subscriber(
        MAIN_QUEUE,
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        no_reply=True,
    )(consumer.handle)

    @app.after_startup
    async def start_relay():
        nonlocal relay_task
        await declare_topology(broker, settings)
        relay_task = asyncio.create_task(relay.run())

    @app.on_shutdown
    async def stop_relay():
        if relay_task is not None:
            relay_task.cancel()
            with suppress(asyncio.CancelledError):
                await relay_task

    return app


async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    await create_worker(settings).run()


if __name__ == "__main__":
    asyncio.run(main())
