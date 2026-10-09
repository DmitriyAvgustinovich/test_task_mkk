import asyncio
import json
import os
import time
from urllib.parse import quote, unquote, urlsplit
from uuid import uuid4

import httpx
import pytest
from faststream import AckPolicy
from faststream.rabbit import RabbitQueue
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.consumers.payments import PaymentConsumer
from app.core.config import Settings
from app.core.time import utcnow
from app.domain.enums import PaymentStatus
from app.integrations.rabbitmq import (
    DLQ,
    MAIN_QUEUE,
    PAYMENTS_EXCHANGE,
    RabbitEventPublisher,
    create_broker,
    declare_topology,
)
from app.integrations.webhooks import HttpWebhookSender
from app.main import create_app
from app.models import Base, Outbox, Payment
from app.services.outbox import OutboxRelay
from app.services.processing import PaymentProcessor

pytestmark = pytest.mark.integration


@pytest.fixture
async def infrastructure():
    database_url = os.getenv("TEST_DATABASE_URL")
    rabbit_url = os.getenv("TEST_RABBITMQ_URL")
    if not database_url or not rabbit_url:
        pytest.skip("Set TEST_DATABASE_URL and TEST_RABBITMQ_URL or use the Docker tests profile")

    token = uuid4().hex
    schema = f"test_{token}"
    vhost = f"test-{token}"
    admin_engine = create_async_engine(database_url)
    async with admin_engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(
        database_url,
        connect_args={"server_settings": {"search_path": schema}},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    parsed = urlsplit(rabbit_url)
    management_url = os.getenv(
        "TEST_RABBITMQ_MANAGEMENT_URL",
        f"http://{parsed.hostname}:15672",
    )
    username = unquote(parsed.username or "guest")
    password = unquote(parsed.password or "guest")
    async with httpx.AsyncClient(base_url=management_url, auth=(username, password)) as admin:
        (await admin.put(f"/api/vhosts/{vhost}", json={})).raise_for_status()
        (
            await admin.put(
                f"/api/permissions/{vhost}/{quote(username, safe='')}",
                json={"configure": ".*", "write": ".*", "read": ".*"},
            )
        ).raise_for_status()
        settings = Settings(
            api_key="integration-key",
            database_url=database_url,
            rabbitmq_url=f"{rabbit_url.rsplit('/', 1)[0]}/{vhost}",
            retry_base_delay_seconds=0.2,
            gateway_min_delay_seconds=0,
            gateway_max_delay_seconds=0,
        )
        broker = create_broker(settings)
        await broker.connect()
        await declare_topology(broker, settings)
        try:
            yield sessions, settings, broker
        finally:
            await broker.stop()
            (await admin.delete(f"/api/vhosts/{vhost}")).raise_for_status()
            await engine.dispose()
            async with admin_engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            await admin_engine.dispose()


class Gateway:
    def __init__(self):
        self.calls = 0

    async def process(self):
        self.calls += 1
        return PaymentStatus.SUCCEEDED


async def drive_until(sessions, relay, payment_id, predicate):
    async with asyncio.timeout(15):
        while True:
            await relay.publish_batch()
            async with sessions() as session:
                payment = await session.get(Payment, payment_id)
                if predicate(payment):
                    return payment
            await asyncio.sleep(0.05)


async def test_real_delivery_retry_dlq_and_concurrent_idempotency(
    infrastructure,
    payment_body,
):
    from uuid import UUID

    sessions, settings, broker = infrastructure
    gateway = Gateway()
    attempts: dict[str, list[float]] = {}

    def webhook(request):
        payload = json.loads(request.content)
        seen = attempts.setdefault(payload["payment_id"], [])
        seen.append(time.monotonic())
        fail_always = payload["metadata"].get("fail_always", False)
        return httpx.Response(503 if fail_always or len(seen) == 1 else 200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        handler = PaymentConsumer(PaymentProcessor(sessions, gateway, HttpWebhookSender(http)))
        broker.subscriber(
            MAIN_QUEUE,
            PAYMENTS_EXCHANGE,
            ack_policy=AckPolicy.MANUAL,
            no_reply=True,
        )(handler.handle)
        await broker.start()
        relay = OutboxRelay(sessions, RabbitEventPublisher(broker), settings)
        api = create_app(settings, sessions)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api),
            base_url="http://test",
            headers={"X-API-Key": "integration-key"},
        ) as client:
            responses = await asyncio.gather(
                *[
                    client.post(
                        "/api/v1/payments",
                        json=payment_body,
                        headers={"Idempotency-Key": "parallel-postgres"},
                    )
                    for _ in range(12)
                ]
            )
            assert all(r.status_code == 202 for r in responses)
            assert len({r.json()["payment_id"] for r in responses}) == 1
            payment_id = UUID(responses[0].json()["payment_id"])
            async with sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Payment)) == 1
                assert await session.scalar(select(func.count()).select_from(Outbox)) == 1

            counts = await asyncio.gather(relay.publish_batch(), relay.publish_batch())
            assert sum(counts) == 1
            payment = await drive_until(
                sessions,
                relay,
                payment_id,
                lambda p: p.webhook_sent_at is not None,
            )
            assert payment.processing_attempts == 2
            assert gateway.calls == 1
            assert len(attempts[str(payment_id)]) == 2
            assert attempts[str(payment_id)][1] - attempts[str(payment_id)][0] >= 0.18

            await broker.publish(
                {"payment_id": str(payment_id), "attempt": 1},
                exchange=PAYMENTS_EXCHANGE,
                routing_key=MAIN_QUEUE.name,
                persist=True,
            )

            bad = await client.post(
                "/api/v1/payments",
                json=payment_body | {"metadata": {"fail_always": True}},
                headers={"Idempotency-Key": "dlq"},
            )
            bad_id = UUID(bad.json()["payment_id"])
            payment = await drive_until(
                sessions,
                relay,
                bad_id,
                lambda p: p.processing_attempts == 3,
            )
            assert payment.status == PaymentStatus.SUCCEEDED
            assert payment.webhook_sent_at is None
            assert gateway.calls == 2
            timestamps = attempts[str(bad_id)]
            assert len(timestamps) == 3
            assert timestamps[1] - timestamps[0] >= 0.18
            assert timestamps[2] - timestamps[1] >= 0.38
            await relay.publish_batch()
            queue = await broker.declare_queue(DLQ)
            async with asyncio.timeout(5):
                while (dead := await queue.get(fail=False)) is None:
                    await asyncio.sleep(0.05)
            assert json.loads(dead.body)["payment_id"] == str(bad_id)
            await dead.ack()

            await broker.publish(
                {"payment_id": "not-a-uuid"},
                exchange=PAYMENTS_EXCHANGE,
                routing_key=MAIN_QUEUE.name,
                persist=True,
            )
            async with asyncio.timeout(5):
                while (poison := await queue.get(fail=False)) is None:
                    await asyncio.sleep(0.05)
            assert json.loads(poison.body)["payment_id"] == "not-a-uuid"
            await poison.ack()


async def test_unroutable_outbox_event_is_retained_until_route_recovers(infrastructure):
    sessions, settings, broker = infrastructure
    routing_key = f"payments.unbound.{uuid4().hex}"
    payload = {"payment_id": str(uuid4()), "attempt": 1}
    async with sessions() as session, session.begin():
        event = Outbox(event_key=routing_key, routing_key=routing_key, payload=payload)
        session.add(event)
        await session.flush()
        event_id = event.id

    relay = OutboxRelay(sessions, RabbitEventPublisher(broker), settings)
    assert await relay.publish_batch() == 0
    async with sessions() as session:
        stored = await session.get(Outbox, event_id)
        assert stored.published_at is None
        assert stored.publish_attempts == 1
        assert stored.last_error

    queue = await broker.declare_queue(RabbitQueue(routing_key, durable=True))
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    await queue.bind(exchange, routing_key=routing_key)
    async with sessions() as session, session.begin():
        stored = await session.get(Outbox, event_id)
        stored.next_attempt_at = utcnow()

    assert await relay.publish_batch() == 1
    async with sessions() as session:
        stored = await session.get(Outbox, event_id)
        assert stored.published_at is not None
        assert stored.publish_attempts == 2
        assert stored.last_error is None
    async with asyncio.timeout(5):
        while (message := await queue.get(fail=False)) is None:
            await asyncio.sleep(0.05)
    assert json.loads(message.body) == payload
    assert message.message_id == str(event_id)
    assert message.delivery_mode == 2
    await message.ack()
