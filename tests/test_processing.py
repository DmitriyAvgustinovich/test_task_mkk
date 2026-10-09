import json
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from app.core.exceptions import PermanentEventError
from app.domain.enums import PaymentStatus
from app.integrations.gateway import PaymentGateway
from app.integrations.webhooks import HttpWebhookSender
from app.models import Outbox, Payment
from app.schemas.events import PaymentEvent
from app.services.processing import PaymentProcessor


async def make_payment(client, body):
    response = await client.post(
        "/api/v1/payments",
        json=body,
        headers={"Idempotency-Key": "processor"},
    )
    return PaymentEvent(payment_id=response.json()["payment_id"])


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_result_and_duplicate_delivery(client, sessions, payment_body, result):
    event = await make_payment(client, payment_body)
    gateway = AsyncMock()
    gateway.process.return_value = result
    received = []

    def webhook(request):
        received.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        processor = PaymentProcessor(sessions, gateway, HttpWebhookSender(http))
        await processor.process(event)
        await processor.process(event)
    gateway.process.assert_awaited_once()
    assert len(received) == 1
    payload = json.loads(received[0].content)
    assert payload["status"] == result.value
    assert payload["amount"] == "1250.50"
    assert received[0].headers["Idempotency-Key"] == payload["event_id"]
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == result
        assert payment.processed_at is not None
        assert payment.webhook_sent_at is not None
        assert payment.processing_attempts == 1


async def test_three_failures_create_retries_and_dlq(client, sessions, payment_body):
    event = await make_payment(client, payment_body)
    gateway = AsyncMock()
    gateway.process.return_value = PaymentStatus.SUCCEEDED
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(503)),
    ) as http:
        processor = PaymentProcessor(sessions, gateway, HttpWebhookSender(http))
        for attempt in (1, 2, 3):
            event.attempt = attempt
            with pytest.raises(httpx.HTTPStatusError) as error:
                await processor.process(event)
            await processor.record_failure(event, error.value)

            await processor.record_failure(event, error.value)
            await processor.process(event)
    gateway.process.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == PaymentStatus.SUCCEEDED
        assert payment.webhook_sent_at is None
        assert payment.processing_attempts == 3
        routes = list(await session.scalars(select(Outbox.routing_key).order_by(Outbox.created_at)))
        assert routes == ["payments.new", "payments.retry.2", "payments.retry.3", "payments.dlq"]
        dead = await session.scalar(select(Outbox).where(Outbox.routing_key == "payments.dlq"))
        assert dead.payload["attempt"] == 3
        assert "503" in dead.payload["error"]


async def test_webhook_retry_resumes_saved_result(client, sessions, payment_body):
    event = await make_payment(client, payment_body)
    gateway = AsyncMock()
    gateway.process.return_value = PaymentStatus.SUCCEEDED
    requests = []

    def webhook(request):
        requests.append(request)
        return httpx.Response(500 if len(requests) == 1 else 204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        processor = PaymentProcessor(sessions, gateway, HttpWebhookSender(http))
        with pytest.raises(httpx.HTTPStatusError) as error:
            await processor.process(event)
        await processor.record_failure(event, error.value)

        restarted = PaymentProcessor(sessions, gateway, HttpWebhookSender(http))
        await restarted.process(event.model_copy(update={"attempt": 2}))
    gateway.process.assert_awaited_once()
    assert requests[0].content == requests[1].content
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.processing_attempts == 2
        assert payment.webhook_sent_at is not None
        assert payment.last_error is None


async def test_timeout_is_retried(client, sessions, payment_body):
    event = await make_payment(client, payment_body)
    gateway = AsyncMock()
    gateway.process.return_value = PaymentStatus.FAILED

    def webhook(request):
        raise httpx.ReadTimeout("timeout", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        processor = PaymentProcessor(sessions, gateway, HttpWebhookSender(http))
        with pytest.raises(httpx.ReadTimeout) as error:
            await processor.process(event)
        await processor.record_failure(event, error.value)
    async with sessions() as session:
        assert await session.scalar(select(Outbox).where(Outbox.routing_key == "payments.retry.2"))


async def test_missing_payment_is_permanent(sessions):
    async with httpx.AsyncClient() as http:
        processor = PaymentProcessor(sessions, AsyncMock(), HttpWebhookSender(http))
        with pytest.raises(PermanentEventError):
            await processor.process(PaymentEvent(payment_id=uuid4()))


async def test_gateway_probability_boundaries(settings):
    settings.gateway_success_rate = 1
    assert await PaymentGateway(settings).process() == PaymentStatus.SUCCEEDED
    settings.gateway_success_rate = 0
    assert await PaymentGateway(settings).process() == PaymentStatus.FAILED


async def test_failure_rolls_back_if_outbox_insert_fails(client, sessions, payment_body):
    event = await make_payment(client, payment_body)
    async with sessions() as session, session.begin():
        session.add(
            Outbox(
                event_key=f"payment:{event.payment_id}:attempt:2",
                routing_key="payments.retry.2",
                payload={"payment_id": str(event.payment_id), "attempt": 2},
            )
        )
    async with httpx.AsyncClient() as http:
        processor = PaymentProcessor(sessions, AsyncMock(), HttpWebhookSender(http))
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError):
            await processor.record_failure(event, RuntimeError("failure"))
    async with sessions() as session:
        payment = await session.get(Payment, UUID(str(event.payment_id)))
        assert payment.processing_attempts == 0
        assert payment.last_error is None
