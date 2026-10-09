from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from faststream import AckPolicy
from faststream.rabbit import TestRabbitBroker as RabbitTestBroker

from app.consumers import payments as consumer_module
from app.consumers.payments import PaymentConsumer
from app.core.exceptions import PermanentEventError
from app.integrations.rabbitmq import MAIN_QUEUE, PAYMENTS_EXCHANGE, create_broker


async def test_handler_via_faststream(settings):
    processor = AsyncMock()
    consumer = PaymentConsumer(processor)
    broker = create_broker(settings)
    broker.subscriber(
        MAIN_QUEUE,
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        no_reply=True,
    )(consumer.handle)
    async with RabbitTestBroker(broker, connect_only=False) as broker:
        await broker.publish(
            {"payment_id": str(uuid4()), "attempt": 1},
            queue=MAIN_QUEUE,
            exchange=PAYMENTS_EXCHANGE,
        )
    processor.process.assert_awaited_once()


async def test_ack_after_retry_commit():
    processor = AsyncMock()
    error = RuntimeError("webhook down")
    processor.process.side_effect = error
    consumer = PaymentConsumer(processor)
    message = AsyncMock()
    order = []
    processor.record_failure.side_effect = lambda *_: order.append("committed")
    message.ack.side_effect = lambda: order.append("acked")
    await consumer.handle({"payment_id": str(uuid4()), "attempt": 1}, message)
    assert order == ["committed", "acked"]
    message.nack.assert_not_awaited()


async def test_db_outage_requeues_without_ack(monkeypatch):
    processor = AsyncMock()
    processor.process.side_effect = RuntimeError("database down")
    processor.record_failure.side_effect = RuntimeError("database down")
    consumer = PaymentConsumer(processor)
    monkeypatch.setattr(consumer_module.asyncio, "sleep", AsyncMock())
    message = AsyncMock()
    await consumer.handle({"payment_id": str(uuid4())}, message)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


@pytest.mark.parametrize(
    "body", [{"payment_id": "bad"}, {"payment_id": str(uuid4()), "attempt": 4}]
)
async def test_poison_messages_are_dead_lettered(body):
    consumer = PaymentConsumer(AsyncMock())
    message = AsyncMock()
    await consumer.handle(body, message)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


async def test_permanent_error_rejects():
    processor = AsyncMock()
    processor.process.side_effect = PermanentEventError("unknown payment")
    consumer = PaymentConsumer(processor)
    message = AsyncMock()
    await consumer.handle({"payment_id": str(uuid4())}, message)
    message.reject.assert_awaited_once_with(requeue=False)
