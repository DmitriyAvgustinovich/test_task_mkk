from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.core.time import utcnow
from app.integrations.rabbitmq import RabbitEventPublisher
from app.models import Outbox, Payment
from app.schemas.payment import PaymentCreate
from app.services.outbox import OutboxRelay
from app.services.payments import PaymentService


async def add_payment(sessions, body):
    async with sessions() as session:
        return await PaymentService(session).create(PaymentCreate.model_validate(body), "relay-key")


async def test_marks_published_after_confirmation(sessions, payment_body, settings):
    await add_payment(sessions, payment_body)
    broker = AsyncMock()
    broker.publish.return_value = True
    relay = OutboxRelay(sessions, RabbitEventPublisher(broker), settings)
    assert await relay.publish_batch() == 1
    assert await relay.publish_batch() == 0
    broker.publish.assert_awaited_once()
    assert broker.publish.call_args.kwargs["persist"] is True
    assert broker.publish.call_args.kwargs["mandatory"] is True
    async with sessions() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is not None
        assert event.publish_attempts == 1


@pytest.mark.parametrize("failure", [RuntimeError("broker down"), None, False])
async def test_unconfirmed_publication_is_retried(sessions, payment_body, settings, failure):
    await add_payment(sessions, payment_body)
    broker = AsyncMock()
    if isinstance(failure, Exception):
        broker.publish.side_effect = failure
    else:
        broker.publish.return_value = failure
    relay = OutboxRelay(sessions, RabbitEventPublisher(broker), settings)
    assert await relay.publish_batch() == 0
    async with sessions() as session, session.begin():
        event = await session.scalar(select(Outbox))
        assert event.published_at is None
        assert event.publish_attempts == 1
        assert event.last_error
        event.next_attempt_at = utcnow()
    broker.publish.side_effect = None
    broker.publish.return_value = True
    assert await relay.publish_batch() == 1


async def test_payment_is_rolled_back_with_outbox(sessions, payment_body):
    async with sessions() as session, session.begin():
        session.add(Outbox(event_key="forced-conflict", routing_key="payments.new", payload={}))
    async with sessions() as session:
        payment = Payment(
            amount="1.00",
            currency="RUB",
            description="atomic",
            metadata_={},
            idempotency_key="atomic",
            request_hash="a" * 64,
            webhook_url="http://example.com/hook",
        )
        session.add_all(
            [
                payment,
                Outbox(event_key="forced-conflict", routing_key="payments.new", payload={}),
            ]
        )
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()
        assert await session.scalar(select(Payment)) is None


async def test_crash_after_confirmation_republishes_same_event(sessions, payment_body, settings):
    await add_payment(sessions, payment_body)
    broker = AsyncMock()
    broker.publish.return_value = True
    relay = OutboxRelay(sessions, RabbitEventPublisher(broker), settings)

    def failed_commit(_):
        raise RuntimeError("crash after broker confirm, before database commit")

    session_class = sessions.class_.sync_session_class
    sqlalchemy_event.listen(session_class, "before_commit", failed_commit)
    try:
        with pytest.raises(RuntimeError, match="crash after broker confirm"):
            await relay.publish_batch()
    finally:
        sqlalchemy_event.remove(session_class, "before_commit", failed_commit)
    async with sessions() as session:
        assert (await session.scalar(select(Outbox))).published_at is None
    assert await relay.publish_batch() == 1
    calls = broker.publish.call_args_list
    assert len(calls) == 2
    assert calls[0].kwargs["message_id"] == calls[1].kwargs["message_id"]
    assert calls[0].args == calls[1].args
