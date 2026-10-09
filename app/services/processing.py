from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import PermanentEventError
from app.core.time import utcnow
from app.domain.constants import MAX_ATTEMPTS
from app.domain.enums import PaymentStatus
from app.domain.ports import Gateway, WebhookSender
from app.models.outbox import Outbox
from app.models.payment import Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas.events import PaymentEvent


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: Gateway,
        webhooks: WebhookSender,
    ):
        self.session_factory = session_factory
        self.gateway = gateway
        self.webhooks = webhooks

    async def locked_payment(self, session: AsyncSession, event: PaymentEvent) -> Payment:
        payment = await PaymentRepository(session).get(event.payment_id, for_update=True)
        if payment is None:
            raise PermanentEventError(f"Payment {event.payment_id} does not exist")
        if event.attempt > payment.processing_attempts + 1:
            raise PermanentEventError("Event attempt is ahead of persisted payment state")
        return payment

    @staticmethod
    def already_handled(payment: Payment, event: PaymentEvent) -> bool:
        return payment.webhook_sent_at is not None or payment.processing_attempts >= event.attempt

    async def process(self, event: PaymentEvent) -> None:

        async with self.session_factory() as session, session.begin():
            payment = await self.locked_payment(session, event)
            if self.already_handled(payment, event):
                return
            if payment.status == PaymentStatus.PENDING:
                payment.status = await self.gateway.process()
                payment.processed_at = utcnow()

        async with self.session_factory() as session, session.begin():
            payment = await self.locked_payment(session, event)
            if self.already_handled(payment, event):
                return

            await self.webhooks.send(
                payment.webhook_url,
                {
                    "event_id": f"payment:{payment.id}:result",
                    "event_type": "payment.processed",
                    "payment_id": str(payment.id),
                    "status": payment.status.value,
                    "amount": format(payment.amount, ".2f"),
                    "currency": payment.currency.value,
                    "metadata": payment.metadata_,
                    "processed_at": payment.processed_at.isoformat(),
                },
            )
            payment.webhook_sent_at = utcnow()
            payment.processing_attempts = event.attempt
            payment.last_error = None

    async def record_failure(self, event: PaymentEvent, error: Exception) -> None:
        async with self.session_factory() as session, session.begin():
            payment = await self.locked_payment(session, event)
            if self.already_handled(payment, event):
                return
            payment.processing_attempts = event.attempt
            payment.last_error = f"{type(error).__name__}: {error}"[:2000]
            payload = event.model_dump(mode="json")
            if event.attempt < MAX_ATTEMPTS:
                next_attempt = event.attempt + 1
                payload["attempt"] = next_attempt
                routing_key = f"payments.retry.{next_attempt}"
                event_key = f"payment:{payment.id}:attempt:{next_attempt}"
            else:
                if payment.status == PaymentStatus.PENDING:
                    payment.status = PaymentStatus.FAILED
                    payment.processed_at = utcnow()
                routing_key = "payments.dlq"
                event_key = f"payment:{payment.id}:dead"
                payload.update({"error": payment.last_error, "status": payment.status.value})
            OutboxRepository(session).add(
                Outbox(
                    event_key=event_key,
                    routing_key=routing_key,
                    payload=payload,
                )
            )
