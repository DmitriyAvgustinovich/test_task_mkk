import hashlib
import json
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import IdempotencyConflict, PaymentNotFound
from app.models.outbox import Outbox
from app.models.payment import Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas.payment import PaymentAccepted, PaymentCreate, PaymentDetail


def request_hash(body: PaymentCreate) -> str:
    data = body.model_dump(mode="json")

    data["amount"] = format(body.amount, ".2f")
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)

    async def get(self, payment_id: UUID) -> PaymentDetail:
        payment = await self.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound
        return PaymentDetail.model_validate(payment)

    async def create(self, body: PaymentCreate, key: str) -> PaymentAccepted:
        fingerprint = request_hash(body)
        existing = await self.payments.get_by_idempotency_key(key)
        if existing:
            return self.existing_response(existing, fingerprint)

        payment_id = uuid4()
        payment = Payment(
            id=payment_id,
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            metadata_=body.metadata,
            webhook_url=str(body.webhook_url),
            idempotency_key=key,
            request_hash=fingerprint,
        )
        self.payments.add(payment)
        self.outbox.add(
            Outbox(
                event_key=f"payment:{payment_id}:attempt:1",
                routing_key="payments.new",
                payload={"payment_id": str(payment_id), "attempt": 1},
            )
        )
        try:
            await self.session.commit()
        except IntegrityError:
            await self.session.rollback()
            existing = await self.payments.get_by_idempotency_key(key)
            if existing is None:
                raise
            return self.existing_response(existing, fingerprint)
        return PaymentAccepted.model_validate(payment)

    @staticmethod
    def existing_response(payment: Payment, fingerprint: str) -> PaymentAccepted:
        if payment.request_hash != fingerprint:
            raise IdempotencyConflict
        return PaymentAccepted.model_validate(payment)
