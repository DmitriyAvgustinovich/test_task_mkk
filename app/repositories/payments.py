from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None:
        query = select(Payment).where(Payment.id == payment_id)
        if for_update:
            query = query.with_for_update()
        return await self.session.scalar(query)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return await self.session.scalar(select(Payment).where(Payment.idempotency_key == key))

    def add(self, payment: Payment) -> None:
        self.session.add(payment)
