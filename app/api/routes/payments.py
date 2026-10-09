from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException

from app.api.deps import get_payment_service
from app.schemas.payment import PaymentAccepted, PaymentCreate, PaymentDetail
from app.services.payments import PaymentService

router = APIRouter(prefix="/payments", tags=["payments"])
PaymentServiceDependency = Annotated[PaymentService, Depends(get_payment_service)]


@router.post("", response_model=PaymentAccepted, status_code=202)
async def post_payment(
    body: PaymentCreate,
    service: PaymentServiceDependency,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
) -> PaymentAccepted:
    if not idempotency_key.strip():
        raise HTTPException(status_code=422, detail="Idempotency-Key must not be blank")
    return await service.create(body, idempotency_key)


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID, service: PaymentServiceDependency) -> PaymentDetail:
    return await service.get(payment_id)
