from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.domain.constants import MAX_ATTEMPTS


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: UUID
    attempt: Annotated[int, Field(ge=1, le=MAX_ATTEMPTS)] = 1
