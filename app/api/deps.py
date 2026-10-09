from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import UnauthorizedError
from app.core.security import is_valid_api_key
from app.services.health import HealthService
from app.services.payments import PaymentService

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def authenticate(
    request: Request,
    supplied: Annotated[str | None, Depends(api_key_header)],
) -> None:
    expected = request.app.state.settings.api_key.get_secret_value()
    if not is_valid_api_key(supplied, expected):
        raise UnauthorizedError


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


Session = Annotated[AsyncSession, Depends(get_session)]


def get_payment_service(session: Session) -> PaymentService:
    return PaymentService(session)


def get_health_service(session: Session) -> HealthService:
    return HealthService(session)
