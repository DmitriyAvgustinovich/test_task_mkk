from typing import Protocol
from uuid import UUID

from app.domain.enums import PaymentStatus


class Gateway(Protocol):
    async def process(self) -> PaymentStatus: ...


class WebhookSender(Protocol):
    async def send(self, url: str, payload: dict) -> None: ...


class EventPublisher(Protocol):
    async def publish(self, payload: dict, routing_key: str, message_id: UUID) -> None: ...
