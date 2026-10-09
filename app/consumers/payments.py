import asyncio
import logging
from typing import Any

from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from app.core.exceptions import PermanentEventError
from app.schemas.events import PaymentEvent
from app.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)


class PaymentConsumer:
    def __init__(self, processor: PaymentProcessor):
        self.processor = processor

    async def handle(self, body: Any, message: RabbitMessage) -> None:
        try:
            event = PaymentEvent.model_validate(body)
        except ValidationError:
            logger.exception("Malformed payment event message_id=%s", message.message_id)
            await message.reject(requeue=False)
            return

        try:
            await self.processor.process(event)
        except PermanentEventError:
            logger.exception("Unprocessable payment event payment_id=%s", event.payment_id)
            await message.reject(requeue=False)
            return
        except Exception as exc:
            logger.exception(
                "Payment attempt failed payment_id=%s attempt=%s",
                event.payment_id,
                event.attempt,
            )
            try:
                await self.processor.record_failure(event, exc)
            except Exception:
                logger.exception("Cannot persist retry; returning the original message to RabbitMQ")
                await asyncio.sleep(1)
                await message.nack(requeue=True)
                return
        await message.ack()
