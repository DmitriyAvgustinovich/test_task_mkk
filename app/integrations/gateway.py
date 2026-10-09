import asyncio
import random

from app.core.config import Settings
from app.domain.enums import PaymentStatus


class PaymentGateway:
    def __init__(self, settings: Settings):
        self.settings = settings

    async def process(self) -> PaymentStatus:
        await asyncio.sleep(
            random.uniform(
                self.settings.gateway_min_delay_seconds,
                self.settings.gateway_max_delay_seconds,
            )
        )
        return (
            PaymentStatus.SUCCEEDED
            if random.random() < self.settings.gateway_success_rate
            else PaymentStatus.FAILED
        )
