from uuid import UUID

from faststream.rabbit import Channel, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import Settings
from app.domain.constants import MAX_ATTEMPTS

PAYMENTS_EXCHANGE = RabbitExchange("payments", durable=True)
DLX = RabbitExchange("payments.dlx", durable=True)
MAIN_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    queue_type=QueueType.QUORUM,
    arguments={
        "x-dead-letter-exchange": DLX.name,
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": 1000000,
    },
)
DLQ = RabbitQueue("payments.dlq", durable=True, queue_type=QueueType.QUORUM)


def retry_queues(settings: Settings) -> list[RabbitQueue]:
    return [
        RabbitQueue(
            f"payments.retry.{attempt}",
            durable=True,
            queue_type=QueueType.QUORUM,
            arguments={
                "x-message-ttl": int(settings.retry_base_delay_seconds * 2 ** (attempt - 2) * 1000),
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
                "x-dead-letter-routing-key": MAIN_QUEUE.name,
                "x-dead-letter-strategy": "at-least-once",
                "x-overflow": "reject-publish",
            },
        )
        for attempt in range(2, MAX_ATTEMPTS + 1)
    ]


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        graceful_timeout=30,
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dead_letter_exchange = await broker.declare_exchange(DLX)
    main = await broker.declare_queue(MAIN_QUEUE)
    await main.bind(payments_exchange, routing_key=MAIN_QUEUE.name)
    dead = await broker.declare_queue(DLQ)
    await dead.bind(dead_letter_exchange, routing_key=DLQ.name)
    for queue in retry_queues(settings):
        retry = await broker.declare_queue(queue)
        await retry.bind(payments_exchange, routing_key=queue.name)


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker):
        self.broker = broker

    async def publish(self, payload: dict, routing_key: str, message_id: UUID) -> None:
        confirmation = await self.broker.publish(
            payload,
            exchange=DLX if routing_key == "payments.dlq" else PAYMENTS_EXCHANGE,
            routing_key=routing_key,
            message_id=str(message_id),
            persist=True,
            mandatory=True,
            timeout=10,
        )
        if confirmation is None or confirmation is False:
            raise RuntimeError("Broker did not confirm the publication")
