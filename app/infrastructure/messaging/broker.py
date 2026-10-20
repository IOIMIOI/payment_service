"""Единая топология RabbitMQ для consumer и издателя outbox."""

from faststream.rabbit import RabbitBroker, RabbitQueue
from faststream.rabbit.schemas import Channel

from app.config import get_settings

PAYMENT_QUEUE = "payments.new"
WEBHOOK_QUEUE = "payments.webhook"
DLQ_NAMES = (PAYMENT_QUEUE + ".dlq", WEBHOOK_QUEUE + ".dlq")
QUEUES = {name: RabbitQueue(name, durable=True) for name in DLQ_NAMES}
for name in (PAYMENT_QUEUE, WEBHOOK_QUEUE):
    QUEUES[name] = RabbitQueue(
        name,
        durable=True,
        arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": name + ".dlq"},
    )


def create_broker(url: str | None = None) -> RabbitBroker:
    # mandatory без on_return_raises недостаточно: недоставленное сообщение
    # должно вызвать исключение, иначе outbox ошибочно отметит его опубликованным.
    return RabbitBroker(
        url or get_settings().rabbitmq_url,
        default_channel=Channel(
            publisher_confirms=True,
            on_return_raises=True,
            prefetch_count=1,
            global_qos=True,
        ),
        timeout=10,
        reconnect_interval=2,
        graceful_timeout=20,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    # DLQ существуют до получения первого сообщения основными очередями.
    for queue in QUEUES.values():
        await broker.declare_queue(queue)
