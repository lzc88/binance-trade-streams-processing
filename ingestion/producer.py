import json
import logging

from confluent_kafka import KafkaError, Message, Producer

from ingestion import config

logger = logging.getLogger(__name__)


def _delivery_report(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error(f"Delivery failed for {msg.key()}: {err}")


class KafkaTradeProducer:

    def __init__(self, topic: str = config.KAFKA_RAW_TRADES_TOPIC) -> None:

        self.topic = topic

        self._producer = Producer(
            {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "linger.ms": 20,
                "batch.size": 65536,
                "compression.type": "lz4",
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def publish(self, trade: dict) -> None:

        self._producer.produce(
            topic=self.topic,
            key=trade["symbol"].encode("utf-8"),
            value=json.dumps(trade).encode("utf-8"),
            on_delivery=_delivery_report,
        )

        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)
