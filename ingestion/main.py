import asyncio
import logging
import signal

from ingestion import config
from ingestion.ws_client import BinanceTradeStreamClient
from ingestion.producer import KafkaTradeProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:

    producer = KafkaTradeProducer()
    client = BinanceTradeStreamClient(symbols=config.SYMBOLS, on_trade=producer.publish)

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, client.stop)

    logger.info(f"Starting ingestion for symbols: {config.SYMBOLS}")

    try:
        await client.run()

    finally:
        logger.info("Flushing producer before exit")
        producer.flush()


if __name__ == "__main__":
    asyncio.run(main())
