import time
import argparse
import asyncio
import logging

from collections import Counter

from ingestion import config
from ingestion.ws_client import BinanceTradeStreamClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)

MIN_DURATION_SECONDS = 10
MAX_DURATION_SECONDS = 60


def _duration_type(value: str) -> int:

    duration = int(value)

    if not (MIN_DURATION_SECONDS <= duration <= MAX_DURATION_SECONDS):
        raise argparse.ArgumentTypeError(
            f"--duration must be between {MIN_DURATION_SECONDS} and {MAX_DURATION_SECONDS} seconds"
        )

    return duration


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--duration",
        type=_duration_type,
        default=10,
        help=f"Seconds to listen for ({MIN_DURATION_SECONDS}-{MAX_DURATION_SECONDS}, default: 10)",
    )

    return parser.parse_args()


async def run(duration: int, symbols: list[str]) -> None:

    counts: Counter[str] = Counter()

    def on_trade(trade: dict) -> None:
        counts[trade["symbol"]] += 1

    client = BinanceTradeStreamClient(symbols=symbols, on_trade=on_trade)

    logger.info(f"Listening for {duration}s on symbols: {symbols}")

    started_at = time.monotonic()

    run_task = asyncio.create_task(client.run())

    await asyncio.sleep(duration)

    client.stop()

    await run_task

    elapsed = time.monotonic() - started_at

    total = sum(counts.values())

    print(f"\nElapsed: {elapsed:.1f}s")
    print(f"Total events: {total} ({total / elapsed:.1f}/s)\n")
    print(f"{'Symbol':<12}{'Count':>10}{'Events/sec':>14}")

    for symbol, count in counts.most_common():
        print(f"{symbol:<12}{count:>10}{count / elapsed:>14.1f}")


def main() -> None:

    args = parse_args()

    symbols = config.SYMBOLS

    asyncio.run(run(args.duration, symbols))


if __name__ == "__main__":
    main()
