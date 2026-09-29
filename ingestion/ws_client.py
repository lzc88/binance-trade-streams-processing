import json
import logging
import asyncio
import websockets

from typing import Callable, Dict

from ingestion import config

logger = logging.getLogger(__name__)


def _combined_stream_url(symbols: list[str]) -> str:

    streams = "/".join(f"{s}@trade" for s in symbols)

    return f"{config.BINANCE_WS_BASE_URL}?streams={streams}"


def _to_normalized_trade(raw: dict) -> dict:

    return {
        "event_type": "trade",
        "event_time": raw["E"],
        "symbol": raw["s"],
        "trade_id": raw["t"],
        "price": raw["p"],
        "quantity": raw["q"],
        "trade_time": raw["T"],
        "is_buyer_maker": raw["m"],
    }


class BinanceTradeStreamClient:

    def __init__(self, symbols: list[str], on_trade: Callable[[dict], None]) -> None:

        self.symbols = symbols
        self._last_trade_id: Dict[str, int] = {}
        self._stop = asyncio.Event()

        # producer callback to publish received trade
        self.on_trade = on_trade

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:

        backoff = config.RECONNECT_MIN_BACKOFF_SECONDS
        url = _combined_stream_url(self.symbols)

        while not self._stop.is_set():

            try:
                logger.info(f"Connecting to {url}")

                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20, close_timeout=1
                ) as ws:

                    logger.info(f"Connected; streaming {len(self.symbols)} symbols")
                    backoff = config.RECONNECT_MIN_BACKOFF_SECONDS

                    async for raw_message in ws:

                        if self._stop.is_set():
                            break

                        self._handle_message(raw_message)

            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                logger.warning(
                    f"WebSocket disconnected ({exc}); reconnecting in {backoff:.1f}s"
                )

            except Exception:
                logger.exception(
                    f"Unexpected error in stream loop; reconnecting in {backoff:.1f}s"
                )

            if self._stop.is_set():
                break

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.RECONNECT_MAX_BACKOFF_SECONDS)

    def _handle_message(self, raw_message: str | bytes) -> None:

        try:
            envelope = json.loads(raw_message)
            payload = envelope["data"]
            trade = _to_normalized_trade(payload)

        except (json.JSONDecodeError, KeyError) as exc:
            logger.error(f"Failed to parse message: {raw_message} ({exc})")
            return

        self._check_for_gap(trade["symbol"], trade["trade_id"])
        self.on_trade(trade)

    def _check_for_gap(self, symbol: str, trade_id: int) -> None:

        last_id = self._last_trade_id.get(symbol)

        if last_id is not None and trade_id > last_id + 1:

            missing = trade_id - last_id - 1

            logger.warning(
                f"Gap detected for {symbol}: missing {missing} trade(s) between {last_id} and {trade_id}",
            )

        self._last_trade_id[symbol] = trade_id
