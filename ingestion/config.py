import os

from dotenv import load_dotenv

load_dotenv()

DEFAULT_SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]

SYMBOLS = [
    s.strip().lower()
    for s in os.getenv("SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")
    if s.strip()
]

BINANCE_WS_BASE_URL = os.getenv(
    "BINANCE_WS_BASE_URL", "wss://stream.binance.com:9443/stream"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_RAW_TRADES_TOPIC = os.getenv("KAFKA_RAW_TRADES_TOPIC", "raw_trades")

RECONNECT_MIN_BACKOFF_SECONDS = float(os.getenv("RECONNECT_MIN_BACKOFF_SECONDS", "1"))
RECONNECT_MAX_BACKOFF_SECONDS = float(os.getenv("RECONNECT_MAX_BACKOFF_SECONDS", "60"))
