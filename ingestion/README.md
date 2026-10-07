# Ingestion Layer

### Overview

<img src="../assets/ingestion.jpg" alt="Ingestion Flow" width="66%">

<br>

- [Binance Public WebSocket Market Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams) as the data source; subscribe to multiple active pairs concurrently via a combined stream

- `websockets` for the asyncio WebSocket client

- `confluent-kafka` for the Kafka producer

- Redpanda as the local Kafka API compatible broker (and Redpanda Console for inspecting topics)

### Running Locally

1. Copy `.env.example` to `.env` and adjust as needed (defaults below):

    | Variable | Default | Description |
    | --- | --- | --- |
    | `SYMBOLS` | `btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt` | Comma-separated trading pairs to subscribe to (case-insensitive) |
    | `BINANCE_WS_BASE_URL` | `wss://stream.binance.com:9443/stream` | Combined stream endpoint |
    | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Redpanda's external listener from `docker-compose.yml` |
    | `KAFKA_RAW_TRADES_TOPIC` | `raw_trades` | Destination topic |
    | `RECONNECT_MIN_BACKOFF_SECONDS` | `1` | Initial reconnect delay |
    | `RECONNECT_MAX_BACKOFF_SECONDS` | `60` | Maximum reconnect delay |

2. Start all services:

    ```bash
    ./bootstrap.sh
    ```

3. Start ingestion:

    ```bash
    uv run -m ingestion.main
    ```

4. Check that trades are arriving (or from the Redpanda Console at http://localhost:8080):

    ```bash
    docker exec redpanda rpk topic consume raw_trades -n 5
    ```

### Testing Throughput

To measure per-symbol throughput without Kafka:

```bash
uv run -m ingestion.count_trade_events --duration 10
```

### Event Schema

Each Binance trade payload is converted from its single-letter field names to the project schema before publishing:

| Field | Binance Field | Type | Notes |
| --- | --- | --- | --- |
| `event_type` | — | string | Always `"trade"` |
| `event_time` | `E` | int (epoch ms) | When Binance emitted the event |
| `symbol` | `s` | string | Uppercase, e.g. `BTCUSDT` (also used as the Kafka key) |
| `trade_id` | `t` | int | Increases by 1 for each trade on a symbol; used for gap detection |
| `price` | `p` | string (decimal) | Kept as a string to avoid float precision loss |
| `quantity` | `q` | string (decimal) | Kept as a string to avoid float precision loss |
| `trade_time` | `T` | int (epoch ms) | Trade execution time; the **event time** used for downstream watermarking |
| `is_buyer_maker` | `m` | bool | |

### BinanceTradeStreamClient (`ws_client.py`)

- Handles the parsing and normalisation of trade records to match the event schema

    Raw:

    ```json
    {"stream": "btcusdt@trade",
    "data": {"e": "trade", "E": 1759800000123, "s": "BTCUSDT", "t": 5123443,
            "p": "62150.10", "q": "0.004", "T": 1759800000120, "m": true, "M": true}}
    ```

    Normalised:

    ```json
    {"event_type": "trade", "event_time": 1759800000123, "symbol": "BTCUSDT",
    "trade_id": 5123443, "price": "62150.10", "quantity": "0.004",
    "trade_time": 1759800000120, "is_buyer_maker": true}
    ```

- Reconnection with exponential backoff

    - Reconnect on any `ConnectionClosed`, `OSError`, or unexpected exceptions

    - When Binance closes the connection (24-hour forced disconnect), the client reconnects to the same combined stream

- Per-symbol gap detection

    - Client keeps the last `trade_id` seen per symbol

    - State is persisted across reconnects, so gaps caused by disconnections are caught

    - State is stored in-memory only, so it is reset when the process restarts

    - Warning is logged if a new trade's ID jumps by more than 1

    - **For now, trades executed during the reconnect window are not backfilled**

- Other Features

    - **Keepalive**: Client pings every 20s and treats a missing pong within 20s as a dead connection

    - **Malformed messages**: Messages that fail JSON parsing or have missing fields are logged and skipped (non-breaking); any other parsing error is caught by the stream loop and triggers a reconnect

    - **Graceful shutdown**: SIGINT/SIGTERM stops the stream loop and the producer is flushed before exit (up to 10s)

### Kafka Producer (`producer.py`)

```python
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
```

Events are keyed by trade symbol, and every trade for a given pair goes to the same partition.

This maintains per-symbol ordering and subsequently provides Flink's keyed state with the right partitioning.

1. `produce()`

    - Hashes the key (trade symbol) to pick a partition
    
    - Message is placed in an in-memory queue in the Python process
    
    - Returns immediately; nothing sent over network yet

2. Background Thread (C)

    - `librdkafka` broker thread collects queued messages into a batch per partition

    - Sends a batch when either:

        - Waited for `linger.ms=20`
        
        - Reaches `batch.size=65536` (64 KB)

    - Compresses the batch with `compression.type=lz4` and sends it to Redpanda broker

    - Failed messages are retried

3. Redpanda

     - `acks=all` (equivalent to `acks=1` on the single-node local setup) waits for all in-sync replicas of the partition to acknowledge
    
    - When Redpanda acks (or when message finally fails), `librdkafka` puts a delivery report on a second internal queue in the Python process

4. `poll(timeout)`

    - Retrieves the delivery reports in that queue and runs `on_delivery=_delivery_report` callback

    - Delivery failures are logged at `ERROR` level

    - `poll(0)` does not block and handles whatever reports are ready, then returns

5. `flush(timeout)`

    - Calls `poll()` in a loop until either:
    
        - Every queued message has been delivered (or failed)
        
        - Timeout runs out (10 seconds)

    - Returns the number of messages still undelivered

### Producer Idempotency

`enable.idempotence` ensures that within a producer session, messages are written to each partition once and in order.

Suppose a scenario where:

1. Producer sends a batch of trades

2. Redpanda writes the batch, but the ack is lost due to network blip or timeout

3. `librdkafka` assumes the send failed and retries the same batch

4. Redpanda writes it again, so those trades now appear twice and likely unordered

<br>

Redpanda solves this by:

- Tracking the producer with an ID

- Each batch sent to a partition carries that ID along with a sequence number

- Redpanda tracks the last sequence number it wrote for each producer and partition

- **Same number means a duplicate retry; acks the batch without writing it**

- **Gap in numbers means that an earlier batch is missing; batch is rejected, and `librdkafka` resends in the right order**

<br>

However, the guarantee only lasts for one producer session. If the producer restarts, Redpanda cannot match the messages against the old session's messages.

Also if Binance were to send the same trade twice, both copies will be written. This is because idempotence only removes duplicates due to producer retries.