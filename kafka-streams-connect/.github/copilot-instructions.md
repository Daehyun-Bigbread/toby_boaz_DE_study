# Copilot Instructions

## Project Overview

BOAZ study repo demonstrating Kafka stream processing and Kafka Connect using Python and Docker Compose. All Kafka infrastructure runs in Docker; Python scripts run inside the `app` service container.

## Environment Setup & Commands

### Start / stop the stack
```bash
docker compose up -d          # start all services (zookeeper, kafka, kafka-connect, app)
docker compose down           # stop and remove containers
docker compose logs -f kafka  # follow logs for a specific service
```

### Run Python scripts (inside the app container)
```bash
docker compose exec app bash                 # open a shell in the app container
python producer_payment.py                   # Part 1&2: generate payment events
python streams_filter.py                     # Part 1: stateless high-amount filter
python streams_aggregate.py                  # Part 2: stateful per-user aggregation
python consumer_alert.py                     # Part 3/4: consume alerts
```

### Register the Kafka Connect file-sink connector (from host)
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connect-config/file-sink.json
```

### Check connector status
```bash
curl http://localhost:8083/connectors/file-sink-connector/status
```

## Architecture

### Topic flow

```
producer_payment.py
        │
        ▼
   [payments]  (3 partitions, key = user_id, value = JSON)
        │
        ├──▶ streams_filter.py ──▶ [high-amount-alerts] ──▶ consumer_alert.py
        │                                                 └──▶ Kafka Connect File Sink → /output/alerts.txt
        │
        └──▶ streams_aggregate.py ──▶ [user-total-amount]
```

### Services (docker-compose.yml)
| Service | Image | Port |
|---|---|---|
| zookeeper | cp-zookeeper:7.5.0 | 2181 (internal) |
| kafka | cp-kafka:7.5.0 | 9092 |
| kafka-connect | cp-kafka-connect:7.5.0 | 8083 |
| app | custom (Python 3.11-slim) | — |

- Kafka Connect plugin JAR lives in `connect-plugins/`, mounted into the container at `/etc/kafka-connect/plugins`.
- Connect output file is written to `./connect-output/alerts.txt` on the host (mounted as `/output` in the container).

## Key Conventions

### Serialization
All messages use JSON with `ensure_ascii=False` to preserve Korean characters. The producer serializer pattern used throughout:
```python
value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
```

### Message key
All producers key messages by `user_id` (encoded as UTF-8 bytes) to ensure per-user ordering across partitions.

### Consumer group IDs
Each script uses a distinct `group_id` to allow independent offset tracking:
- `streams-filter-group` — `streams_filter.py`
- `streams-aggregate-group` — `streams_aggregate.py`
- `alert-monitor-group` — `consumer_alert.py`

### State store
`streams_aggregate.py` uses an in-memory `defaultdict(int)` as the state store — **not** a persistent Kafka Streams KTable. State is lost on restart; use `auto_offset_reset='earliest'` to replay from the beginning.

### Payment event schema
```json
{ "user_id": "김철수", "store": "스타벅스", "amount": 95000, "ts": "14:32:01" }
```

### Alert event schema (adds `alert` field)
```json
{ "user_id": "김철수", "store": "스타벅스", "amount": 150000, "ts": "14:32:01", "alert": "🚨 고액 거래 감지! 150,000원" }
```

### High-amount threshold
Defined in `streams_filter.py` as `THRESHOLD = 100000` (100,000 KRW).

### Bootstrap servers
Always `kafka:9092` (Docker internal network). Scripts are designed to run inside the `app` container, not from the host.
