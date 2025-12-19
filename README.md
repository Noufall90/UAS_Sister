# Pub-Sub Log Aggregator Terdistribusi

Sistem log aggregator multi-service dengan Pub-Sub pattern yang mendukung idempotency, deduplication, transaksi, dan kontrol konkurensi. Dibangun dengan Docker Compose, Python FastAPI, dan PostgreSQL.

## Arsitektur

```
┌─────────────────────────────────────────────────────────┐
│              Docker Compose Network                     │
│  ┌──────────────┐    ┌──────────────┐                   │
│  │  Publisher   │──▶ │ Aggregator   │                   │
│  │  (Worker)    │    │  (FastAPI)   │                   │
│  └──────────────┘    │              │◀─┐               │
│                      │  - POST /    │   │               │
│                      │    publish   │   │               │
│                      │  - GET /     │   │               │
│                      │    events    │   │               │
│                      │  - GET /     │   │               │
│                      │    stats     │   │               │
│                      └──────────────┘   │               │
│                            │            │               │
│                            ▼            │               │
│  ┌──────────────┐    ┌──────────────┐   │               │ 
│  │   Redis      │    │ PostgreSQL   │───┘               │
│  │   (Broker)   │    │ (Persistent) │                   │
│  └──────────────┘    │              │                   │
│                      │ - Events     │                   │
│                      │ - Dedup      │                   │
│                      │ - Stats      │                   │
│                      └──────────────┘                   │
└─────────────────────────────────────────────────────────┘
```

### Services

1. **Aggregator**: API service yang menerima publish event, menyimpan dengan dedup, dan provide GET endpoints
2. **Publisher**: Mensimulasikan multiple event sources dengan duplikasi untuk test idempotency
3. **PostgreSQL**: Persistent storage untuk dedup store dan processed events
4. **Redis**: Message broker internal (untuk future expansion)

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (untuk development)
- PostgreSQL 16 (untuk testing lokal)

### Build & Run

```bash
# Build dan start services
docker-compose up --build

```

### API Endpoints

#### 1. Health Check
```bash
curl http://localhost:8080/health
```

#### 2. Publish Single Event
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": {
      "topic": "logs.application",
      "event_id": "evt-12345",
      "timestamp": "2025-12-18T10:30:00Z",
      "source": "service-a",
      "payload": {"level": "INFO", "message": "Started"}
    }
  }'
```

#### 3. Publish Batch Events
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {"topic": "logs.api", "event_id": "1", "timestamp": "2025-12-18T10:30:00Z", "source": "api-gateway", "payload": {}},
      {"topic": "logs.db", "event_id": "2", "timestamp": "2025-12-18T10:30:01Z", "source": "database", "payload": {}}
    ]
  }'
```

#### 4. Get All Events (or filter by topic)
```bash
# Get all events
curl http://localhost:8080/events

# Filter by topic
curl http://localhost:8080/events?topic=logs.application
```

#### 5. Get Statistics
```bash
curl http://localhost:8080/stats
```

Response:
```json
{
  "received": 50000,
  "unique_processed": 32500,
  "duplicate_dropped": 17500,
  "topics": ["logs.application", "logs.payment", ...],
  "uptime_seconds": 245.3,
  "unique_rate": 65.0,
  "duplicate_rate": 35.0
}
```

## 🔑 Key Features

### 1. Idempotent Consumer
- Event dengan `(topic, event_id)` yang sama hanya diproses sekali
- Menggunakan UNIQUE constraint di database
- Multiple workers dapat safely process events concurrent

### 2. Deduplication
- **Persisten**: Dedup store di Postgres mencegah reprocessing setelah restart
- **Atomik**: Menggunakan `INSERT ... ON CONFLICT DO NOTHING` untuk atomicity
- **Transaksional**: SERIALIZABLE isolation level

### 3. Transaction & Concurrency Control
- Setiap processing menggunakan explicit transaction (SERIALIZABLE)
- Stats increment dilindungi dengan UPDATE ... SET count = count + 1
- Concurrent operations dijamin race-condition free via unique constraints

### 4. At-Least-Once Delivery
- Publisher mengirim duplikasi untuk simulate at-least-once
- Aggregator menghandle dengan idempotency
- Hasil: exactly-once processing

### 5. Event Batching
- POST /publish accept single atau batch events
- Batch processing untuk efficiency
- Partial failure handling dengan detailed error reporting

## Data Model

### Event Schema
```json
{
  "topic": "string (required)",
  "event_id": "string (required, unique per topic)",
  "timestamp": "ISO8601 (required)",
  "source": "string (required)",
  "payload": "object (optional)"
}
```

### Run Specific Test
```bash
pytest src/tests/test_comprehensive.py::test_concurrent_duplicate_processing -v
```

## Performance

### Throughput Test
```
Event Rate: ~50,000 events dari publisher
Duplicate Rate: ~35%
Unique Events Processed: ~32,500
Processing Time: ~240 seconds
Throughput: ~208 events/second
```

### Benchmark dengan 50K events
```
Database: PostgreSQL 16 dengan connection pool (5-20 connections)
Indexes: topic, timestamp untuk fast queries
Batch Size: Variable 5-50 events per POST
```

## Configuration

### Environment Variables

```bash
# Database
DATABASE_URL=postgresql://user:pass@postgres:5432/log_aggregator

# Redis (future use)
REDIS_URL=redis://redis:6379

# Logger
LOG_LEVEL=INFO

# Publisher
AGGREGATOR_URL=http://aggregator:8080
PUBLISHER_WORKERS=3
EVENT_COUNT=50000
DUPLICATE_RATE=0.35
```

## 🔧 Development

### Local Setup (without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Setup PostgreSQL
createdb log_aggregator_test

# Run tests
pytest src/tests/ -v

# Run aggregator
python -m uvicorn src.main:app --reload

# Run publisher (in another terminal)
cd publisher && python -m publisher.main
```

### Project Structure

```
.
├── src/
│   ├── __init__.py
│   ├── main.py              # Aggregator API
│   ├── models.py            # Pydantic models
│   ├── database.py          # PostgreSQL operations
│   ├── tests/
│   │   ├── __init__.py
│   │   ├── test_main.py     # Old tests (deprecated)
│   │   └── test_comprehensive.py  # 24 comprehensive tests
├── publisher/
│   ├── __init__.py
│   └── main.py              # Publisher simulator
├── docker-compose.yml       # Multi-service orchestration
├── Dockerfile               # Aggregator image
├── publisher.Dockerfile     # Publisher image
├── requirements.txt
└── README.md
```

## Monitoring

### Endpoints untuk Monitoring

```bash
# Health check
GET /health

# Statistics dan uptime
GET /stats

# System info
GET /info


- [x] Teori (T1-T10): Ringkas dengan sitasi APA
- [x] Arsitektur multi-service (Compose): 4 services (aggregator, publisher, postgres, redis)
- [x] Idempotent consumer: unique constraint + INSERT ... ON CONFLICT
- [x] Deduplication: persisten di database
- [x] Transaksi: SERIALIZABLE isolation
- [x] Konkurensi: atomic operations, no lost-update
- [x] Event batching: POST /publish accept single atau batch
- [x] Persistensi: named volumes
- [x] Tests: 24 comprehensive tests
- [x] API endpoints: /publish, /events, /stats, /health
- [x] Dockerfile & Compose: production-ready
- [x] Logging & observability: detailed logs

## 📹 Video Demo

Link: https://youtu.be/2QVxdHnHN-c

---

**Created**: December 2025  
**Authors**: UAS Sistem Terdistribusi  
**Version**: 1.0.0
