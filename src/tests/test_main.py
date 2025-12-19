import pytest
import asyncio
import time
from fastapi.testclient import TestClient
from src.main import app, stats
from src.database import init_db, mark_processed, is_processed
import os

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
async def setup_db():
    await init_db()
    yield
    # Cleanup
    if os.path.exists("dedup.db"):
        os.remove("dedup.db")

@pytest.mark.asyncio
async def test_publish_single_event():
    response = client.post("/publish", json={
        "events": {
            "topic": "test",
            "event_id": "1",
            "timestamp": "2023-01-01T00:00:00Z",
            "source": "app",
            "payload": {}
        }
    })
    assert response.status_code == 200
    await asyncio.sleep(0.1)  # Wait for processing
    assert stats["received"] == 1
    assert stats["unique_processed"] == 1

@pytest.mark.asyncio
async def test_deduplication():
    # Publish duplicate
    client.post("/publish", json={
        "events": {
            "topic": "test",
            "event_id": "1",
            "timestamp": "2023-01-01T00:00:00Z",
            "source": "app",
            "payload": {}
        }
    })
    await asyncio.sleep(0.1)
    assert stats["duplicate_dropped"] == 1
    assert stats["unique_processed"] == 1

@pytest.mark.asyncio
async def test_batch_publish():
    events = [
        {"topic": "test", "event_id": "2", "timestamp": "2023-01-01T00:00:00Z", "source": "app", "payload": {}},
        {"topic": "test", "event_id": "3", "timestamp": "2023-01-01T00:00:00Z", "source": "app", "payload": {}}
    ]
    response = client.post("/publish", json={"events": events})
    assert response.status_code == 200
    await asyncio.sleep(0.1)
    assert stats["received"] == 3  # Previous + 2
    assert stats["unique_processed"] == 3

@pytest.mark.asyncio
async def test_schema_validation():
    response = client.post("/publish", json={
        "events": {"topic": "test"}  # Missing fields
    })
    assert response.status_code == 400

@pytest.mark.asyncio
async def test_get_events():
    response = client.get("/events?topic=test")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3

@pytest.mark.asyncio
async def test_get_stats():
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["received"] == 3
    assert data["unique_processed"] == 3
    assert data["duplicate_dropped"] == 1
    assert "test" in data["topics"]
    assert data["uptime"] > 0

@pytest.mark.asyncio
async def test_persistence_after_restart():
    # Simulate restart by clearing in-memory stats but keeping DB
    original_processed = stats["unique_processed"]
    stats["unique_processed"] = 0
    # Publish same event again
    client.post("/publish", json={
        "events": {
            "topic": "test",
            "event_id": "1",
            "timestamp": "2023-01-01T00:00:00Z",
            "source": "app",
            "payload": {}
        }
    })
    await asyncio.sleep(0.1)
    assert stats["duplicate_dropped"] == 2  # Should still be duplicate

@pytest.mark.asyncio
async def test_stress_batch():
    events = [{"topic": "stress", "event_id": str(i), "timestamp": "2023-01-01T00:00:00Z", "source": "app", "payload": {}} for i in range(100)]
    start = time.time()
    response = client.post("/publish", json={"events": events})
    assert response.status_code == 200
    await asyncio.sleep(1)  # Wait for processing
    end = time.time()
    assert end - start < 2  # Reasonable time
    assert stats["unique_processed"] >= 103  # Previous + 100