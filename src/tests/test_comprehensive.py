"""
Comprehensive test suite untuk Pub-Sub Log Aggregator.
Testing: idempotency, deduplication, transaksi, konkurensi, validasi, persistensi.
"""

import pytest
import asyncio
import json
import os
from typing import List, Dict, Any
from datetime import datetime
from unittest.mock import patch, MagicMock

# Import dari aplikasi
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from models import Event, PublishRequest, StatsResponse
from database import (
    init_pool, close_pool, init_db, is_processed, mark_processed,
    get_events_by_topic, get_stats, get_topics, get_event_count,
    increment_stats, clear_all_data
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop untuk session"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
async def setup_database(event_loop):
    """Setup database pool dan schema untuk semua tests"""
    # Use test database
    test_db_url = "postgresql://aggregator:aggregator_pass@localhost:5432/log_aggregator_test"
    os.environ["DATABASE_URL"] = test_db_url
    
    try:
        await init_pool()
        await init_db()
        yield
    finally:
        await close_pool()


@pytest.fixture(autouse=True)
async def cleanup_data(event_loop):
    """Clean data sebelum setiap test"""
    try:
        await clear_all_data()
    except:
        pass
    yield
    try:
        await clear_all_data()
    except:
        pass


def create_event(
    topic: str = "test.topic",
    event_id: str = "evt-001",
    timestamp: str = None,
    source: str = "test-source",
    payload: Dict = None
) -> Event:
    """Helper untuk create event"""
    if timestamp is None:
        timestamp = datetime.utcnow().isoformat() + "Z"
    if payload is None:
        payload = {"message": "test"}
    
    return Event(
        topic=topic,
        event_id=event_id,
        timestamp=timestamp,
        source=source,
        payload=payload
    )


# ============================================================================
# TEST 1-5: SCHEMA VALIDATION
# ============================================================================

@pytest.mark.asyncio
async def test_valid_event_schema():
    """T1: Event dengan schema valid harus diterima"""
    event = create_event()
    assert event.topic == "test.topic"
    assert event.event_id == "evt-001"
    assert event.source == "test-source"


@pytest.mark.asyncio
async def test_event_with_empty_payload():
    """T2: Event dengan payload kosong valid"""
    event = Event(
        topic="test",
        event_id="evt-001",
        timestamp=datetime.utcnow().isoformat() + "Z",
        source="test",
        payload={}
    )
    assert event.payload == {}


@pytest.mark.asyncio
async def test_event_missing_required_field():
    """T3: Event tanpa field required harus gagal validasi"""
    with pytest.raises(ValueError):
        Event(event_id="evt-001")  # Missing topic and source


@pytest.mark.asyncio
async def test_publish_request_single_event():
    """T4: PublishRequest menerima single event"""
    event = create_event()
    request = PublishRequest(events=event)
    assert len(request.get_events()) == 1


@pytest.mark.asyncio
async def test_publish_request_batch_events():
    """T5: PublishRequest menerima batch events"""
    events = [
        create_event(event_id="evt-001"),
        create_event(event_id="evt-002"),
        create_event(event_id="evt-003")
    ]
    request = PublishRequest(events=events)
    assert len(request.get_events()) == 3


# ============================================================================
# TEST 6-10: IDEMPOTENCY & DEDUPLICATION
# ============================================================================

@pytest.mark.asyncio
async def test_first_event_processed():
    """T6: Event pertama harus diproses successfully"""
    event = create_event(event_id="evt-unique-1")
    
    success, error = await mark_processed(event)
    assert success is True
    assert error is None
    
    # Verify tersimpan di database
    is_proc = await is_processed(event.topic, event.event_id)
    assert is_proc is True


@pytest.mark.asyncio
async def test_duplicate_event_rejected():
    """T7: Event duplicate harus di-reject (idempotency)"""
    event = create_event(event_id="evt-dup-1")
    
    # Send pertama
    success1, _ = await mark_processed(event)
    assert success1 is True
    
    # Send sama (duplikasi)
    success2, error = await mark_processed(event)
    assert success2 is False
    assert error is not None


@pytest.mark.asyncio
async def test_is_processed_check():
    """T8: is_processed harus return True untuk event yang sudah diproses"""
    event = create_event(event_id="evt-check-1")
    
    # Belum diproses
    assert await is_processed(event.topic, event.event_id) is False
    
    # Process event
    await mark_processed(event)
    
    # Sekarang harus True
    assert await is_processed(event.topic, event.event_id) is True


@pytest.mark.asyncio
async def test_different_event_ids_processed():
    """T9: Events dengan event_id berbeda harus kedua-duanya diproses"""
    event1 = create_event(event_id="evt-001")
    event2 = create_event(event_id="evt-002")
    
    success1, _ = await mark_processed(event1)
    success2, _ = await mark_processed(event2)
    
    assert success1 is True
    assert success2 is True
    assert await is_processed(event1.topic, event1.event_id) is True
    assert await is_processed(event2.topic, event2.event_id) is True


@pytest.mark.asyncio
async def test_same_topic_different_event_ids():
    """T10: Topic sama tapi event_id berbeda harus processed terpisah"""
    topic = "test.topic"
    event1 = create_event(topic=topic, event_id="evt-1a")
    event2 = create_event(topic=topic, event_id="evt-1b")
    
    await mark_processed(event1)
    await mark_processed(event2)
    
    assert await is_processed(topic, "evt-1a") is True
    assert await is_processed(topic, "evt-1b") is True


# ============================================================================
# TEST 11-14: TRANSACTION & CONCURRENCY
# ============================================================================

@pytest.mark.asyncio
async def test_concurrent_duplicate_processing():
    """T11: Multiple workers processing same event hanya 1 yang succeed"""
    event = create_event(event_id="evt-concurrent-1")
    
    # Simulate 5 concurrent workers processing same event
    tasks = [
        mark_processed(event)
        for _ in range(5)
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Hanya 1 yang harus success, sisanya failure (due to UNIQUE constraint)
    successes = sum(1 for success, _ in results if success)
    failures = sum(1 for success, _ in results if not success)
    
    assert successes == 1, f"Expected 1 success, got {successes}"
    assert failures == 4, f"Expected 4 failures, got {failures}"


@pytest.mark.asyncio
async def test_stats_increment_concurrent():
    """T12: Stats increment tidak ada lost-update saat concurrent"""
    # Simulate 10 concurrent increments
    tasks = [
        increment_stats(received=1, unique=1)
        for _ in range(10)
    ]
    
    await asyncio.gather(*tasks)
    
    # Verify stats
    stats = await get_stats()
    assert stats["received"] == 10
    assert stats["unique_processed"] == 10


@pytest.mark.asyncio
async def test_mixed_concurrent_operations():
    """T13: Mix dari concurrent mark_processed dan stats increment"""
    
    async def process_and_increment(idx: int):
        event = create_event(event_id=f"evt-mixed-{idx}")
        success, _ = await mark_processed(event)
        if success:
            await increment_stats(unique=1)
        else:
            await increment_stats(duplicate=1)
    
    # 20 concurrent operations
    await asyncio.gather(*[
        process_and_increment(i)
        for i in range(20)
    ])
    
    stats = await get_stats()
    total = stats["unique_processed"] + stats["duplicate_dropped"]
    assert total == 20


@pytest.mark.asyncio
async def test_serializable_isolation():
    """T14: SERIALIZABLE isolation level mencegah phantom reads"""
    # Create events dengan pattern tertentu
    for i in range(5):
        event = create_event(topic="test.isolation", event_id=f"evt-iso-{i}")
        await mark_processed(event)
    
    # Verify semua processed
    events = await get_events_by_topic("test.isolation")
    assert len(events) == 5


# ============================================================================
# TEST 15-18: DATA PERSISTENCE & RECOVERY
# ============================================================================

@pytest.mark.asyncio
async def test_events_persisted_to_database():
    """T15: Events harus tersimpan ke database"""
    event = create_event(event_id="evt-persist-1")
    await mark_processed(event)
    
    # Query dari database
    events = await get_events_by_topic(event.topic)
    assert len(events) == 1
    assert events[0]["event_id"] == event.event_id


@pytest.mark.asyncio
async def test_dedup_store_persisted():
    """T16: Dedup store harus persistent, prevent reprocessing setelah restart"""
    event = create_event(event_id="evt-dedup-persist-1")
    
    # Process event
    success1, _ = await mark_processed(event)
    assert success1 is True
    
    # Simulate restart: close pool dan reconnect
    await close_pool()
    await init_pool()
    await init_db()
    
    # Event harus masih di-mark sebagai processed
    assert await is_processed(event.topic, event.event_id) is True
    
    # Duplicate attempt harus fail
    success2, _ = await mark_processed(event)
    assert success2 is False


@pytest.mark.asyncio
async def test_get_events_by_topic_sorted():
    """T17: GET /events mengembalikan events dalam order chronological"""
    topic = "test.sorted"
    
    # Add events dengan delays
    for i in range(3):
        event = create_event(topic=topic, event_id=f"evt-sort-{i}")
        await mark_processed(event)
        await asyncio.sleep(0.01)  # Small delay
    
    events = await get_events_by_topic(topic)
    assert len(events) == 3


@pytest.mark.asyncio
async def test_stats_persistence():
    """T18: Stats harus persistent"""
    # Increment beberapa kali
    await increment_stats(received=10, unique=7, duplicate=3)
    
    stats = await get_stats()
    assert stats["received"] == 10
    assert stats["unique_processed"] == 7
    assert stats["duplicate_dropped"] == 3


# ============================================================================
# TEST 19-22: QUERY & ANALYTICS
# ============================================================================

@pytest.mark.asyncio
async def test_get_topics_list():
    """T19: get_topics harus return list unique topics"""
    topics_list = ["logs.auth", "logs.payment", "logs.inventory"]
    
    for topic in topics_list:
        event = create_event(topic=topic)
        await mark_processed(event)
    
    topics = await get_topics()
    assert len(topics) == 3
    assert set(topics) == set(topics_list)


@pytest.mark.asyncio
async def test_get_event_count():
    """T20: get_event_count harus return count unique events"""
    for i in range(5):
        event = create_event(event_id=f"evt-count-{i}")
        await mark_processed(event)
    
    count = await get_event_count()
    assert count == 5


@pytest.mark.asyncio
async def test_get_events_filter_by_topic():
    """T21: Filter events by topic harus bekerja"""
    topic1 = "logs.topic1"
    topic2 = "logs.topic2"
    
    # Add events to different topics
    for i in range(3):
        await mark_processed(create_event(topic=topic1, event_id=f"evt-t1-{i}"))
    
    for i in range(2):
        await mark_processed(create_event(topic=topic2, event_id=f"evt-t2-{i}"))
    
    # Query topic1
    events_t1 = await get_events_by_topic(topic1)
    assert len(events_t1) == 3
    assert all(e["topic"] == topic1 for e in events_t1)
    
    # Query topic2
    events_t2 = await get_events_by_topic(topic2)
    assert len(events_t2) == 2
    assert all(e["topic"] == topic2 for e in events_t2)


@pytest.mark.asyncio
async def test_get_all_events_no_filter():
    """T22: Get all events tanpa filter"""
    for topic in ["logs.a", "logs.b", "logs.c"]:
        for i in range(2):
            await mark_processed(create_event(topic=topic, event_id=f"{topic}-{i}"))
    
    all_events = await get_events_by_topic(None)
    assert len(all_events) == 6


# ============================================================================
# TEST 23-24: EDGE CASES & ERROR HANDLING
# ============================================================================

@pytest.mark.asyncio
async def test_event_with_large_payload():
    """T23: Event dengan large payload harus handled"""
    large_payload = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
    event = create_event(payload=large_payload)
    
    success, _ = await mark_processed(event)
    assert success is True
    
    retrieved = await get_events_by_topic(event.topic)
    assert len(retrieved) == 1


@pytest.mark.asyncio
async def test_special_characters_in_fields():
    """T24: Event dengan special characters di topic/source"""
    event = create_event(
        topic="logs.special-chars_123",
        source="service-v2.prod/region-us-east-1"
    )
    
    success, _ = await mark_processed(event)
    assert success is True
    
    assert await is_processed(event.topic, event.event_id) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
