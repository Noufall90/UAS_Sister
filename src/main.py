from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import ValidationError
import asyncio
import logging
import time
from typing import List, Optional, Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager

from src.models import Event, PublishRequest, PublishResponse, EventResponse, StatsResponse, HealthResponse
from src.database import (
    init_pool, close_pool, init_db, is_processed, mark_processed,
    get_events_by_topic, get_stats, get_topics, get_event_count,
    increment_stats
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
_startup_time = None
_consumer_task = None
_queue: asyncio.Queue = None


async def consumer_worker():
    """Worker yang memproses event dari queue secara berurutan"""
    while True:
        try:
            event = await _queue.get()
            logger.debug(f"Consumer processing event: {event.topic}/{event.event_id}")
            
            # Check apakah sudah diproses sebelumnya
            already_processed = await is_processed(event.topic, event.event_id)
            
            if already_processed:
                # Duplikat ditemukan
                await increment_stats(duplicate=1)
                logger.info(f"✗ Duplicate dropped: {event.topic}/{event.event_id}")
            else:
                # Event baru, process dan simpan
                success, error = await mark_processed(event)
                if success:
                    await increment_stats(unique=1)
                    logger.info(f"✓ Event processed: {event.topic}/{event.event_id}")
                else:
                    # Mungkin race condition, event sudah diproses oleh worker lain
                    await increment_stats(duplicate=1)
                    logger.warning(f"✗ Event rejected: {error} - {event.topic}/{event.event_id}")
            
            _queue.task_done()
        except Exception as e:
            logger.error(f"Error in consumer worker: {e}")
            _queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager untuk startup/shutdown"""
    global _startup_time, _consumer_task, _queue
    
    # Startup
    logger.info("Starting aggregator service...")
    _startup_time = time.time()
    _queue = asyncio.Queue(maxsize=10000)
    
    try:
        # Initialize database pool dengan retry logic
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                await init_pool()
                await init_db()
                logger.info("Database initialized successfully")
                break
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Failed to initialize database after {max_retries} retries: {e}")
                    raise
                logger.warning(f"Database connection failed (attempt {retry_count}/{max_retries}): {e}")
                await asyncio.sleep(2)
        
        # Start consumer worker
        _consumer_task = asyncio.create_task(consumer_worker())
        logger.info("Consumer worker started")
        
        yield
    finally:
        # Shutdown
        logger.info("Shutting down aggregator service...")
        
        # Cancel consumer task
        if _consumer_task:
            _consumer_task.cancel()
            try:
                await _consumer_task
            except asyncio.CancelledError:
                pass
        
        # Wait untuk queue kosong
        await _queue.join()
        logger.info("Queue emptied")
        
        # Close database pool
        await close_pool()
        logger.info("Aggregator service stopped")


# Create FastAPI app
app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Sistem log aggregator terdistribusi dengan idempotency, dedup, transaksi/konkurensi",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint untuk orchestration (Compose)"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat() + "Z",
        version="1.0.0"
    )


@app.post("/publish", response_model=PublishResponse, tags=["Publishing"])
async def publish_events(request: PublishRequest):
    """
    Publish event ke aggregator.
    Menerima single event atau batch event.
    Mengembalikan status accepted, rejected, dan error details.
    
    Idempotency: Event dengan (topic, event_id) yang sama hanya diproses sekali.
    """
    events_to_publish = request.get_events()
    accepted = 0
    rejected = 0
    errors = []
    
    try:
        for idx, event in enumerate(events_to_publish):
            try:
                # Validasi event schema
                validated_event = Event(**event.dict())
                
                # Increment received count
                await increment_stats(received=1)
                
                # Put ke queue untuk diproses async
                await _queue.put(validated_event)
                accepted += 1
                logger.debug(f"Event accepted for processing: {event.topic}/{event.event_id}")
                
            except ValidationError as e:
                rejected += 1
                error_detail = {
                    "index": idx,
                    "event_id": event.get("event_id", "unknown"),
                    "error": str(e)
                }
                errors.append(error_detail)
                logger.warning(f"Event validation failed: {error_detail}")
            except Exception as e:
                rejected += 1
                error_detail = {
                    "index": idx,
                    "error": str(e)
                }
                errors.append(error_detail)
                logger.error(f"Unexpected error processing event: {error_detail}")
        
        return PublishResponse(
            status="accepted",
            count=len(events_to_publish),
            accepted=accepted,
            rejected=rejected,
            errors=errors
        )
    
    except Exception as e:
        logger.error(f"Error in publish endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events", response_model=List[EventResponse], tags=["Events"])
async def get_events(topic: Optional[str] = Query(None, description="Filter by topic")):
    """
    Get semua unique events yang sudah diproses.
    Optionally filter by topic.
    """
    try:
        events = await get_events_by_topic(topic)
        return events
    except Exception as e:
        logger.error(f"Error getting events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/events", response_model=PublishResponse, tags=["Events"])
async def post_events(request: PublishRequest):
    """
    POST to /events is redirected to /publish for convenience.
    Publish event ke aggregator.
    Menerima single event atau batch event.
    """
    return await publish_events(request)


@app.get("/stats", response_model=StatsResponse, tags=["Statistics"])
async def get_aggregator_stats():
    """
    Get statistik aggregator: received, unique_processed, duplicate_dropped, topics, uptime.
    """
    try:
        stats = await get_stats()
        topics = await get_topics()
        uptime = time.time() - _startup_time
        
        total = stats["received"]
        unique = stats["unique_processed"]
        duplicate = stats["duplicate_dropped"]
        
        unique_rate = (unique / total * 100) if total > 0 else 0
        duplicate_rate = (duplicate / total * 100) if total > 0 else 0
        
        return StatsResponse(
            received=total,
            unique_processed=unique,
            duplicate_dropped=duplicate,
            topics=topics,
            uptime_seconds=uptime,
            unique_rate=round(unique_rate, 2),
            duplicate_rate=round(duplicate_rate, 2)
        )
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/clear", tags=["Admin"])
async def admin_clear_data():
    """
    Clear semua data (hanya untuk testing/development).
    DANGER: menghapus semua events dan stats.
    """
    try:
        from src.database import clear_all_data
        await clear_all_data()
        return {"status": "success", "message": "All data cleared"}
    except Exception as e:
        logger.error(f"Error clearing data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/info", tags=["Info"])
async def get_info():
    """Get informasi tentang aggregator"""
    uptime = time.time() - _startup_time
    event_count = await get_event_count()
    
    return {
        "service": "Pub-Sub Log Aggregator",
        "version": "1.0.0",
        "uptime_seconds": uptime,
        "total_unique_events": event_count,
        "database": "PostgreSQL 16",
        "features": [
            "Idempotent consumer",
            "At-least-once delivery",
            "SERIALIZABLE transaction isolation",
            "Unique constraint deduplication",
            "Concurrent processing",
            "Event batching",
            "Persistent dedup store"
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info"
    )
