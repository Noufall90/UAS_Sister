import asyncpg
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import json
import os

logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/log_aggregator"
)

# Connection pool
_pool = None


async def init_pool():
    """Initialize database connection pool"""
    global _pool
    if _pool is not None:
        return
    
    try:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=5,
            max_size=20,
            command_timeout=60,
        )
        logger.info("Database pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise


async def close_pool():
    """Close database connection pool"""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


async def init_db():
    """Initialize database schema - tables created by init-db.sql"""
    try:
        async with _pool.acquire() as conn:
            # Verify tables exist
            result = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'processed_events'
                )
            """)
            
            if result:
                logger.info("Database schema verified successfully")
            else:
                logger.warning("Database tables not found, attempting to create...")
                # Fallback: create tables if init-db.sql didn't run
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS processed_events (
                        id BIGSERIAL PRIMARY KEY,
                        topic TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        source TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (topic, event_id)
                    )
                """)
                
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS dedup_store (
                        id BIGSERIAL PRIMARY KEY,
                        topic TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (topic, event_id)
                    )
                """)
                
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS event_stats (
                        id INT PRIMARY KEY DEFAULT 1,
                        received INT DEFAULT 0,
                        unique_processed INT DEFAULT 0,
                        duplicate_dropped INT DEFAULT 0,
                        CONSTRAINT single_row CHECK (id = 1)
                    )
                """)
                
                await conn.execute("""
                    INSERT INTO event_stats (id, received, unique_processed, duplicate_dropped)
                    VALUES (1, 0, 0, 0)
                    ON CONFLICT DO NOTHING
                """)
                
                logger.info("Database schema created successfully")
                
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise


@asynccontextmanager
async def get_connection():
    """Get connection dari pool dengan automatic release"""
    if _pool is None:
        raise RuntimeError("Database pool not initialized")
    
    conn = await _pool.acquire()
    try:
        yield conn
    finally:
        await _pool.release(conn)


async def is_processed(topic: str, event_id: str) -> bool:
    """Check apakah event sudah diproses (idempotency check)"""
    async with get_connection() as conn:
        result = await conn.fetchval(
            "SELECT 1 FROM dedup_store WHERE topic = $1 AND event_id = $2",
            topic, event_id
        )
        return result is not None


async def mark_processed(event) -> Tuple[bool, Optional[str]]:
    """
    Mark event sebagai processed dengan transaksi SERIALIZABLE.
    Mengembalikan (success, error_message)
    
    Menggunakan INSERT ... ON CONFLICT DO NOTHING untuk atomik dedup.
    Ini mencegah race condition saat multiple workers memproses event yang sama.
    """
    async with get_connection() as conn:
        async with conn.transaction(isolation='serializable'):
            try:
                # Insert ke dedup store (dengan unique constraint)
                await conn.execute(
                    """
                    INSERT INTO dedup_store (topic, event_id)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    """,
                    event.topic, event.event_id
                )
                
                # Insert ke processed events (dengan unique constraint)
                await conn.execute(
                    """
                    INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT DO NOTHING
                    """,
                    event.topic,
                    event.event_id,
                    event.timestamp,
                    event.source,
                    json.dumps(event.payload)
                )
                
                return True, None
            except asyncpg.UniqueViolationError as e:
                logger.warning(f"Duplicate event rejected: {event.topic}/{event.event_id}")
                return False, "Duplicate event"
            except Exception as e:
                logger.error(f"Error marking event as processed: {e}")
                return False, str(e)


async def get_events_by_topic(topic: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get semua processed events, optionally filter by topic"""
    async with get_connection() as conn:
        if topic:
            rows = await conn.fetch(
                """
                SELECT topic, event_id, timestamp, source, payload, processed_at
                FROM processed_events
                WHERE topic = $1
                ORDER BY processed_at ASC
                """,
                topic
            )
        else:
            rows = await conn.fetch(
                """
                SELECT topic, event_id, timestamp, source, payload, processed_at
                FROM processed_events
                ORDER BY processed_at ASC
                """
            )
        
        return [
            {
                "topic": row["topic"],
                "event_id": row["event_id"],
                "timestamp": row["timestamp"],
                "source": row["source"],
                "payload": row["payload"],
                "processed_at": row["processed_at"].isoformat() if row["processed_at"] else None
            }
            for row in rows
        ]


async def increment_stats(received: int = 0, unique: int = 0, duplicate: int = 0):
    """
    Increment statistics dengan transaksi untuk mencegah lost-update
    saat multiple workers melakukan update concurrent.
    """
    async with get_connection() as conn:
        async with conn.transaction(isolation='serializable'):
            try:
                await conn.execute(
                    """
                    UPDATE event_stats
                    SET 
                        received = received + $1,
                        unique_processed = unique_processed + $2,
                        duplicate_dropped = duplicate_dropped + $3
                    WHERE id = 1
                    """,
                    received, unique, duplicate
                )
            except Exception as e:
                logger.error(f"Error updating stats: {e}")
                raise


async def get_stats() -> Dict[str, Any]:
    """Get current statistics"""
    async with get_connection() as conn:
        stats = await conn.fetchrow(
            "SELECT received, unique_processed, duplicate_dropped FROM event_stats WHERE id = 1"
        )
        
        if not stats:
            return {
                "received": 0,
                "unique_processed": 0,
                "duplicate_dropped": 0
            }
        
        return {
            "received": stats["received"],
            "unique_processed": stats["unique_processed"],
            "duplicate_dropped": stats["duplicate_dropped"]
        }


async def get_topics() -> List[str]:
    """Get daftar unik topik yang telah diproses"""
    async with get_connection() as conn:
        topics = await conn.fetch(
            "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
        )
        return [row["topic"] for row in topics]


async def get_event_count() -> int:
    """Get total unique events yang telah diproses"""
    async with get_connection() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM processed_events")
        return count or 0


async def clear_all_data():
    """Clear semua data (untuk testing)"""
    async with get_connection() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE processed_events CASCADE")
            await conn.execute("TRUNCATE dedup_store CASCADE")
            await conn.execute("UPDATE event_stats SET received = 0, unique_processed = 0, duplicate_dropped = 0 WHERE id = 1")
