"""
Publisher service: Generate dan kirim event ke aggregator.
Mensimulasikan multiple sources dan mengirim duplikasi untuk test idempotency.
"""

import asyncio
import aiohttp
import logging
import os
import random
import uuid
import time
from typing import List, Dict, Any
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8080")
NUM_WORKERS = int(os.getenv("PUBLISHER_WORKERS", "3"))
EVENT_COUNT = int(os.getenv("EVENT_COUNT", "50000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.35"))  # 35% duplikasi

# Topics dan sources untuk realistic simulation
TOPICS = [
    "logs.authentication",
    "logs.payment",
    "logs.inventory",
    "logs.user_service",
    "logs.notification",
    "logs.database",
    "logs.cache",
    "logs.api_gateway"
]

SOURCES = [
    "service-a",
    "service-b",
    "service-c",
    "worker-1",
    "worker-2",
    "scheduler",
    "batch-job"
]

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def generate_event(event_id: str = None) -> Dict[str, Any]:
    """Generate random event"""
    return {
        "topic": random.choice(TOPICS),
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": random.choice(SOURCES),
        "payload": {
            "level": random.choice(LOG_LEVELS),
            "message": f"Log message {random.randint(1, 10000)}",
            "duration_ms": random.randint(1, 5000),
            "status": random.choice(["success", "partial", "failed"]),
            "user_id": f"user-{random.randint(1, 10000)}",
            "transaction_id": str(uuid.uuid4())
        }
    }


async def publish_batch(
    session: aiohttp.ClientSession,
    events: List[Dict[str, Any]],
    worker_id: int
) -> tuple[int, int]:
    """
    Publish batch of events ke aggregator.
    Return (successful, failed)
    """
    try:
        async with session.post(
            f"{AGGREGATOR_URL}/publish",
            json={"events": events},
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            result = await resp.json()
            
            if resp.status == 200:
                accepted = result.get("accepted", 0)
                logger.debug(f"Worker {worker_id}: Published batch, accepted={accepted}")
                return accepted, len(events) - accepted
            else:
                logger.error(f"Worker {worker_id}: Publish failed with status {resp.status}")
                return 0, len(events)
    except Exception as e:
        logger.error(f"Worker {worker_id}: Error publishing batch: {e}")
        return 0, len(events)


async def publisher_worker(worker_id: int, events_per_worker: int):
    """
    Worker yang generate dan publish events.
    Juga mengirim beberapa duplikasi dari events yang sudah dikirim.
    """
    logger.info(f"Worker {worker_id} started")
    
    sent_events = {}  # Track events yang sudah dikirim untuk duplikasi
    total_sent = 0
    total_failed = 0
    
    async with aiohttp.ClientSession() as session:
        # Generate dan publish events
        for i in range(events_per_worker):
            batch = []
            batch_size = random.randint(5, 50)
            
            for _ in range(batch_size):
                # Tentukan apakah event baru atau duplikasi
                if sent_events and random.random() < DUPLICATE_RATE:
                    # Send duplikasi dari event yang sudah dikirim
                    dup_event = random.choice(list(sent_events.values()))
                    batch.append(dup_event)
                else:
                    # Generate event baru
                    event = generate_event()
                    sent_events[event["event_id"]] = event
                    batch.append(event)
            
            # Publish batch
            accepted, failed = await publish_batch(session, batch, worker_id)
            total_sent += accepted
            total_failed += failed
            
            # Jitter untuk simulate realistic load
            await asyncio.sleep(random.uniform(0.01, 0.1))
        
        logger.info(
            f"Worker {worker_id} finished: sent={total_sent}, failed={total_failed}, "
            f"unique_events_tracked={len(sent_events)}"
        )


async def run_publisher():
    """
    Run publisher dengan multiple workers.
    Mengirim total EVENT_COUNT events terdistribusi across workers.
    """
    logger.info(f"Starting publisher with {NUM_WORKERS} workers")
    logger.info(f"Total events to generate: {EVENT_COUNT}")
    logger.info(f"Duplicate rate: {DUPLICATE_RATE * 100:.1f}%")
    logger.info(f"Target aggregator: {AGGREGATOR_URL}")
    
    # Wait untuk aggregator ready
    logger.info("Waiting for aggregator to be ready...")
    max_retries = 30
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{AGGREGATOR_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        logger.info("✓ Aggregator is ready")
                        break
        except Exception as e:
            logger.debug(f"Aggregator not ready, retrying... ({attempt + 1}/{max_retries})")
            await asyncio.sleep(1)
    else:
        logger.error("Aggregator failed to start in time")
        return
    
    # Start publishers
    start_time = time.time()
    events_per_worker = EVENT_COUNT // NUM_WORKERS
    
    tasks = [
        publisher_worker(i, events_per_worker)
        for i in range(NUM_WORKERS)
    ]
    
    await asyncio.gather(*tasks)
    
    elapsed = time.time() - start_time
    logger.info(f"\n✓ Publisher completed in {elapsed:.2f}s")
    logger.info(f"  Throughput: {EVENT_COUNT / elapsed:.0f} events/second")
    
    # Get final stats
    await asyncio.sleep(2)  # Wait untuk semua events diproses
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{AGGREGATOR_URL}/stats") as resp:
                stats = await resp.json()
                logger.info("\nFinal Aggregator Stats:")
                logger.info(f"  Received: {stats['received']}")
                logger.info(f"  Unique processed: {stats['unique_processed']}")
                logger.info(f"  Duplicates dropped: {stats['duplicate_dropped']}")
                logger.info(f"  Unique rate: {stats['unique_rate']:.2f}%")
                logger.info(f"  Duplicate rate: {stats['duplicate_rate']:.2f}%")
                logger.info(f"  Topics: {len(stats['topics'])}")
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")


if __name__ == "__main__":
    asyncio.run(run_publisher())
