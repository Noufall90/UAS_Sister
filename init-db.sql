-- Create processed_events table
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
);

-- Create dedup_store table
CREATE TABLE IF NOT EXISTS dedup_store (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (topic, event_id)
);

-- Create event_stats table
CREATE TABLE IF NOT EXISTS event_stats (
    id INT PRIMARY KEY DEFAULT 1,
    received INT DEFAULT 0,
    unique_processed INT DEFAULT 0,
    duplicate_dropped INT DEFAULT 0,
    CONSTRAINT single_row CHECK (id = 1)
);

-- Initialize stats with default values
INSERT INTO event_stats (id, received, unique_processed, duplicate_dropped)
VALUES (1, 0, 0, 0)
ON CONFLICT (id) DO NOTHING;

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_processed_events_topic 
ON processed_events(topic);

CREATE INDEX IF NOT EXISTS idx_processed_events_received_at 
ON processed_events(received_at DESC);

CREATE INDEX IF NOT EXISTS idx_dedup_store_topic_event_id 
ON dedup_store(topic, event_id);

-- Grant permissions to aggregator user
GRANT ALL PRIVILEGES ON DATABASE log_aggregator TO aggregator;
GRANT ALL PRIVILEGES ON SCHEMA public TO aggregator;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO aggregator;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO aggregator;
