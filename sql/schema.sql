CREATE TABLE IF NOT EXISTS ingestion_jobs (
    job_id VARCHAR(36) PRIMARY KEY,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    progress_percent NUMERIC(5,2) NOT NULL DEFAULT 0,
    total_rows INTEGER,
    processed_rows INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trips_clean (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(255) NOT NULL,
    region VARCHAR(255),
    origin_lat DOUBLE PRECISION NOT NULL,
    origin_lon DOUBLE PRECISION NOT NULL,
    destination_lat DOUBLE PRECISION NOT NULL,
    destination_lon DOUBLE PRECISION NOT NULL,
    origin_cell VARCHAR(64) NOT NULL,
    destination_cell VARCHAR(64) NOT NULL,
    trip_datetime TIMESTAMP NOT NULL,
    time_of_day VARCHAR(32) NOT NULL,
    datasource VARCHAR(255),
    event_hash VARCHAR(64) UNIQUE NOT NULL,
    ingestion_job_id VARCHAR(36),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trips_clean_trip_datetime ON trips_clean (trip_datetime);
CREATE INDEX IF NOT EXISTS idx_trips_clean_region ON trips_clean (region);
CREATE INDEX IF NOT EXISTS idx_trips_clean_city ON trips_clean (city);
CREATE INDEX IF NOT EXISTS idx_trips_clean_origin_coords ON trips_clean (origin_lat, origin_lon);
CREATE INDEX IF NOT EXISTS idx_trips_clean_destination_coords ON trips_clean (destination_lat, destination_lon);
CREATE INDEX IF NOT EXISTS idx_trips_clean_grouping ON trips_clean (origin_cell, destination_cell, time_of_day);

CREATE TABLE IF NOT EXISTS trip_group_summary (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(255) NOT NULL,
    region VARCHAR(255),
    origin_cell VARCHAR(64) NOT NULL,
    destination_cell VARCHAR(64) NOT NULL,
    time_of_day VARCHAR(32) NOT NULL,
    trip_count INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_trip_group_summary_key UNIQUE (city, region, origin_cell, destination_cell, time_of_day)
);

CREATE TABLE IF NOT EXISTS weekly_region_stats (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(255) NOT NULL,
    region VARCHAR(255),
    week_start DATE NOT NULL,
    trip_count INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_weekly_region_stats_key UNIQUE (city, region, week_start)
);
