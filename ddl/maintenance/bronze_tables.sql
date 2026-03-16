%sql
USE CATALOG loadstar;

CREATE TABLE IF NOT EXISTS loadstar.maintenance_bronze.truck_events (
    event_id STRING,
    truck_id STRING,
    event_type STRING,          -- FAILURE | REPAIR | DOWNTIME_START | DOWNTIME_END
    event_timestamp TIMESTAMP,
    site_id STRING,
    failure_type STRING,
    vendor_id STRING,
    weather_condition STRING,
    notes STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);