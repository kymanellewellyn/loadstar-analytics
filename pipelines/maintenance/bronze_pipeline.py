"""
Bronze layer pipeline for maintenance events.

This pipeline ingests raw JSON events from the maintenance volume using Auto Loader
with an explicit schema for type safety and data quality validation.
"""

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

from src.common.paths import get_volume_path
from src.common.config import CATALOG, DOMAINS
from src.maintenance.schemas import MAINTENANCE_EVENT_SCHEMA

# Get domain configuration
domain = "maintenance"
domain_config = DOMAINS[domain]

# Source and target paths
SOURCE_PATH = get_volume_path(domain, "events")
TARGET_SCHEMA = domain_config["bronze_schema"]
TARGET_TABLE = domain_config["bronze_table"]

@dlt.table(
    name=TARGET_TABLE,
    comment="Bronze layer: Raw maintenance events with explicit schema validation",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def maintenance_events_bronze():
    """
    Stream raw maintenance events from volume to bronze table.
    
    Uses Auto Loader (cloudFiles) with explicit schema for:
    - Type safety (enforces correct data types)
    - Data quality gate (malformed events fail fast)
    - Incremental processing (only new files)
    - Exactly-once guarantees
    - Self-documenting (schema IS the contract)
    
    Schema evolution: New fields go to _rescued_data column for visibility.
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", 
                    f"/Volumes/{CATALOG}/{TARGET_SCHEMA}/_checkpoints/bronze_schema")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .schema(MAINTENANCE_EVENT_SCHEMA)
            .load(SOURCE_PATH)
            .select(
                "*",
                current_timestamp().alias("_ingestion_timestamp"),
                input_file_name().alias("_source_file")
            )
    )
