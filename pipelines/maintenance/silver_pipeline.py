"""
Silver layer pipeline for maintenance events.

This pipeline transforms bronze data into analytics-ready silver tables using
Lakeflow Declarative Pipelines (DLT) with built-in data quality expectations.
"""

import dlt
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window

from src.common.config import DOMAINS

# Get domain configuration
domain = "maintenance"
domain_config = DOMAINS[domain]

BRONZE_TABLE = domain_config["bronze_table"]

# ==============================================================================
# SILVER LAYER: Core Cleaned Data
# ==============================================================================

@dlt.table(
    name="maintenance_events_clean",
    comment="Silver layer: Cleaned and flattened maintenance events - single source of truth for FAILURE and REPAIR events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
# Data Quality Expectations - Drop records that fail critical validations
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_type", "event_type IS NOT NULL")
@dlt.expect_or_drop("valid_event_timestamp", "event_timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_truck_id", "truck_id IS NOT NULL")
# Track violations for event-specific data (don't drop, just monitor)
@dlt.expect("failure_has_data", "event_type != 'FAILURE' OR failure IS NOT NULL")
@dlt.expect("repair_has_data", "event_type != 'REPAIR' OR repair IS NOT NULL")
# REMOVED: downtime expectation - downtime will be derived in gold layer
def maintenance_events_clean():
    """
    Transform bronze data into analytics-ready silver table.
    
    Transformations:
    - Parse event_timestamp from ISO 8601 string to timestamp type
    - Flatten nested structures (truck.*, location.*, producer.*, weather.*)
    - Deduplicate events by event_id (keep latest by ingestion timestamp)
    - Apply DLT expectations for data quality validation
    - Preserve event-specific data (failure, repair, service)
    
    This table serves as the foundation for specialized silver tables
    (failure_events, repair_events) which will filter from this.
    """
    # Read from bronze and apply transformations
    df = (
        dlt.read(BRONZE_TABLE)
            # Parse timestamp from string to proper timestamp type
            .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
            
            # Flatten producer context
            .withColumn("producer_system", col("producer.system"))
            .withColumn("producer_region", col("producer.region"))
            .withColumn("producer_site_id", col("producer.site_id"))
            .withColumn("producer_device_id", col("producer.device_id"))
            
            # Flatten truck context
            .withColumn("truck_id", col("truck.truck_id"))
            .withColumn("truck_vin", col("truck.vin"))
            .withColumn("truck_make", col("truck.make"))
            .withColumn("truck_model", col("truck.model"))
            .withColumn("truck_year", col("truck.year"))
            .withColumn("truck_capacity_tons", col("truck.capacity_tons"))
            .withColumn("truck_home_site_id", col("truck.home_site_id"))
            .withColumn("truck_status", col("truck.status"))
            .withColumn("truck_odometer_miles", col("truck.odometer_miles"))
            .withColumn("truck_engine_hours", col("truck.engine_hours"))
            
            # Flatten location context
            .withColumn("location_site_id", col("location.site_id"))
            .withColumn("location_site_name", col("location.site_name"))
            .withColumn("location_latitude", col("location.latitude"))
            .withColumn("location_longitude", col("location.longitude"))
            .withColumn("location_geofence_zone", col("location.geofence_zone"))
            .withColumn("location_city", col("location.city"))
            .withColumn("location_state", col("location.state"))
            
            # Flatten weather context
            .withColumn("weather_condition", col("weather.condition"))
            .withColumn("weather_temperature_f", col("weather.temperature_f"))
            .withColumn("weather_humidity_pct", col("weather.humidity_pct"))
            .withColumn("weather_wind_mph", col("weather.wind_mph"))
            .withColumn("weather_visibility_miles", col("weather.visibility_miles"))
            .withColumn("weather_severity_level", col("weather.severity_level"))
            
            # Keep nested event-specific structures (will be used by specialized tables)
            .withColumn("failure", col("failure"))
            .withColumn("repair", col("repair"))
            # REMOVED: downtime column - no longer in schema
            .withColumn("service", col("service"))
            .withColumn("notes", col("notes"))
            .withColumn("tags", col("tags"))
    )
    
    # Deduplicate: Keep latest record per event_id based on _ingestion_timestamp
    # Note: DLT doesn't have built-in deduplication, so we handle this manually
    window_spec = Window.partitionBy("event_id").orderBy(col("_ingestion_timestamp").desc())
    df = df.withColumn("row_num", row_number().over(window_spec))
    df = df.filter(col("row_num") == 1).drop("row_num")
    
    # Select final schema
    return df.select(
        # Core event metadata
        col("event_id"),
        col("event_type"),
        col("event_version"),
        col("event_timestamp"),
        
        # Flattened producer context
        col("producer_system"),
        col("producer_region"),
        col("producer_site_id"),
        col("producer_device_id"),
        
        # Flattened truck context
        col("truck_id"),
        col("truck_vin"),
        col("truck_make"),
        col("truck_model"),
        col("truck_year"),
        col("truck_capacity_tons"),
        col("truck_home_site_id"),
        col("truck_status"),
        col("truck_odometer_miles"),
        col("truck_engine_hours"),
        
        # Flattened location context
        col("location_site_id"),
        col("location_site_name"),
        col("location_latitude"),
        col("location_longitude"),
        col("location_geofence_zone"),
        col("location_city"),
        col("location_state"),
        
        # Flattened weather context
        col("weather_condition"),
        col("weather_temperature_f"),
        col("weather_humidity_pct"),
        col("weather_wind_mph"),
        col("weather_visibility_miles"),
        col("weather_severity_level"),
        
        # Event-specific nested structures (kept nested)
        col("failure"),
        col("repair"),
        # REMOVED: downtime column
        col("service"),
        col("notes"),
        col("tags"),
        
        # Metadata from bronze layer
        col("_ingestion_timestamp"),
        col("_source_file"),
        col("_rescued_data")
    )

# ==============================================================================
# SILVER LAYER: Specialized Event Tables
# ==============================================================================

@dlt.table(
    name="failure_events",
    comment="Silver layer: Failure-specific events with flattened diagnostics and sensor data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("has_failure_details", "failure_id IS NOT NULL")
@dlt.expect("has_failure_type", "failure_type IS NOT NULL")
@dlt.expect("has_severity", "severity IS NOT NULL")
def failure_events():
    """
    Specialized table for FAILURE events with flattened diagnostics and sensor data.
    
    This table:
    - Filters only FAILURE events from maintenance_events_clean
    - Flattens failure-specific nested structures (failure.*, diagnostics.*)
    - Includes all context (truck, location, weather) for analysis
    - Enforces data quality: must be FAILURE type with failure data present
    
    Use cases:
    - Failure pattern analysis (which failure types are most common?)
    - Correlation with weather/location (do failures increase in cold weather?)
    - Truck reliability scoring (which trucks fail most often?)
    - Predictive maintenance modeling (predict next failure)
    """
    return (
        dlt.read_stream("maintenance_events_clean")
        .filter(col("event_type") == "FAILURE")
        .select(
            # Core identifiers
            col("event_id"),
            col("event_timestamp"),
            
            # Truck context (for analysis)
            col("truck_id"),
            col("truck_vin"),
            col("truck_make"),
            col("truck_model"),
            col("truck_year"),
            col("truck_capacity_tons"),
            col("truck_home_site_id"),
            col("truck_status"),
            col("truck_odometer_miles"),
            col("truck_engine_hours"),
            
            # Location context
            col("location_site_id"),
            col("location_site_name"),
            col("location_latitude"),
            col("location_longitude"),
            col("location_geofence_zone"),
            col("location_city"),
            col("location_state"),
            
            # Flatten failure details
            col("failure.failure_id").alias("failure_id"),
            to_timestamp(col("failure.failure_timestamp")).alias("failure_timestamp"),  # NEW: Point in time
            col("failure.failure_type").alias("failure_type"),
            col("failure.failure_code").alias("failure_code"),
            col("failure.severity").alias("severity"),
            col("failure.symptoms").alias("symptoms"),
            
            # Flatten diagnostics
            col("failure.diagnostics.fault_codes").alias("fault_codes"),
            col("failure.diagnostics.sensor_readings.battery_voltage").alias("battery_voltage"),
            col("failure.diagnostics.sensor_readings.engine_temp_f").alias("engine_temp_f"),
            col("failure.diagnostics.sensor_readings.hydraulic_pressure_psi").alias("hydraulic_pressure_psi"),
            col("failure.diagnostics.sensor_readings.brake_line_pressure_psi").alias("brake_line_pressure_psi"),
            col("failure.diagnostics.sensor_readings.oil_pressure_psi").alias("oil_pressure_psi"),
            col("failure.diagnostics.sensor_readings.tire_pressure_psi").alias("tire_pressure_psi"),
            
            # Weather context (for correlation analysis)
            col("weather_condition"),
            col("weather_temperature_f"),
            col("weather_humidity_pct"),
            col("weather_wind_mph"),
            col("weather_visibility_miles"),
            col("weather_severity_level"),
            
            # Metadata
            col("_ingestion_timestamp"),
            col("_source_file")
        )
    )


@dlt.table(
    name="repair_events",
    comment="Silver layer: Repair-specific events with failure linkage, vendor details, and parts tracking",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("has_repair_details", "repair_id IS NOT NULL")
@dlt.expect("has_addresses_failure_id", "addresses_failure_id IS NOT NULL")  # NEW: Ensure linkage
@dlt.expect("has_repair_timestamps", "repair_start_timestamp IS NOT NULL AND repair_end_timestamp IS NOT NULL")
def repair_events():
    """
    Specialized table for REPAIR events with failure linkage and service details.
    
    This table:
    - Filters only REPAIR events from maintenance_events_clean
    - Flattens repair-specific nested structures (repair.*, service.*)
    - Links repairs to failures via addresses_failure_id
    - Tracks repair timestamps (start and end)
    - Includes vendor, technicians, and parts used
    
    Use cases:
    - Repair effectiveness analysis (do repairs resolve failures?)
    - Vendor performance tracking (which vendors are fastest/best?)
    - Cost tracking (parts + labor costs)
    - Downtime calculation (repair_end - failure_timestamp)
    - Repeat failure detection (same failure after repair?)
    """
    return (
        dlt.read_stream("maintenance_events_clean")
        .filter(col("event_type") == "REPAIR")
        .select(
            # Core identifiers
            col("event_id"),
            col("event_timestamp"),
            
            # Truck context
            col("truck_id"),
            col("truck_vin"),
            col("truck_make"),
            col("truck_model"),
            col("truck_year"),
            col("truck_status"),
            
            # Location context
            col("location_site_id"),
            col("location_site_name"),
            col("location_city"),
            col("location_state"),
            
            # Flatten repair details
            col("repair.repair_id").alias("repair_id"),
            col("repair.addresses_failure_id").alias("addresses_failure_id"),  # NEW: Links to failure
            col("repair.repair_status").alias("repair_status"),
            col("repair.repair_category").alias("repair_category"),
            col("repair.labor_hours").alias("labor_hours"),
            to_timestamp(col("repair.repair_start_timestamp")).alias("repair_start_timestamp"),  # NEW
            to_timestamp(col("repair.repair_end_timestamp")).alias("repair_end_timestamp"),      # NEW
            
            # Flatten service details
            col("service.vendor_id").alias("vendor_id"),
            col("service.vendor_name").alias("vendor_name"),
            col("service.technicians").alias("technicians"),  # Array of technician structs
            col("service.parts_used").alias("parts_used"),    # Array of part structs
            
            # Metadata
            col("_ingestion_timestamp"),
            col("_source_file")
        )
    )


# REMOVED: downtime_events table - downtime will be derived in gold layer from failure → repair joins
