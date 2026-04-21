"""
Explicit schemas for maintenance domain data.

This module defines PySpark StructType schemas for all maintenance event types
(FAILURE, REPAIR, DOWNTIME) to ensure type safety and data quality at ingestion.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ==============================================================================
# EVENT COMPONENT SCHEMAS (Reusable Building Blocks)
# ==============================================================================

# Producer context schema: identifies the system, region, site, and device producing the event
PRODUCER_SCHEMA = StructType([
    StructField("system", StringType(), False),
    StructField("region", StringType(), False),
    StructField("site_id", StringType(), False),
    StructField("device_id", StringType(), False),
])

# Truck context schema: describes truck attributes and operational metrics
TRUCK_SCHEMA = StructType([
    StructField("truck_id", StringType(), False),
    StructField("vin", StringType(), False),
    StructField("make", StringType(), False),
    StructField("model", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("capacity_tons", DoubleType(), False),
    StructField("home_site_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("odometer_miles", DoubleType(), False),
    StructField("engine_hours", DoubleType(), False),
])

# Location context schema: describes site and geospatial information
LOCATION_SCHEMA = StructType([
    StructField("site_id", StringType(), False),
    StructField("site_name", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("geofence_zone", StringType(), True),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
])

# Weather context schema: describes environmental conditions at event time
WEATHER_SCHEMA = StructType([
    StructField("condition", StringType(), False),
    StructField("temperature_f", DoubleType(), False),
    StructField("humidity_pct", DoubleType(), False),
    StructField("wind_mph", DoubleType(), False),
    StructField("visibility_miles", DoubleType(), False),
    StructField("severity_level", StringType(), False),
])

# Sensor readings schema: contains truck sensor metrics (nullable)
SENSOR_READINGS_SCHEMA = StructType([
    StructField("battery_voltage", DoubleType(), True),
    StructField("engine_temp_f", DoubleType(), True),
    StructField("hydraulic_pressure_psi", DoubleType(), True),
    StructField("brake_line_pressure_psi", DoubleType(), True),
    StructField("oil_pressure_psi", DoubleType(), True),
    StructField("tire_pressure_psi", DoubleType(), True),
])

# Diagnostics schema: contains fault codes and sensor readings (nullable)
DIAGNOSTICS_SCHEMA = StructType([
    StructField("fault_codes", ArrayType(StringType()), True),
    StructField("sensor_readings", SENSOR_READINGS_SCHEMA, True),
])

# Failure event schema: describes failure details and diagnostics (nullable)
FAILURE_SCHEMA = StructType([
    StructField("failure_id", StringType(), True),
    StructField("failure_timestamp", StringType(), True),  # NEW: Single point in time when failure occurred
    StructField("failure_type", StringType(), True),
    StructField("failure_code", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("symptoms", StringType(), True),
    StructField("diagnostics", DIAGNOSTICS_SCHEMA, True),
])

# Repair event schema: describes repair details with failure linkage (nullable)
REPAIR_SCHEMA = StructType([
    StructField("repair_id", StringType(), True),
    StructField("addresses_failure_id", StringType(), True),  # NEW: Links to failure_id
    StructField("repair_status", StringType(), True),
    StructField("repair_category", StringType(), True),
    StructField("labor_hours", DoubleType(), True),
    StructField("repair_start_timestamp", StringType(), True),  # NEW: When repair work began
    StructField("repair_end_timestamp", StringType(), True),    # RENAMED from completion_timestamp
])

# DOWNTIME_SCHEMA REMOVED - downtime will be derived in gold layer from failure → repair

# Technician schema: describes technician details (nullable)
TECHNICIAN_SCHEMA = StructType([
    StructField("technician_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("certification_level", StringType(), True),
])

# Part schema: describes part details used in service (nullable)
PART_SCHEMA = StructType([
    StructField("part_id", StringType(), True),
    StructField("part_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_cost", DoubleType(), True),
])

# Service schema: describes vendor, technicians, and parts used (nullable)
SERVICE_SCHEMA = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("vendor_name", StringType(), True),
    StructField("technicians", ArrayType(TECHNICIAN_SCHEMA), True),
    StructField("parts_used", ArrayType(PART_SCHEMA), True),
])

# Notes schema: contains freeform notes from driver, dispatcher, and maintenance (nullable)
NOTES_SCHEMA = StructType([
    StructField("driver_note", StringType(), True),
    StructField("dispatcher_note", StringType(), True),
    StructField("maintenance_note", StringType(), True),
])


# ==============================================================================
# BRONZE LAYER: COMPLETE EVENT SCHEMA
# ==============================================================================

# Maintenance event schema: top-level schema for all maintenance events
MAINTENANCE_EVENT_SCHEMA = StructType([
    # Top-level event metadata (NOT NULL - these are critical)
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_version", StringType(), False),
    StructField("event_timestamp", StringType(), False),  # ISO 8601 string (will parse in silver)
    
    # Context sections (NOT NULL - always present)
    StructField("producer", PRODUCER_SCHEMA, False),
    StructField("truck", TRUCK_SCHEMA, False),
    StructField("location", LOCATION_SCHEMA, False),
    StructField("weather", WEATHER_SCHEMA, False),
    
    # Event-specific sections (NULLABLE - depends on event_type)
    StructField("failure", FAILURE_SCHEMA, True),
    StructField("repair", REPAIR_SCHEMA, True),
    # REMOVED: StructField("downtime", DOWNTIME_SCHEMA, True) - downtime derived in gold
    StructField("service", SERVICE_SCHEMA, True),
    
    # Additional metadata (NULLABLE)
    StructField("notes", NOTES_SCHEMA, True),
    StructField("tags", ArrayType(StringType()), True),
])
