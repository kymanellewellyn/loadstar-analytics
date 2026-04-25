"""
Gold layer pipeline for maintenance analytics.

This pipeline creates business-ready gold tables and views for operational
dashboards, KPIs, and analysis. Gold layer aggregates and enriches silver data
for consumption by BI tools and analysts.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, expr, lit, coalesce, to_date,
    year, month, dayofmonth, quarter, dayofweek, weekofyear,
    date_format,
    row_number, sum as _sum, explode
)
from pyspark.sql.window import Window

# ==============================================================================
# GOLD LAYER: DIMENSIONS
# ==============================================================================

@dp.materialized_view(
    name="dim_date",
    comment="Gold layer dimension: Calendar date attributes for time-based analysis",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_date():
    """
    Date dimension with calendar attributes.
    
    Extracts unique dates from all event timestamps and enriches with:
    - Calendar attributes (year, month, day, quarter)
    - Day names and week information
    - Fiscal period support (can be added later)
    
    Grain: One row per unique date
    """
    # Get all unique dates from failure and repair events
    failure_dates = spark.read.table("failure_events").select(
        to_date(col("failure_timestamp")).alias("date")
    )
    
    repair_dates = spark.read.table("repair_events").select(
        to_date(col("repair_start_timestamp")).alias("date")
    )
    
    # Union and deduplicate
    all_dates = failure_dates.union(repair_dates).distinct()
    
    # Add calendar attributes
    return all_dates.select(
        col("date"),
        year(col("date")).alias("year"),
        month(col("date")).alias("month"),
        dayofmonth(col("date")).alias("day"),
        quarter(col("date")).alias("quarter"),
        dayofweek(col("date")).alias("day_of_week"),  # 1=Sunday, 7=Saturday
        date_format(col("date"), "EEEE").alias("day_name"),
        weekofyear(col("date")).alias("week_of_year"),
        date_format(col("date"), "MMMM").alias("month_name")
    )


@dp.materialized_view(
    name="dim_truck",
    comment="Gold layer dimension: Truck/fleet asset master with SCD Type 2 history tracking",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_truck():
    """
    Truck dimension with SCD Type 2 history tracking.
    
    Reads from truck_master (Unity Catalog managed Delta table) which maintains
    complete history of truck attribute changes using slowly changing dimension
    Type 2 pattern.
    
    Grain: One row per truck_sk (unique for each version of truck attributes)
    
    Use is_current = true to get latest attributes, or query historical versions
    using effective_start_date/effective_end_date for point-in-time analysis.
    """
    return spark.read.table("loadstar_dev.maintenance.truck_master")


@dp.materialized_view(
    name="dim_site",
    comment="Gold layer dimension: Site/location master with geographic attributes",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_site():
    """
    Site dimension with location details.
    
    Extracts unique sites with geographic attributes from failure_events.
    
    Grain: One row per site_id
    """
    # Get unique sites from failure_events
    window_spec = Window.partitionBy("location_site_id").orderBy(col("failure_timestamp").desc())
    
    return (
        spark.read.table("failure_events")
        .select(
            col("location_site_id"),
            col("location_site_name"),
            col("location_latitude"),
            col("location_longitude"),
            col("location_city"),
            col("location_state"),
            col("failure_timestamp")
        )
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num", "failure_timestamp")
        .withColumn("site_sk", row_number().over(Window.orderBy("location_site_id")))
        .select(
            col("site_sk"),
            col("location_site_id").alias("site_id"),
            col("location_site_name").alias("site_name"),
            col("location_latitude").alias("latitude"),
            col("location_longitude").alias("longitude"),
            col("location_city").alias("city"),
            col("location_state").alias("state")
        )
    )


@dp.materialized_view(
    name="dim_failure_type",
    comment="Gold layer dimension: Failure type classification",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_failure_type():
    """
    Failure type dimension.
    
    Distinct failure types from failure_events.
    
    Grain: One row per failure_type
    """
    return (
        spark.read.table("failure_events")
        .select(col("failure_type"))
        .distinct()
        .withColumn("failure_type_sk", row_number().over(Window.orderBy("failure_type")))
        .select(
            col("failure_type_sk"),
            col("failure_type")
        )
    )


@dp.materialized_view(
    name="dim_vendor",
    comment="Gold layer dimension: Repair vendor/service provider master",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_vendor():
    """
    Vendor dimension.
    
    Distinct vendors from repair_events.
    
    Grain: One row per vendor_id
    """
    return (
        spark.read.table("repair_events")
        .select(
            col("vendor_id"),
            col("vendor_name")
        )
        .distinct()
        .withColumn("vendor_sk", row_number().over(Window.orderBy("vendor_id")))
        .select(
            col("vendor_sk"),
            col("vendor_id"),
            col("vendor_name")
        )
    )


@dp.materialized_view(
    name="dim_weather_condition",
    comment="Gold layer dimension: Weather condition classification",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_weather_condition():
    """
    Weather condition dimension.
    
    Distinct weather conditions from failure_events for correlation analysis.
    
    Grain: One row per weather_condition
    """
    return (
        spark.read.table("failure_events")
        .select(col("weather_condition"))
        .distinct()
        .withColumn("weather_condition_sk", row_number().over(Window.orderBy("weather_condition")))
        .select(
            col("weather_condition_sk"),
            col("weather_condition")
        )
    )


# ==============================================================================
# GOLD LAYER: FACT TABLES
# ==============================================================================

@dp.materialized_view(
    name="fact_failure_event",
    comment="Gold layer fact: Failure events with dimensional keys and measures, using SCD Type 2 temporal joins",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def fact_failure_event():
    """
    Failure event fact table with SCD Type 2 temporal joins.
    
    Grain: One row per failure event
    
    Joins failure_events with dimensions to create surrogate keys.
    Uses temporal join for dim_truck to find the correct truck version
    based on failure_timestamp (point-in-time accuracy).
    
    Temporal join logic:
    - truck_id matches
    - failure_timestamp >= effective_start_date
    - failure_timestamp < effective_end_date (or effective_end_date IS NULL for current)
    
    Future enhancements (Phase 3):
    - time_since_last_failure_hours (window function)
    - is_repeat_failure_flag (window function)
    """
    failures = spark.read.table("failure_events").alias("f")
    dim_truck_df = spark.read.table("dim_truck").alias("t")
    dim_site_df = spark.read.table("dim_site").alias("s")
    dim_failure_type_df = spark.read.table("dim_failure_type").alias("ft")
    dim_weather_df = spark.read.table("dim_weather_condition").alias("w")
    
    return (
        failures
        # Temporal join to dim_truck (SCD Type 2)
        .join(
            dim_truck_df,
            (col("f.truck_id") == col("t.truck_id")) &
            (to_date(col("f.failure_timestamp")) >= col("t.effective_start_date")) &
            (
                col("t.effective_end_date").isNull() |
                (to_date(col("f.failure_timestamp")) < col("t.effective_end_date"))
            ),
            "left"
        )
        # Join to dim_site
        .join(dim_site_df, col("f.location_site_id") == col("s.site_id"), "left")
        # Join to dim_failure_type
        .join(dim_failure_type_df, col("f.failure_type") == col("ft.failure_type"), "left")
        # Join to dim_weather_condition
        .join(dim_weather_df, col("f.weather_condition") == col("w.weather_condition"), "left")
        .select(
            # Degenerate dimension (no separate dim table)
            col("f.failure_id").alias("failure_event_id"),
            
            # Foreign keys to dimensions
            to_date(col("f.failure_timestamp")).alias("date_key"),
            col("t.truck_sk"),
            col("s.site_sk"),
            col("ft.failure_type_sk"),
            col("w.weather_condition_sk"),
            
            # Timestamps
            col("f.failure_timestamp"),
            
            # Failure attributes
            col("f.failure_code"),
            col("f.severity"),
            
            # Measures - operational metrics
            col("f.truck_odometer_miles"),
            col("f.truck_engine_hours"),
            
            # Measures - sensor readings
            col("f.battery_voltage"),
            col("f.engine_temp_f"),
            col("f.hydraulic_pressure_psi"),
            col("f.brake_line_pressure_psi"),
            col("f.oil_pressure_psi"),
            col("f.tire_pressure_psi"),
            
            # Weather measures
            col("f.weather_temperature_f"),
            col("f.weather_humidity_pct")
        )
    )


@dp.materialized_view(
    name="fact_repair_event",
    comment="Gold layer fact: Repair events with dimensional keys, measures, calculated costs, and SCD Type 2 temporal joins",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def fact_repair_event():
    """
    Repair event fact table with SCD Type 2 temporal joins.
    
    Grain: One row per repair event
    
    Joins repair_events with dimensions to create surrogate keys.
    Uses temporal join for dim_truck to find the correct truck version
    based on repair_start_timestamp (point-in-time accuracy).
    
    Calculates repair_cost from parts_used array (quantity * unit_cost).
    Calculates repair_duration_hours from start/end timestamps.
    
    Links to failures via addresses_failure_id (can join to fact_failure_event).
    """
    repairs = spark.read.table("repair_events").alias("r")
    dim_truck_df = spark.read.table("dim_truck").alias("t")
    dim_site_df = spark.read.table("dim_site").alias("s")
    dim_vendor_df = spark.read.table("dim_vendor").alias("v")
    
    # Calculate total parts cost
    repairs_with_cost = (
        repairs
        .withColumn("part", explode(col("parts_used")))
        .withColumn("part_cost", col("part.quantity") * col("part.unit_cost"))
        .groupBy(
            col("event_id"),
            col("repair_id"),
            col("addresses_failure_id"),
            col("truck_id"),
            col("location_site_id"),
            col("vendor_id"),
            col("repair_status"),
            col("repair_category"),
            col("labor_hours"),
            col("repair_start_timestamp"),
            col("repair_end_timestamp")
        )
        .agg(_sum("part_cost").alias("total_parts_cost"))
        .alias("r")
    )
    
    return (
        repairs_with_cost
        # Temporal join to dim_truck (SCD Type 2)
        .join(
            dim_truck_df,
            (col("r.truck_id") == col("t.truck_id")) &
            (to_date(col("r.repair_start_timestamp")) >= col("t.effective_start_date")) &
            (
                col("t.effective_end_date").isNull() |
                (to_date(col("r.repair_start_timestamp")) < col("t.effective_end_date"))
            ),
            "left"
        )
        # Join to dim_site
        .join(dim_site_df, col("r.location_site_id") == col("s.site_id"), "left")
        # Join to dim_vendor
        .join(dim_vendor_df, col("r.vendor_id") == col("v.vendor_id"), "left")
        .select(
            # Degenerate dimensions
            col("r.repair_id").alias("repair_event_id"),
            col("r.addresses_failure_id"),  # Link to failure
            
            # Foreign keys to dimensions
            to_date(col("r.repair_start_timestamp")).alias("date_key"),
            col("t.truck_sk"),
            col("s.site_sk"),
            col("v.vendor_sk"),
            
            # Timestamps
            col("r.repair_start_timestamp"),
            col("r.repair_end_timestamp"),
            
            # Repair attributes
            col("r.repair_status"),
            col("r.repair_category"),
            
            # Measures
            col("r.labor_hours"),
            col("r.total_parts_cost").alias("repair_cost"),
            expr("(unix_timestamp(repair_end_timestamp) - unix_timestamp(repair_start_timestamp)) / 3600.0").alias("repair_duration_hours")
        )
    )


# ==============================================================================
# GOLD LAYER: Operational Views
# ==============================================================================

@dp.materialized_view(
    name="current_open_failures",
    comment="Gold layer: Failures without completed repairs - trucks currently down and needing attention",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def current_open_failures():
    """
    Identify failures that have NOT been repaired yet (open failures).
    
    This view:
    - Joins failure_events with repair_events on failure_id
    - Filters for failures with NO matching repair (repair_id IS NULL)
    - Returns the full failure record for operational triage
    
    Use cases:
    - Operations dashboard: "Which trucks are down right now?"
    - Maintenance prioritization: "Which failures need immediate attention?"
    - Fleet availability tracking: "How many trucks are out of service?"
    - Repair scheduling: "What work is pending?"
    
    Business rule: A failure is "open" if no repair event links to it via
    addresses_failure_id. Once a repair is recorded, the failure is "closed".
    """
    failures = spark.read.table("failure_events").alias("f")
    repairs = spark.read.table("repair_events").alias("r")
    
    return (
        failures
        .join(
            repairs,
            col("f.failure_id") == col("r.addresses_failure_id"),
            "left"
        )
        .filter(col("r.repair_id").isNull())
        .select("f.*")
    )
