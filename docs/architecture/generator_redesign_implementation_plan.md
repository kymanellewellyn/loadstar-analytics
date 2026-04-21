# **Synthetic Data Generator Redesign - Implementation Plan**

## **Executive Summary**

**Problem**: Current generator creates:
- FAILURE events with single timestamp (no duration tracking)
- REPAIR events with unlinked `failure_id` (generates NEW random IDs, not referencing actual failures)
- DOWNTIME events as separate events (semantically incorrect - should be derived)

**Solution**: Redesign generator to:
1. Generate FAILURE events with `failure_timestamp` (point in time)
2. Generate REPAIR events that **reference actual FAILURE events** via `addresses_failure_id`
3. Add `repair_start_timestamp` and `repair_end_timestamp` to REPAIR events
4. **Remove DOWNTIME event generation** (will derive in gold layer)
5. Ensure temporal logic (repairs happen AFTER failures)

---

## **Current Generator Architecture** (Broken)

### **File**: `src/maintenance/landing/generate_truck_maintenance_events.py`
- Randomly selects event builders (failure, repair, downtime)
- Events are **completely independent** (no linkage)

### **Event Builders**: `src/maintenance/landing/event_builders.py`

**Current FAILURE event** (lines 134-188):
```python
def create_failure_event(base_timestamp):
    event_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    
    return {
        "event_timestamp": format_timestamp_as_utc(event_timestamp),  # Single point
        "failure": {
            "failure_id": f"failure_{uuid.uuid4().hex[:10]}",  # Random ID
            # No start/end timestamps
        },
        "repair": None,
        "downtime": {...},  # Should be removed
    }
```

**Current REPAIR event** (lines 191-266):
```python
def create_repair_event(base_timestamp):
    downtime_start_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    repair_end_timestamp = downtime_start_timestamp + timedelta(hours=random.randint(1, 8))
    
    return {
        "event_timestamp": format_timestamp_as_utc(repair_end_timestamp),
        "failure": {
            "failure_id": f"failure_{uuid.uuid4().hex[:10]}",  # ❌ NEW random ID (not linked!)
        },
        "repair": {
            "repair_id": f"repair_{uuid.uuid4().hex[:10]}",
            "completion_timestamp": format_timestamp_as_utc(repair_end_timestamp),  # Redundant!
            # No repair_start_timestamp
        },
    }
```

**Issue**: `failure_id` in REPAIR events is NEWLY GENERATED, not referencing actual FAILURE events!

---

## **New Generator Architecture** (Fixed)

### **High-Level Flow**
```
1. Generate N FAILURE events
   ↓
2. For each failure, decide: Will it get repaired? (80% yes, 20% no)
   ↓
3. Generate REPAIR events that reference specific failures
   ↓
4. Interleave events by timestamp (chronological order)
   ↓
5. Write to volume
```

### **New Event Builder Functions**

#### **1. Updated FAILURE Event**
```python
def create_failure_event(base_timestamp, failure_id=None):
    """
    Generate a FAILURE event.
    
    Args:
        base_timestamp: Base time anchor
        failure_id: Optional pre-generated failure_id (for linkage)
    
    Returns:
        tuple: (failure_event_dict, failure_metadata_for_repair)
    """
    event_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    
    # Generate failure_id if not provided
    if failure_id is None:
        failure_id = f"failure_{uuid.uuid4().hex[:10]}"
    
    selected_failure = random.choice(FAILURE_TYPES)
    selected_truck = random.choice(TRUCKS)
    
    failure_event = {
        "event_id": f"event_{uuid.uuid4().hex}",
        "event_type": "FAILURE",
        "event_timestamp": format_timestamp_as_utc(event_timestamp),
        "truck": {...},  # Include truck details
        "failure": {
            "failure_id": failure_id,
            "failure_timestamp": format_timestamp_as_utc(event_timestamp),  # NEW: Point in time
            "failure_type": selected_failure["failure_type"],
            # Remove start/end timestamps (just one point in time)
        },
        "repair": None,
        # "downtime": {...},  # REMOVED - will derive in gold
    }
    
    # Return metadata needed to generate linked repair
    failure_metadata = {
        "failure_id": failure_id,
        "failure_timestamp": event_timestamp,
        "failure_type": selected_failure["failure_type"],
        "truck_id": selected_truck["truck_id"],
        "truck": selected_truck,
    }
    
    return failure_event, failure_metadata
```

#### **2. Updated REPAIR Event**
```python
def create_repair_event_for_failure(failure_metadata, base_timestamp):
    """
    Generate a REPAIR event that addresses a specific FAILURE.
    
    Args:
        failure_metadata: Dict containing failure_id, failure_timestamp, failure_type, truck details
        base_timestamp: Base time anchor (for reference, but uses failure_timestamp)
    
    Returns:
        repair_event_dict
    """
    failure_timestamp = failure_metadata["failure_timestamp"]
    failure_id = failure_metadata["failure_id"]
    failure_type = failure_metadata["failure_type"]
    truck = failure_metadata["truck"]
    
    # Repair starts 1-24 hours after failure (wait time for diagnosis, parts, mechanic)
    repair_start_timestamp = failure_timestamp + timedelta(
        hours=random.randint(1, 24),
        minutes=random.randint(0, 59)
    )
    
    # Repair duration: 1-8 hours
    labor_hours = round(random.uniform(1.0, 8.0), 1)
    repair_end_timestamp = repair_start_timestamp + timedelta(hours=labor_hours)
    
    selected_vendor = random.choice(VENDORS)
    
    repair_event = {
        "event_id": f"event_{uuid.uuid4().hex}",
        "event_type": "REPAIR",
        "event_timestamp": format_timestamp_as_utc(repair_end_timestamp),  # Event time = when repair completed
        "truck": {...},  # Same truck as failure
        "failure": None,  # No failure section in repair event
        "repair": {
            "repair_id": f"repair_{uuid.uuid4().hex[:10]}",
            "addresses_failure_id": failure_id,  # ⭐ NEW: Links to actual failure!
            "repair_status": "COMPLETED",
            "repair_category": f"{failure_type}_REPAIR",
            "labor_hours": labor_hours,
            "repair_start_timestamp": format_timestamp_as_utc(repair_start_timestamp),  # ⭐ NEW
            "repair_end_timestamp": format_timestamp_as_utc(repair_end_timestamp),      # ⭐ NEW (renamed from completion_timestamp)
        },
        "service": {...},  # vendor, technicians, parts
    }
    
    return repair_event
```

#### **3. Remove `create_downtime_event`**
```python
# DELETE THIS FUNCTION - downtime will be derived in gold layer
```

---

## **New Generation Logic**

### **File**: `src/maintenance/landing/generate_truck_maintenance_events.py`

```python
def create_raw_events(number_of_events, random_seed=42, base_timestamp=None):
    """
    Generate a list of synthetic raw maintenance events with proper linkage.
    
    Strategy:
    1. Generate failures first (60% of events)
    2. Generate repairs for 80% of failures (linked via failure_id)
    3. Interleave events chronologically
    """
    random.seed(random_seed)
    
    if base_timestamp is None:
        base_timestamp = datetime.now(timezone.utc) - timedelta(days=7)
    
    # Calculate event distribution
    num_failures = int(number_of_events * 0.60)  # 60% failures
    num_repairs = int(num_failures * 0.80)       # 80% of failures get repaired
    
    all_events = []
    failure_metadata_list = []
    
    # Step 1: Generate FAILURE events
    print(f"Generating {num_failures} failure events...")
    for _ in range(num_failures):
        failure_event, failure_metadata = create_failure_event(base_timestamp)
        all_events.append(failure_event)
        failure_metadata_list.append(failure_metadata)
    
    # Step 2: Generate REPAIR events for random subset of failures
    print(f"Generating {num_repairs} repair events (80% of failures)...")
    failures_to_repair = random.sample(failure_metadata_list, num_repairs)
    
    for failure_metadata in failures_to_repair:
        repair_event = create_repair_event_for_failure(failure_metadata, base_timestamp)
        all_events.append(repair_event)
    
    # Step 3: Sort all events by timestamp (chronological order)
    all_events.sort(key=lambda e: e["event_timestamp"])
    
    print(f"Generated {len(all_events)} total events:")
    print(f"  - {num_failures} FAILURE events")
    print(f"  - {num_repairs} REPAIR events ({num_failures - num_repairs} unrepaired failures)")
    
    return all_events
```

---

## **Schema Changes**

### **File**: `src/maintenance/schemas.py`

```python
# ==============================================================================
# UPDATED SCHEMAS
# ==============================================================================

FAILURE_SCHEMA = StructType([
    StructField("failure_id", StringType(), True),
    StructField("failure_timestamp", StringType(), True),  # ⭐ NEW: Single point in time (ISO 8601)
    StructField("failure_type", StringType(), True),
    StructField("failure_code", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("symptoms", StringType(), True),
    StructField("diagnostics", DIAGNOSTICS_SCHEMA, True),
])

REPAIR_SCHEMA = StructType([
    StructField("repair_id", StringType(), True),
    StructField("addresses_failure_id", StringType(), True),  # ⭐ NEW: Foreign key to failure_id
    StructField("repair_status", StringType(), True),
    StructField("repair_category", StringType(), True),
    StructField("labor_hours", DoubleType(), True),
    StructField("repair_start_timestamp", StringType(), True),  # ⭐ NEW: When repair work began
    StructField("repair_end_timestamp", StringType(), True),    # ⭐ RENAMED from completion_timestamp
])

# DELETE THIS - downtime will be calculated, not stored
# DOWNTIME_SCHEMA = StructType([...])

MAINTENANCE_EVENT_SCHEMA = StructType([
    # ... keep existing fields ...
    
    # Event-specific sections (NULLABLE)
    StructField("failure", FAILURE_SCHEMA, True),
    StructField("repair", REPAIR_SCHEMA, True),
    # StructField("downtime", DOWNTIME_SCHEMA, True),  # ⭐ REMOVED
    StructField("service", SERVICE_SCHEMA, True),
    
    # ... keep existing fields ...
])
```

---

## **Silver Layer Updates**

### **File**: `pipelines/maintenance/silver_pipeline.py`

#### **1. Update `failure_events` Table**
```python
@dlt.table(
    name="failure_events",
    comment="Cleaned failure events with flattened structure"
)
def failure_events():
    return (
        dlt.read_stream("maintenance_events_clean")
        .filter(F.col("event_type") == "FAILURE")
        .select(
            # Core identifiers
            "event_id",
            "event_timestamp",
            
            # Truck context
            "truck_id", "vin", "make", "model", "year",
            "capacity_tons", "home_site_id", "status",
            "odometer_miles", "engine_hours",
            
            # Location context
            "site_id", "site_name", "latitude", "longitude",
            "geofence_zone", "city", "state",
            
            # Failure details
            "failure_id",
            "failure_timestamp",  # ⭐ NEW: Point in time when failure occurred
            "failure_type",
            "failure_code",
            "severity",
            "symptoms",
            
            # Diagnostics
            "fault_codes",
            "battery_voltage", "engine_temp_f",
            "hydraulic_pressure_psi", "brake_line_pressure_psi",
            "oil_pressure_psi", "tire_pressure_psi",
            
            # Weather
            "weather_condition", "temperature_f", "humidity_pct",
            "wind_mph", "visibility_miles", "severity_level",
        )
    )
```

#### **2. Update `repair_events` Table**
```python
@dlt.table(
    name="repair_events",
    comment="Cleaned repair events with flattened structure and failure linkage"
)
def repair_events():
    return (
        dlt.read_stream("maintenance_events_clean")
        .filter(F.col("event_type") == "REPAIR")
        .select(
            # Core identifiers
            "event_id",
            "event_timestamp",
            
            # Truck context
            "truck_id", "vin", "make", "model",
            
            # Location context
            "site_id", "site_name", "city", "state",
            
            # Repair details
            "repair_id",
            "addresses_failure_id",     # ⭐ NEW: Links to failure_events.failure_id
            "repair_status",
            "repair_category",
            "labor_hours",
            "repair_start_timestamp",   # ⭐ NEW: When repair began
            "repair_end_timestamp",     # ⭐ RENAMED from completion_timestamp
            
            # Service details
            "vendor_id", "vendor_name",
            "technicians",  # Array
            "parts_used",   # Array
        )
    )
```

#### **3. Remove `downtime_events` Table**
```python
# DELETE THIS - downtime will be calculated in gold layer
```

---

## **Gold Layer: Downtime Calculation**

### **File**: `pipelines/maintenance/gold_pipeline.py`

```python
@dlt.table(
    name="fact_truck_downtime",
    comment="Truck downtime derived from failure → repair linkage"
)
def fact_truck_downtime():
    """
    Calculate truck downtime by joining failures to repairs.
    
    Downtime = repair_end_timestamp - failure_timestamp
    
    Includes:
    - Diagnostic/wait time = repair_start - failure
    - Repair duration = repair_end - repair_start
    - Total downtime = repair_end - failure
    """
    
    failures = dlt.read("failure_events").select(
        F.col("failure_id"),
        F.col("failure_timestamp"),
        F.col("failure_type"),
        F.col("truck_id"),
        F.col("site_id"),
        F.col("severity"),
    )
    
    repairs = dlt.read("repair_events").select(
        F.col("addresses_failure_id").alias("failure_id"),  # Join key
        F.col("repair_id"),
        F.col("repair_start_timestamp"),
        F.col("repair_end_timestamp"),
        F.col("repair_status"),
        F.col("labor_hours"),
        F.col("vendor_id"),
    )
    
    return (
        failures
        .join(
            repairs,
            on="failure_id",
            how="left"  # Include unrepaired failures (repair columns will be NULL)
        )
        .select(
            # Identifiers
            F.col("failure_id"),
            F.col("repair_id"),
            F.col("truck_id"),
            F.col("site_id"),
            F.col("failure_type"),
            F.col("vendor_id"),
            
            # Timestamps
            F.col("failure_timestamp"),
            F.col("repair_start_timestamp"),
            F.col("repair_end_timestamp"),
            
            # Calculated downtime metrics
            (F.unix_timestamp("repair_start_timestamp") - F.unix_timestamp("failure_timestamp")) / 3600
                .alias("diagnostic_wait_hours"),
            
            (F.unix_timestamp("repair_end_timestamp") - F.unix_timestamp("repair_start_timestamp")) / 3600
                .alias("repair_duration_hours"),
            
            (F.unix_timestamp("repair_end_timestamp") - F.unix_timestamp("failure_timestamp")) / 3600
                .alias("total_downtime_hours"),
            
            # Flags
            F.when(F.col("repair_id").isNotNull(), True).otherwise(False)
                .alias("was_repaired"),
            
            F.when(F.col("repair_status") == "COMPLETED", True).otherwise(False)
                .alias("repair_successful"),
        )
    )
```

---

## **Migration Steps**

### **Phase 1: Backup (Optional)**
```sql
-- If current data is valuable
CREATE TABLE loadstar_dev.maintenance.maintenance_events_bronze_backup_20260420 AS
SELECT * FROM loadstar_dev.maintenance.maintenance_events_bronze;
```

### **Phase 2: Drop Existing Tables**
```sql
DROP TABLE IF EXISTS loadstar_dev.maintenance.downtime_events CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.repair_events CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.failure_events CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.maintenance_events_clean CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.maintenance_events_bronze CASCADE;
```

### **Phase 3: Update Code**
1. ✅ Update `src/maintenance/schemas.py`
2. ✅ Update `src/maintenance/landing/event_builders.py`
3. ✅ Update `src/maintenance/landing/generate_truck_maintenance_events.py`
4. ✅ Update `pipelines/maintenance/silver_pipeline.py`

### **Phase 4: Regenerate Data**
```python
# Clear existing raw data
dbutils.fs.rm("/Volumes/loadstar_dev/maintenance/truck_maintenance_events_raw/events", recurse=True)

# Generate new data with correct schema
%run /Workspace/Users/kymane.llewellyn@gmail.com/Projects/loadstar-analytics/src/maintenance/landing/generate_truck_maintenance_events.py
```

### **Phase 5: Run Pipeline**
1. Start pipeline update (bronze → silver)
2. Verify data quality
3. Build gold layer

---

## **Data Quality Validation**

After regeneration, validate:

```sql
-- 1. Verify failure-repair linkage
SELECT 
    COUNT(DISTINCT f.failure_id) AS total_failures,
    COUNT(DISTINCT r.addresses_failure_id) AS repaired_failures,
    COUNT(DISTINCT r.addresses_failure_id) * 100.0 / COUNT(DISTINCT f.failure_id) AS repair_rate_pct
FROM loadstar_dev.maintenance.failure_events f
LEFT JOIN loadstar_dev.maintenance.repair_events r
    ON f.failure_id = r.addresses_failure_id;

-- Expected: ~80% repair rate

-- 2. Verify temporal ordering (repairs after failures)
SELECT 
    f.failure_id,
    f.failure_timestamp,
    r.repair_start_timestamp,
    r.repair_end_timestamp,
    CASE 
        WHEN r.repair_start_timestamp < f.failure_timestamp THEN 'TEMPORAL_VIOLATION'
        ELSE 'OK'
    END AS temporal_check
FROM loadstar_dev.maintenance.failure_events f
JOIN loadstar_dev.maintenance.repair_events r
    ON f.failure_id = r.addresses_failure_id
WHERE r.repair_start_timestamp < f.failure_timestamp;

-- Expected: 0 rows (no temporal violations)

-- 3. Verify downtime calculations
SELECT 
    failure_id,
    repair_id,
    diagnostic_wait_hours,
    repair_duration_hours,
    total_downtime_hours,
    -- Verify math: total = diagnostic_wait + repair_duration
    ABS(total_downtime_hours - (diagnostic_wait_hours + repair_duration_hours)) AS calculation_error
FROM loadstar_dev.maintenance.fact_truck_downtime
WHERE calculation_error > 0.01;

-- Expected: 0 rows (no calculation errors)
```

---

## **Summary of Changes**

 Component | Current | New |
-----------|---------|-----|
 **FAILURE event** | Single `event_timestamp`, no duration tracking | `failure_timestamp` (point in time when failure occurred) |
 **REPAIR event** | Random `failure_id` (not linked), single `completion_timestamp` | `addresses_failure_id` (links to actual failure), `repair_start_timestamp` + `repair_end_timestamp` |
 **DOWNTIME event** | Separate events (DOWNTIME_START/END) | **REMOVED** - derived from failure → repair |
 **Generator logic** | Random independent events | Failures generated first, repairs reference specific failures |
 **Silver layer** | `downtime_events` table | **REMOVED** - downtime calculated in gold |
 **Gold layer** | Not implemented | `fact_truck_downtime` with calculated metrics |

---

## **Benefits of New Design**

✅ **Proper linkage**: Repairs reference actual failures  
✅ **Temporal logic**: Repairs always after failures  
✅ **Duration tracking**: Start/end timestamps for repairs  
✅ **Semantic correctness**: Downtime derived from failure → repair  
✅ **Data quality**: Can validate repair rates, temporal ordering  
✅ **Analytics ready**: Can track repeat failures, repair effectiveness  
✅ **No redundancy**: Each timestamp has unique meaning  

---

**Status**: 🟢 Ready for implementation  
**Estimated effort**: 2-3 hours (code changes + testing)  
**Risk**: Medium (requires dropping tables and regenerating data)  
**Mitigation**: Backup existing data (optional), validate thoroughly after regeneration
