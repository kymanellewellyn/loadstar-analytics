# **Schema Redesign Plan: Event Timestamps & Downtime Calculation**

## **Problem Statement**

### **Current (Incorrect) Design**
```
FAILURE Event:
  - event_timestamp: "2026-04-14T13:56:59Z"  ← Single timestamp
  - No failure start/end times

REPAIR Event:
  - event_timestamp: "2026-04-14T14:00:00Z"  ← Single timestamp
  - repair.completion_timestamp: same as event_timestamp ← Not useful!
  - No repair start/end times

DOWNTIME_START/END Events:
  - Separate events with different downtime_ids
  - Not paired with FAILURE/REPAIR
  - Semantically incorrect (downtime should be derived, not separate events)
```

### **What's Wrong**
1. ❌ **No duration tracking** - Single timestamps don't capture event duration
2. ❌ **Can't calculate true downtime** - Don't know when failure started vs when repair ended
3. ❌ **Semantically incorrect** - Downtime should be `repair_end_time - failure_start_time`
4. ❌ **DOWNTIME events are redundant** - Should be derived from FAILURE→REPAIR

---

## **Correct Design**

### **Semantic Model**
```
Truck is operating normally
    ↓
FAILURE STARTS (failure_start_timestamp)
    ↓
[Truck is down - diagnosing, waiting for parts, etc.]
    ↓
FAILURE ENDS (failure_end_timestamp) - Failure diagnosed
    ↓
REPAIR STARTS (repair_start_timestamp)
    ↓
[Repair work happening]
    ↓
REPAIR ENDS (repair_end_timestamp) - Truck back in service
    ↓
Truck is operating normally

DOWNTIME = repair_end_timestamp - failure_start_timestamp
```

### **Required Schema Changes**

#### **1. FAILURE_SCHEMA** (src/maintenance/schemas.py)
```python
FAILURE_SCHEMA = StructType([
    StructField("failure_id", StringType(), True),
    StructField("failure_type", StringType(), True),
    StructField("failure_code", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("symptoms", StringType(), True),
    
    # NEW FIELDS
    StructField("failure_start_timestamp", StringType(), True),  # When failure occurred
    StructField("failure_end_timestamp", StringType(), True),    # When diagnosis complete
    
    StructField("diagnostics", DIAGNOSTICS_SCHEMA, True),
])
```

#### **2. REPAIR_SCHEMA** (src/maintenance/schemas.py)
```python
REPAIR_SCHEMA = StructType([
    StructField("repair_id", StringType(), True),
    StructField("repair_status", StringType(), True),
    StructField("repair_category", StringType(), True),
    StructField("labor_hours", DoubleType(), True),
    
    # CHANGED FIELDS
    StructField("repair_start_timestamp", StringType(), True),   # When repair work began
    StructField("repair_end_timestamp", StringType(), True),     # When repair completed (was "completion_timestamp")
])
```

#### **3. Remove DOWNTIME_SCHEMA**
```python
# DELETE THIS - downtime will be calculated, not stored
# DOWNTIME_SCHEMA = ...
```

#### **4. Remove downtime from MAINTENANCE_EVENT_SCHEMA**
```python
MAINTENANCE_EVENT_SCHEMA = StructType([
    # ... keep existing fields ...
    
    # Event-specific sections (NULLABLE - depends on event_type)
    StructField("failure", FAILURE_SCHEMA, True),
    StructField("repair", REPAIR_SCHEMA, True),
    # StructField("downtime", DOWNTIME_SCHEMA, True),  ← DELETE THIS
    StructField("service", SERVICE_SCHEMA, True),
    
    # ... keep existing fields ...
])
```

---

## **Impact Assessment**

### **Files That Need Changes**

#### **1. Source Data Generator** (if we control it)
- Update to generate events with start/end timestamps
- Remove DOWNTIME_START/DOWNTIME_END generation

#### **2. Schema Definition**
- ✅ `src/maintenance/schemas.py` - Update FAILURE_SCHEMA, REPAIR_SCHEMA, remove DOWNTIME_SCHEMA

#### **3. Bronze Pipeline**
- ✅ `pipelines/maintenance/bronze_pipeline.py` - No changes (reads schema from schemas.py)

#### **4. Silver Pipeline**
- ✅ `pipelines/maintenance/silver_pipeline.py` - Update to flatten new timestamp fields
- ❌ Remove `downtime_events` table definition

#### **5. Gold Pipeline**
- ⚠️ `pipelines/maintenance/gold_pipeline.py` - Create `fact_truck_downtime` with calculated downtime

### **Tables That Need Dropping & Recreating**

```sql
-- All existing tables must be dropped (schema change)
DROP TABLE IF EXISTS loadstar_dev.maintenance.maintenance_events_bronze CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.maintenance_events_clean CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.failure_events CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.repair_events CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.downtime_events CASCADE;
```

---

## **Downtime Calculation Strategy**

### **Option 1: Calculate in Gold (Query-Time)** ⭐ RECOMMENDED
```sql
-- In gold layer, join FAILURE → REPAIR and calculate
SELECT 
    f.failure_id,
    r.repair_id,
    f.failure_start_timestamp,
    r.repair_end_timestamp,
    (UNIX_TIMESTAMP(r.repair_end_timestamp) - UNIX_TIMESTAMP(f.failure_start_timestamp)) / 3600 
        AS downtime_hours
FROM failure_events f
JOIN repair_events r 
    ON f.truck_id = r.truck_id
    AND r.repair_start_timestamp >= f.failure_start_timestamp
    AND r.repair_start_timestamp <= f.failure_end_timestamp + INTERVAL 24 HOURS
```

**Pros:**
- ✅ Single source of truth (timestamps in failure/repair tables)
- ✅ Easy to recalculate if logic changes
- ✅ No storage overhead
- ✅ Flexible (can adjust join logic)

**Cons:**
- ❌ Query overhead (join + calculation every time)
- ❌ Slower for dashboards with millions of rows

### **Option 2: Pre-Aggregate in Gold (Materialized Table)**
```python
@dlt.table(name="fact_truck_downtime")
def fact_truck_downtime():
    return (
        # Join failure → repair, pre-calculate downtime
        # Store as table
    )
```

**Pros:**
- ✅ Fast query performance (pre-calculated)
- ✅ Good for dashboards/BI tools
- ✅ Can add aggregations (daily rollups, etc.)

**Cons:**
- ❌ Storage overhead
- ❌ Must re-run pipeline to recalculate
- ❌ Duplicate data (timestamps in both facts and downtime table)

### **Option 3: Materialized View in Gold**
```sql
CREATE MATERIALIZED VIEW fact_truck_downtime AS
SELECT 
    -- Pre-calculate downtime, refresh on schedule
FROM failure_events f
JOIN repair_events r ...
```

**Pros:**
- ✅ Fast query performance
- ✅ Can refresh on schedule
- ✅ Less manual pipeline management

**Cons:**
- ❌ Databricks Unity Catalog materialized views have limitations
- ❌ Refresh logic can be complex

---

## **Recommendation**

### **Phase 1: Immediate (Fix Schema)**
1. ✅ Update `schemas.py` with new timestamp fields
2. ✅ Remove DOWNTIME_SCHEMA
3. ✅ Drop all existing tables
4. ✅ Re-run pipeline to recreate with new schema

### **Phase 2: Silver Layer**
1. ✅ Update silver to flatten new timestamps
2. ✅ Remove `downtime_events` table
3. ✅ Keep `failure_events` and `repair_events` with new timestamps

### **Phase 3: Gold Layer**
1. ⭐ **Use Option 1: Query-time calculation** (start simple)
2. ✅ Create `fact_truck_downtime` with calculated downtime
3. ✅ Join failure → repair on truck_id + temporal proximity
4. ✅ Calculate: `downtime_hours = repair_end - failure_start`

### **Phase 4: Optimize Later (If Needed)**
- If query performance becomes an issue (>5 sec)
- Switch to Option 2: Pre-aggregated table
- Or add indexes/partitions to improve join performance

---

## **Migration Steps**

### **Step 1: Backup (if needed)**
```sql
-- Optional: Export current data if valuable
CREATE TABLE maintenance_events_bronze_backup AS 
SELECT * FROM loadstar_dev.maintenance.maintenance_events_bronze;
```

### **Step 2: Drop Tables**
```sql
DROP TABLE IF EXISTS loadstar_dev.maintenance.downtime_events CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.repair_events CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.failure_events CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.maintenance_events_clean CASCADE;
DROP TABLE IF EXISTS loadstar_dev.maintenance.maintenance_events_bronze CASCADE;
```

### **Step 3: Update Schema**
- Edit `src/maintenance/schemas.py`
- Add timestamp fields to FAILURE_SCHEMA, REPAIR_SCHEMA
- Remove DOWNTIME_SCHEMA

### **Step 4: Re-run Pipeline**
- Run bronze → silver pipeline
- Data will be re-ingested with new schema

### **Step 5: Build Gold Layer**
- Create dimension tables
- Create fact tables with calculated downtime

---

## **Open Questions**

### **1. Do we control the source data?**
- **If YES**: Update data generator to emit new timestamp fields
- **If NO**: Can we enrich/derive timestamps from existing data?

### **2. Historical data?**
- Do we need to preserve existing 850 events?
- Or can we re-generate with correct schema?

### **3. Failure→Repair matching logic?**
- How to handle multiple repairs for one failure?
- Time window for matching (24 hours? 48 hours?)
- What if no repair follows a failure?

---

## **Next Steps**

**User Decisions Needed:**
1. ✅ Confirm: Use Option 1 (query-time calculation) for downtime?
2. ✅ Confirm: OK to drop all existing tables?
3. ⚠️ Do we control source data generator? Can we fix it?
4. ⚠️ Failure→Repair matching rules?

**Once decided:**
1. Update schemas
2. Drop tables
3. Re-run pipeline
4. Build gold layer one table at a time

---

**Status**: 🟡 Plan Ready - Awaiting User Approval
