# **Gold Layer Implementation Plan**

## **Business Objectives (Objective 3: Minimize Unplanned Downtime)**

### **Key Metrics to Support**
1. **Unplanned downtime hours per truck**
2. **% downtime that is unplanned**
3. **Mean Time Between Failures (MTBF)**
4. **Mean Time To Repair (MTTR)**
5. **Repeat failure rate**

---

## **Current Silver Layer Assets**

### **Tables Available**
1. ✅ `maintenance_events_bronze` (850 rows) - Raw streaming data
2. ✅ `maintenance_events_clean` (850 rows) - Deduplicated, flattened
3. ✅ `failure_events` (266 rows) - FAILURE events only
4. ✅ `repair_events` (296 rows) - REPAIR events only
5. ⚠️ `downtime_events` (0 rows) - Empty (needs refactor for START/END pairing)

### **Event Type Distribution**
- FAILURE: 266 events (31.3%)
- REPAIR: 296 events (34.8%)
- DOWNTIME_START: 161 events (18.9%)
- DOWNTIME_END: 127 events (14.9%)

**Gap**: 34 unpaired downtime events (161 START - 127 END = 34 still in progress)

---

## **Data Model Requirements**

### **Required Dimensions (from docs)**

#### **Core Conformed Dimensions**
1. `dim_date` - Calendar attributes for time analysis
2. `dim_truck` - Fleet asset details (make, model, year, capacity)
3. `dim_site` - Location/facility information

#### **Maintenance-Specific Dimensions**
4. `dim_failure_type` - Failure classification (BRAKE, ENGINE, TIRE, etc.)
5. `dim_vendor` - Repair service providers
6. `dim_downtime_cause` - Downtime reason categories
7. `dim_weather_condition` - Weather classification for correlation analysis

### **Required Fact Tables (from docs)**

#### **1. fact_failure_event**
**Grain**: One row per failure event

**Keys**:
- failure_event_id (degenerate)
- date_key → dim_date
- truck_sk → dim_truck
- site_sk → dim_site
- failure_type_sk → dim_failure_type

**Measures**:
- ✅ `truck_odometer_miles` (available)
- ✅ `truck_engine_hours` (available)
- ❌ `time_since_last_failure_hours` (needs calculation - window function)
- ❌ `is_repeat_failure_flag` (needs calculation - window function to detect)

#### **2. fact_repair_event**
**Grain**: One row per repair event

**Keys**:
- repair_event_id (degenerate)
- date_key → dim_date
- truck_sk → dim_truck
- site_sk → dim_site
- vendor_sk → dim_vendor
- ❓ failure_type_sk → dim_failure_type (NOT in repair_events - needs join/enrichment)

**Measures**:
- ✅ `labor_hours` (available)
- ❌ `repair_cost` (needs calculation from parts_used array)
- ✅ `repair_duration_hours` (can calculate from timestamps)

#### **3. fact_truck_downtime**
**Grain**: One row per truck + date + site + cause (aggregated)

**Keys**:
- date_key → dim_date
- truck_sk → dim_truck
- site_sk → dim_site
- downtime_cause_sk → dim_downtime_cause
- weather_condition_sk → dim_weather_condition

**Measures**:
- `total_downtime_hours` (sum of all downtime)
- `unplanned_downtime_hours` (sum where is_planned = false)

**Challenge**: Need to pair DOWNTIME_START/END events to calculate duration

---

## **Data Gaps & Required Transformations**

### **1. Downtime Duration Calculation**
**Problem**: Events are START/END pairs, need to calculate duration
**Solution**: 
- Self-join on downtime_id to pair START/END
- Calculate duration: `end_timestamp - start_timestamp`
- Handle unpaired START events (still in progress) - exclude or use current time?

### **2. Time Since Last Failure**
**Problem**: Need to calculate hours since previous failure per truck
**Solution**: 
- Window function: `LAG(event_timestamp) OVER (PARTITION BY truck_id ORDER BY event_timestamp)`
- Calculate: `current_event_timestamp - previous_failure_timestamp`

### **3. Repeat Failure Detection**
**Problem**: Need to flag if same failure_type occurred before on same truck
**Solution**:
- Window function: `LAG(failure_type) OVER (PARTITION BY truck_id ORDER BY event_timestamp)`
- Flag: `current_failure_type == previous_failure_type`

### **4. Repair Cost Calculation**
**Problem**: `parts_used` is an array of structs with quantity and unit_cost
**Solution**:
- Explode array or aggregate: `SUM(quantity * unit_cost)` per repair

### **5. Repair-to-Failure Linkage**
**Problem**: Repairs don't have failure_type_sk (need for dimensional model)
**Solution**:
- Option A: Join repair back to failure on truck_id + timestamp proximity
- Option B: Leave failure_type_sk as nullable in fact_repair_event
- **Decision needed**: Which approach?

---

## **Stream-Static Joins**

**Question**: Did we implement stream-static joins in silver?

**Answer**: ❌ No, we did not.

**Potential Use Cases**:
1. **Vendor lookup** - If we had a static vendor reference table
2. **Truck reference** - If we had a static truck master table
3. **Site reference** - If we had a static site master table

**Current State**: All data comes from events (no external reference tables)

**Decision**: Build dimensions from event data (not stream-static joins)

---

## **Implementation Order**

### **Phase 1: Simple Dimensions (No Transformations)**
1. ✅ `dim_date` - Extract dates, add calendar attributes
2. ✅ `dim_truck` - Deduplicate truck attributes
3. ✅ `dim_site` - Deduplicate site attributes
4. ✅ `dim_failure_type` - Distinct failure types
5. ✅ `dim_vendor` - Distinct vendors
6. ✅ `dim_downtime_cause` - Map downtime.reason to dimension
7. ✅ `dim_weather_condition` - Distinct weather conditions

### **Phase 2: Simple Facts (No Complex Calculations)**
8. ✅ `fact_failure_event` - Without time_since_last_failure, is_repeat_failure
9. ✅ `fact_repair_event` - With calculated repair_cost, without failure_type_sk

### **Phase 3: Complex Transformations**
10. ⚠️ Add `time_since_last_failure_hours` to fact_failure_event (window function)
11. ⚠️ Add `is_repeat_failure_flag` to fact_failure_event (window function)
12. ⚠️ Create downtime pairing logic (START/END join)
13. ⚠️ Build `fact_truck_downtime` (aggregated grain)

---

## **Open Questions for User**

### **1. Downtime Pairing**
- **Q**: How to handle unpaired DOWNTIME_START events (still in progress)?
- **Options**:
  - A) Exclude from fact table (only completed downtime)
  - B) Use current timestamp as end (estimate duration)
  - C) Create separate "in_progress_downtime" table

### **2. Repair-Failure Linkage**
- **Q**: How to link repairs to failure types?
- **Options**:
  - A) Temporal join (repair within X hours of failure on same truck)
  - B) Leave failure_type_sk nullable in fact_repair_event
  - C) Assume repair_category maps to failure_type (business rule)

### **3. MTBF / MTTR Calculations**
- **Q**: Should these be calculated in gold or left for BI tool?
- **Options**:
  - A) Pre-calculate as aggregated metrics table
  - B) Provide facts, let BI calculate on the fly

### **4. Cost Data**
- **Q**: Parts have unit_cost, but no labor cost. How to handle?
- **Options**:
  - A) Only track parts cost as repair_cost
  - B) Derive labor cost (labor_hours * assumed rate)
  - C) Track parts_cost separately from labor_cost

---

## **Next Steps**

1. **User decision on open questions**
2. **Start with Phase 1: Simple dimensions** (quick wins)
3. **Build one table at a time with validation**
4. **Test with sample queries before moving to next table**
5. **Document assumptions and business rules**

---

**Status**: 🟡 Planning Complete - Awaiting User Decisions
