# **Catalog & Schema Naming Conventions**

## **Overview**

Loadstar Analytics uses a **unified schema per domain** approach following Databricks best practices for Lakeflow Spark Declarative Pipelines.

---

## **Catalog Structure**

### **Development Environment**
```
loadstar_dev/
├── maintenance/          # Unified schema for maintenance domain
│   ├── _checkpoints/     # Volume: Auto Loader checkpoints
│   ├── truck_maintenance_events_raw/  # Volume: raw JSON events
│   ├── maintenance_events_bronze      # Table: bronze layer
│   ├── maintenance_events_clean       # Table: silver layer
│   ├── failure_events                 # Table: silver layer
│   ├── repair_events                  # Table: silver layer
│   └── downtime_events                # Table: silver layer
└── transport/            # Unified schema for transport domain
    ├── _checkpoints/     # Volume: Auto Loader checkpoints
    ├── transport_events_raw/         # Volume: raw JSON events
    └── (tables TBD)
```

### **Production Environment**
```
loadstar/
├── maintenance/          # Production maintenance data
└── transport/            # Production transport data
```

---

## **Naming Rules**

### **1. Catalog Names**
- **Development**: `loadstar_dev`
- **Production**: `loadstar`

### **2. Schema Names**
- **One schema per business domain** (not per layer)
- Format: `{domain_name}` (e.g., `maintenance`, `transport`)
- ❌ **DO NOT**: Create separate schemas for bronze/silver/gold layers
- ✅ **DO**: Use unified schema with layer indicated by table properties

### **3. Table Names**
- Include layer context in table name when helpful
- Format: `{entity}_{layer}` or `{entity}_events`
- Examples:
  - `maintenance_events_bronze` (bronze layer)
  - `maintenance_events_clean` (silver layer)
  - `failure_events` (silver - derived entity)

### **4. Volume Names**
- Format: `{descriptive_name}` 
- Examples:
  - `truck_maintenance_events_raw` (data volume)
  - `_checkpoints` (Auto Loader metadata)

---

## **Why Unified Schemas?**

### **Benefits**
1. **Simpler lineage**: All related tables in one schema
2. **Easier discovery**: One place to look for domain data
3. **Standard pattern**: Matches Databricks recommended practices
4. **Less governance overhead**: Fewer schemas to manage

### **Layer Distinction**
Layers are distinguished by:
- **Table names** (e.g., `_bronze`, `_clean` suffixes)
- **Table properties** (`quality` = `bronze`/`silver`/`gold`)
- **Pipeline lineage** (visible in Lakeflow DAG)

---

## **Migration Notes**

### **Old Structure (Deprecated)**
```
loadstar/
├── maintenance_raw/      # ❌ Deprecated
├── maintenance_bronze/   # ❌ Deprecated
├── maintenance_silver/   # ❌ Deprecated
├── maintenance_gold/     # ❌ Deprecated
├── transport_raw/        # ❌ Deprecated
├── transport_bronze/     # ❌ Deprecated
├── transport_silver/     # ❌ Deprecated
└── transport_gold/       # ❌ Deprecated
```

**Status**: All deprecated schemas have been dropped (they were empty).

---

## **Configuration Reference**

Current configuration in `src/common/config.py`:

```python
CATALOG = "loadstar_dev"  # Change to "loadstar" for production

DOMAINS = {
    "maintenance": {
        "schema": "maintenance",
        "raw_volume": "truck_maintenance_events_raw",
        "bronze_table": "maintenance_events_bronze",
    },
    "transport": {
        "schema": "transport",
        "raw_volume": "transport_events_raw",
        "bronze_table": "transport_events_bronze",
    }
}
```

---

## **References**

- **Pipeline Root**: `/Users/kymane.llewellyn@gmail.com/Projects/loadstar-analytics`
- **Maintenance Pipeline**: `pipelines/maintenance/`
- **Transport Pipeline**: `pipelines/transport/` (TBD)
- **Config Module**: `src/common/config.py`

---

**Last Updated**: [Current Date]
**Status**: ✅ Active Convention
