CATALOG = "loadstar"

DOMAINS = {
    "maintenance": {
        "raw_schema": "maintenance_raw",
        "bronze_schema": "maintenance_bronze",
        "silver_schema": "maintenance_silver",
        "gold_schema": "maintenance_gold",
        "raw_volume": "truck_maintenance_events_raw",
        "bronze_table": "truck_maintenance_events"
    },
    "transport": {
        "raw_schema": "transport_raw",
        "bronze_schema": "transport_bronze",
        "silver_schema": "transport_silver",
        "gold_schema": "transport_gold",
        "raw_volume": "transport_events_raw",
        "bronze_table": "transport_events"
    }
}