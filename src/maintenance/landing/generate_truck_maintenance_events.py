# Example: Professional approach to avoid hardcoding in Databricks notebooks

# 1. Use Databricks widgets for pipeline/job centric parameters
dbutils.widgets.text("event_date", "2026-03-16")
event_date = dbutils.widgets.get("event_date")

# 2. Use environment variables or Databricks secrets for sensitive values
truck_events_path = dbutils.secrets.get(scope="loadstar-secrets", key="truck_events_path")

 3. Use config files for global/asset bundle centric values
import json

with open("/dbfs/mnt/loadstar/config/maintenance_config.json", "r") as f:
    config = json.load(f)

_events_schema = config["raw_events_schema"]
asset_bundle_id = config["asset_bundle_id"]

# 4. Generate raw maintenance truck events dynamically
raw_events_df = (
    spark.read.format("json")
    .schema(raw_events_schema)
    .load(f"{truck_events_path}/{asset_bundle_id}/