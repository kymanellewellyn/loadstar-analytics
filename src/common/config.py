"""
Configuration for Loadstar Analytics project.

Defines catalog, schema, and resource naming conventions across environments.
"""

# Catalog configuration (use loadstar_dev for development, loadstar for production)
CATALOG = "loadstar_dev"

# Domain configurations with unified schema per domain (all layers in one schema)
DOMAINS = {
    "maintenance": {
        "schema": "maintenance",  # Unified schema for all layers
        "raw_volume": "truck_maintenance_events_raw",  # Volume path for raw JSON files
        "bronze_table": "maintenance_events_bronze",  # Bronze layer table name
    },
    "transport": {
        "schema": "transport",  # Unified schema for all layers
        "raw_volume": "transport_events_raw",  # Volume path for raw JSON files
        "bronze_table": "transport_events_bronze",  # Bronze layer table name
    }
}

def get_full_table_name(domain: str, table_name: str) -> str:
    """
    Get fully qualified table name.
    
    Args:
        domain: Domain name (e.g., 'maintenance', 'transport')
        table_name: Table name without catalog/schema prefix
    
    Returns:
        Fully qualified table name (catalog.schema.table)
    
    Example:
        >>> get_full_table_name("maintenance", "failure_events")
        'loadstar_dev.maintenance.failure_events'
    """
    schema = DOMAINS[domain]["schema"]
    return f"{CATALOG}.{schema}.{table_name}"
