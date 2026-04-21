from src.common.config import CATALOG, DOMAINS


def get_volume_path(domain, subdir=None):
    """
    Generate Unity Catalog volume path for a domain.
    
    Args:
        domain: Domain name (e.g., 'maintenance', 'transport')
        subdir: Optional subdirectory within the volume (e.g., 'events', 'checkpoints')
    
    Returns:
        Full volume path
    
    Example:
        >>> get_volume_path("maintenance", "events")
        '/Volumes/loadstar_dev/maintenance/truck_maintenance_events_raw/events'
    """
    if domain not in DOMAINS:
        raise ValueError(f"Unsupported domain: {domain}")
    
    config = DOMAINS[domain]
    # Use unified schema (not separate raw_schema)
    base_path = f"/Volumes/{CATALOG}/{config['schema']}/{config['raw_volume']}"
    
    return f"{base_path}/{subdir}" if subdir else base_path
