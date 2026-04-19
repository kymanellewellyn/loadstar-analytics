from src.common.config import CATALOG, DOMAINS


def get_volume_path(domain, subdir=None):
    """Generate Unity Catalog volume path for a domain."""
    if domain not in DOMAINS:
        raise ValueError(f"Unsupported domain: {domain}")
    
    config = DOMAINS[domain]
    base_path = f"/Volumes/{CATALOG}/{config['raw_schema']}/{config['raw_volume']}"
    
    return f"{base_path}/{subdir}" if subdir else base_path