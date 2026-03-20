from src.common.config import CATALOG, DOMAINS


def get_domain_config(domain):
    if domain not in DOMAINS:
        raise ValueError(f"Unsupported domain: {domain}")
    return DOMAINS[domain]


def get_schema_name(domain, layer):
    domain_config = get_domain_config(domain)
    key = f"{layer}_schema"

    if key not in domain_config:
        raise ValueError(f"Unsupported layer: {layer}")

    return domain_config[key]


def get_volume_name(domain):
    return get_domain_config(domain)["raw_volume"]


def get_bronze_table_name(domain):
    return get_domain_config(domain)["bronze_table"]


def get_full_table_name(domain, layer, table_name=None):
    schema_name = get_schema_name(domain, layer)

    if table_name is None:
        if layer == "bronze":
            table_name = get_bronze_table_name(domain)
        else:
            raise ValueError("table_name is required for non-bronze layers")

    return f"{CATALOG}.{schema_name}.{table_name}"


def get_volume_path(domain, subdir=None):
    domain_config = get_domain_config(domain)
    base_path = f"/Volumes/{CATALOG}/{domain_config['raw_schema']}/{domain_config['raw_volume']}"

    if subdir:
        return f"{base_path}/{subdir}"
    return base_path