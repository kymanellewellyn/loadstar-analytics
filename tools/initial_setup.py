from pathlib import Path

# Base project structure
folders = [
    "docs/diagrams",

    "ddl/00_bootstrap",

    "ddl/bronze/maintenance",
    "ddl/bronze/transport",

    "ddl/silver/maintenance",
    "ddl/silver/transport",

    "ddl/gold/dim/conformed",
    "ddl/gold/dim/maintenance",
    "ddl/gold/dim/transport",

    "ddl/gold/fact/maintenance",
    "ddl/gold/fact/transport",

    "ddl/gold/mart/maintenance",
    "ddl/gold/mart/transport",

    "src/common",

    "src/maintenance/bronze",
    "src/maintenance/silver",
    "src/maintenance/gold",

    "src/transport/bronze",
    "src/transport/silver",
    "src/transport/gold",

    "data/landing/maintenance/events_inbox",
    "data/landing/maintenance/reference",

    "data/landing/transport/snapshots",
    "data/landing/transport/reference",
]

# Create folders
for folder in folders:
    Path(folder).mkdir(parents=True, exist_ok=True)

# Create placeholder files so Git tracks empty folders
placeholder_files = [
    "ddl/00_bootstrap/00_databases.sql",
    "ddl/bronze/maintenance/truck_events_raw.sql",
    "ddl/bronze/transport/transport_raw.sql",
    "ddl/silver/maintenance/truck_events_clean.sql",
    "ddl/silver/transport/transport_clean.sql",
    "src/common/config.py",
]

for file_path in placeholder_files:
    file = Path(file_path)
    file.touch(exist_ok=True)

print("Project structure created successfully.")