# NOTE:
# This module is the raw-event generation and landing layer for the Loadstar Analytics project. It imports reusable event builder functions and writes the generated synthetic JSON events into the maintenance raw volume for downstream bronze ingestion.

import json
import random
from datetime import datetime, timedelta, timezone
from src.common.paths import get_volume_path

from src.maintenance.landing.event_builders import (
    create_downtime_event,
    create_failure_event,
    create_repair_event,
)

def create_raw_events(number_of_events, random_seed=42):
    """
    Generate a list of synthetic raw maintenance events.

    Each event is created by randomly selecting one of the supported event builder
    functions (failure, repair, or downtime). The timestamps are anchored from a
    base timestamp set to 7 days before the current UTC time.
    """
    random.seed(random_seed)

    base_timestamp = datetime.now(timezone.utc) - timedelta(days=7)
    raw_events = []

    event_builders = [
        create_failure_event,
        create_repair_event,
        create_downtime_event
    ]

    # Generate synthetic events using random event builders
    for _ in range(number_of_events):
        selected_event_builder = random.choice(event_builders)
        raw_events.append(selected_event_builder(base_timestamp))

    return raw_events

def write_raw_events_to_volume(
    spark,
    number_of_events=250,
    output_folder="events",
    write_mode="append",
    random_seed=42
):
    """
    Generate synthetic raw events and write them to the maintenance raw volume
    as JSON lines text files.

    Parameters:
        spark: active Spark session
        number_of_events: number of events to generate
        output_folder: folder name under the maintenance raw volume
        write_mode: append or overwrite
        random_seed: seed for reproducible synthetic data generation
    """
    output_path = get_volume_path("maintenance", output_folder)

    # Generate synthetic events
    raw_events = create_raw_events(
        number_of_events=number_of_events,
        random_seed=random_seed
    )

    # Convert events to JSON lines format for Spark ingestion
    json_lines = []
    for raw_event in raw_events:
        json_lines.append((json.dumps(raw_event),))

    # Create Spark DataFrame for writing
    raw_events_dataframe = spark.createDataFrame(json_lines, ["value"])

    # Write DataFrame as text files to the specified volume path
    (
        raw_events_dataframe
        .write
        .mode(write_mode)
        .text(output_path)
    )

    print(f"Wrote {number_of_events} raw events to: {output_path}")

# Entry point: generate and write events to volume
write_raw_events_to_volume(
    spark=spark,
    number_of_events=250,
    output_folder="events",
    write_mode="append",
    random_seed=42
)