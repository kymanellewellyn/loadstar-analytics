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
    random.seed(random_seed)

    base_timestamp = datetime.now(timezone.utc) - timedelta(days=7)
    raw_events = []

    event_builders = [
        create_failure_event,
        create_repair_event,
        create_downtime_event
    ]

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
    output_path = get_volume_path("maintenance", output_folder)

    raw_events = create_raw_events(
        number_of_events=number_of_events,
        random_seed=random_seed
    )

    json_lines = [(json.dumps(raw_event),) for raw_event in raw_events]

    raw_events_dataframe = spark.createDataFrame(json_lines, ["value"])

    (
        raw_events_dataframe
        .write
        .mode(write_mode)
        .text(output_path)
    )

    print(f"Wrote {number_of_events} raw events to: {output_path}")


write_raw_events_to_volume(
    spark=spark,
    number_of_events=250,
    output_folder="events",
    write_mode="append",
    random_seed=42
)