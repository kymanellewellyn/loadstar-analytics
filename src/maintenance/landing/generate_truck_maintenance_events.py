# NOTE:
# This module is the raw-event generation and landing layer for the Loadstar Analytics project. It imports reusable event builder functions and writes the generated synthetic JSON events into the maintenance raw volume for downstream bronze ingestion.

import json
import random
from datetime import datetime, timedelta, timezone
from src.common.paths import get_volume_path

from src.maintenance.landing.event_builders import (
    create_failure_event,
    create_repair_event_for_failure,
)

def create_raw_events(number_of_events, random_seed=42, base_timestamp=None):
    """
    Generate a list of synthetic raw maintenance events with proper failure-repair linkage.
    
    Strategy:
    1. Generate failures first (~60% of total events)
    2. Generate repairs for 80% of failures (linked via addresses_failure_id)
    3. Sort all events chronologically by timestamp
    
    This ensures:
    - Repairs always reference actual failure events
    - Repairs occur temporally after their corresponding failures
    - 20% of failures remain unrepaired (realistic scenario)
    
    Parameters:
        number_of_events: Total number of events to generate
        random_seed: Seed for reproducible random event selection
        base_timestamp: Optional fixed timestamp for event generation (defaults to now - 7 days)
    """
    random.seed(random_seed)

    if base_timestamp is None:
        base_timestamp = datetime.now(timezone.utc) - timedelta(days=7)
    
    # Calculate event distribution
    # 60% failures, 40% repairs (which is 80% of failures getting repaired)
    num_failures = int(number_of_events * 0.60)
    num_repairs = int(num_failures * 0.80)  # 80% of failures get repaired
    
    all_events = []
    failure_metadata_list = []
    
    print(f"Generating {num_failures} failure events...")
    
    # Step 1: Generate FAILURE events
    for i in range(num_failures):
        failure_event, failure_metadata = create_failure_event(base_timestamp)
        all_events.append(failure_event)
        failure_metadata_list.append(failure_metadata)
        
        if (i + 1) % 50 == 0:
            print(f"  Generated {i + 1}/{num_failures} failures...")
    
    print(f"✅ Generated {num_failures} failure events")
    print(f"Generating {num_repairs} repair events (80% of failures will be repaired)...")
    
    # Step 2: Generate REPAIR events for random subset of failures
    failures_to_repair = random.sample(failure_metadata_list, num_repairs)
    
    for i, failure_metadata in enumerate(failures_to_repair):
        repair_event = create_repair_event_for_failure(failure_metadata, base_timestamp)
        all_events.append(repair_event)
        
        if (i + 1) % 50 == 0:
            print(f"  Generated {i + 1}/{num_repairs} repairs...")
    
    print(f"✅ Generated {num_repairs} repair events")
    print(f"Note: {num_failures - num_repairs} failures remain unrepaired (realistic scenario)")
    
    # Step 3: Sort all events by timestamp (chronological order)
    print("Sorting events chronologically...")
    all_events.sort(key=lambda e: e["event_timestamp"])
    
    print(f"\n✅ Generated {len(all_events)} total events:")
    print(f"  - {num_failures} FAILURE events")
    print(f"  - {num_repairs} REPAIR events")
    print(f"  - {num_failures - num_repairs} unrepaired failures")
    
    return all_events

def write_raw_events_to_volume(
    number_of_events=250,
    output_folder="events",
    write_mode="append",
    random_seed=42
):
    """
    Generate synthetic raw events and write them to the maintenance raw volume
    as JSON lines text files.

    Parameters:
        number_of_events: number of events to generate
        output_folder: folder name under the maintenance raw volume
        write_mode: append or overwrite
        random_seed: seed for reproducible synthetic data generation
    """
    output_path = get_volume_path("maintenance", output_folder)

    print(f"Generating {number_of_events} synthetic raw events with random_seed={random_seed}...")
    # Generate synthetic events
    raw_events = create_raw_events(
        number_of_events=number_of_events,
        random_seed=random_seed
    )
    print(f"✅ Generated {len(raw_events)} events.")

    # Convert events to JSON lines format for Spark ingestion
    json_lines = []
    for raw_event in raw_events:
        json_lines.append((json.dumps(raw_event),))

    # Create Spark DataFrame for writing
    raw_events_dataframe = spark.createDataFrame(json_lines, ["value"])
    print(f"Writing events to volume path: {output_path} with mode: {write_mode}")

    # Write DataFrame as text files to the specified volume path
    (
        raw_events_dataframe
        .write
        .mode(write_mode)
        .text(output_path)
    )

    print(f"✅ Wrote {number_of_events} raw events to: {output_path}")

# Entry point: generate and write events to volume
if __name__ == "__main__":
    write_raw_events_to_volume(
        number_of_events=250,
        output_folder="events",
        write_mode="append",
        random_seed=42
    )
