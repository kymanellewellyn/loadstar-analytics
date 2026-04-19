import json
from datetime import datetime, timezone

from src.maintenance.landing.generate_truck_maintenance_events import create_raw_events

# Fixed base timestamp for event generation
BASE_TIMESTAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)

def test_create_raw_events_returns_list():
    # Test that create_raw_events returns a list of the correct length
    raw_events = create_raw_events(5, base_timestamp=BASE_TIMESTAMP)

    assert isinstance(raw_events, list), "create_raw_events should return a list"
    assert len(raw_events) == 5, "create_raw_events should return the requested number of events"

def test_create_raw_events_produces_valid_event_types():
    # Test that all events have valid event types
    raw_events = create_raw_events(25, base_timestamp=BASE_TIMESTAMP)

    valid_event_types = {"FAILURE", "REPAIR", "DOWNTIME_START", "DOWNTIME_END"}

    for event in raw_events:
        assert event["event_type"] in valid_event_types, (
            f"Invalid event_type: {event['event_type']}"
        )

def test_create_raw_events_have_common_top_level_fields():
    # Test that all events contain required top-level fields
    raw_events = create_raw_events(10, base_timestamp=BASE_TIMESTAMP)

    required_fields = {
        "event_id",
        "event_type",
        "event_version",
        "event_timestamp",
        "producer",
        "truck",
        "location",
        "weather",
        "failure",
        "repair",
        "downtime",
        "service",
        "notes",
        "tags",
    }

    for event in raw_events:
        assert required_fields.issubset(event.keys()), (
            f"Event missing required fields: {event}"
        )

def test_create_raw_events_are_json_serializable():
    # Test that all events are JSON serializable
    raw_events = create_raw_events(10, base_timestamp=BASE_TIMESTAMP)

    for event in raw_events:
        serialized = json.dumps(event)
        assert isinstance(serialized, str)
        assert serialized.startswith("{")

def test_create_raw_events_changes_with_different_seed():
    # Test that different random seeds produce different events
    first_run = create_raw_events(
        5,
        random_seed=42,
        base_timestamp=BASE_TIMESTAMP,
    )
    second_run = create_raw_events(
        5,
        random_seed=99,
        base_timestamp=BASE_TIMESTAMP,
    )

    assert first_run != second_run, "Different seeds should produce different events"

def run_all_tests():
    # Run all test functions
    test_create_raw_events_returns_list()
    test_create_raw_events_produces_valid_event_types()
    test_create_raw_events_have_common_top_level_fields()
    test_create_raw_events_are_json_serializable()
    test_create_raw_events_changes_with_different_seed()
    print("All generate_truck_maintenance_events tests passed.")

if __name__ == "__main__":
    run_all_tests()