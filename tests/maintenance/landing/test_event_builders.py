from datetime import datetime, timezone

# Import event builder functions and reference data for testing
from src.maintenance.landing.event_builders import (
    format_timestamp_as_utc,
    create_weather_section,
    create_sensor_readings_section,
    get_site_by_id,
    create_common_sections,
    create_failure_event,
    create_repair_event,
    create_downtime_event,
)
from src.maintenance.landing.reference_data import (
    SITES,
    TRUCKS,
    VENDORS,
    TECHNICIANS,
    FAILURE_TYPES,
    WEATHER_CONDITIONS,
)

# Fixed timestamp used for deterministic test cases
BASE_TIMESTAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)

def test_format_timestamp_as_utc_returns_expected_string_format():
    # Test that UTC formatting returns a string ending with 'Z'
    result = format_timestamp_as_utc(BASE_TIMESTAMP)
    assert isinstance(result, str), "Formatted timestamp should be a string"
    assert result.endswith("Z"), "Formatted timestamp should end with Z"

def test_create_weather_section_has_required_fields():
    # Test that weather section contains all required fields
    weather = create_weather_section()

    required_fields = {
        "condition",
        "temperature_f",
        "humidity_pct",
        "wind_mph",
        "visibility_miles",
        "severity_level",
    }

    assert required_fields.issubset(weather.keys()), (
        f"Weather section missing required fields: {weather}"
    )

def test_create_weather_section_uses_valid_reference_values():
    # Test that weather section uses valid reference values
    weather = create_weather_section()

    valid_conditions = {item["condition"] for item in WEATHER_CONDITIONS}
    valid_severity_levels = {item["severity_level"] for item in WEATHER_CONDITIONS}

    assert weather["condition"] in valid_conditions, (
        f"Invalid weather condition: {weather['condition']}"
    )
    assert weather["severity_level"] in valid_severity_levels, (
        f"Invalid severity level: {weather['severity_level']}"
    )

def test_create_sensor_readings_section_has_base_fields():
    # Test that sensor readings section contains base fields for ENGINE
    sensor_readings = create_sensor_readings_section("ENGINE")

    assert "battery_voltage" in sensor_readings
    assert "engine_temp_f" in sensor_readings

def test_create_sensor_readings_section_adds_hydraulic_field():
    # Test that HYDRAULIC sensor readings include hydraulic pressure
    sensor_readings = create_sensor_readings_section("HYDRAULIC")
    assert "hydraulic_pressure_psi" in sensor_readings

def test_create_sensor_readings_section_adds_brake_field():
    # Test that BRAKE sensor readings include brake line pressure
    sensor_readings = create_sensor_readings_section("BRAKE")
    assert "brake_line_pressure_psi" in sensor_readings

def test_create_sensor_readings_section_adds_engine_field():
    # Test that ENGINE sensor readings include oil pressure
    sensor_readings = create_sensor_readings_section("ENGINE")
    assert "oil_pressure_psi" in sensor_readings

def test_create_sensor_readings_section_adds_tire_field():
    # Test that TIRE sensor readings include tire pressure
    sensor_readings = create_sensor_readings_section("TIRE")
    assert "tire_pressure_psi" in sensor_readings

def test_get_site_by_id_returns_valid_site():
    # Test that get_site_by_id returns correct site info
    site = get_site_by_id("SITE_DAL_01")
    assert site["site_id"] == "SITE_DAL_01"

def test_create_common_sections_have_required_shapes():
    # Test that common sections contain all required fields
    producer, truck, location = create_common_sections()

    producer_fields = {"system", "region", "site_id", "device_id"}
    truck_fields = {
        "truck_id",
        "vin",
        "make",
        "model",
        "year",
        "capacity_tons",
        "home_site_id",
        "status",
        "odometer_miles",
        "engine_hours",
    }
    location_fields = {
        "site_id",
        "site_name",
        "latitude",
        "longitude",
        "geofence_zone",
        "city",
        "state",
    }

    assert producer_fields.issubset(producer.keys()), f"Bad producer section: {producer}"
    assert truck_fields.issubset(truck.keys()), f"Bad truck section: {truck}"
    assert location_fields.issubset(location.keys()), f"Bad location section: {location}"

def test_create_common_sections_use_valid_reference_ids():
    # Test that common sections use valid reference IDs
    producer, truck, location = create_common_sections()

    valid_site_ids = {site["site_id"] for site in SITES}
    valid_truck_ids = {truck_item["truck_id"] for truck_item in TRUCKS}

    assert producer["site_id"] in valid_site_ids
    assert location["site_id"] in valid_site_ids
    assert truck["truck_id"] in valid_truck_ids

def test_create_failure_event_has_expected_structure():
    # Test that failure event has expected structure and values
    event = create_failure_event(BASE_TIMESTAMP)

    assert event["event_type"] == "FAILURE"
    assert event["repair"] is None
    assert event["failure"] is not None
    assert event["downtime"] is not None
    assert event["service"] is not None
    assert event["truck"]["status"] == "OUT_OF_SERVICE"

def test_create_failure_event_uses_valid_reference_values():
    # Test that failure event uses valid reference values
    event = create_failure_event(BASE_TIMESTAMP)

    valid_failure_types = {item["failure_type"] for item in FAILURE_TYPES}
    valid_failure_codes = {item["failure_code"] for item in FAILURE_TYPES}
    valid_vendor_ids = {item["vendor_id"] for item in VENDORS}
    valid_technician_ids = {item["technician_id"] for item in TECHNICIANS}

    assert event["failure"]["failure_type"] in valid_failure_types
    assert event["failure"]["failure_code"] in valid_failure_codes
    assert event["service"]["vendor_id"] in valid_vendor_ids

    for technician in event["service"]["technicians"]:
        assert technician["technician_id"] in valid_technician_ids

def test_create_repair_event_has_expected_structure():
    # Test that repair event has expected structure and values
    event = create_repair_event(BASE_TIMESTAMP)

    assert event["event_type"] == "REPAIR"
    assert event["repair"] is not None
    assert event["failure"] is not None
    assert event["downtime"] is not None
    assert event["service"] is not None
    assert event["truck"]["status"] == "IN_SERVICE"
    assert event["repair"]["repair_status"] == "COMPLETED"

def test_create_repair_event_has_parts_used():
    # Test that repair event includes parts used with required fields
    event = create_repair_event(BASE_TIMESTAMP)

    assert event["service"]["parts_used"], "Repair event should have parts_used"

    for part in event["service"]["parts_used"]:
        required_fields = {"part_id", "part_name", "quantity", "unit_cost"}
        assert required_fields.issubset(part.keys()), f"Bad repair part: {part}"

def test_create_downtime_event_has_expected_structure():
    # Test that downtime event has expected structure and values
    event = create_downtime_event(BASE_TIMESTAMP)

    assert event["event_type"] in {"DOWNTIME_START", "DOWNTIME_END"}
    assert event["failure"] is None
    assert event["repair"] is None
    assert event["service"]["vendor_id"] is None
    assert event["service"]["vendor_name"] is None
    assert event["service"]["technicians"] == []
    assert event["service"]["parts_used"] == []

def test_create_downtime_event_end_timestamp_logic():
    # Test that downtime event end timestamp logic is correct
    event = create_downtime_event(BASE_TIMESTAMP)

    if event["event_type"] == "DOWNTIME_START":
        assert event["downtime"]["end_timestamp"] is None
    else:
        assert event["downtime"]["end_timestamp"] is not None

def run_all_tests():
    # Run all test functions and print success message
    test_format_timestamp_as_utc_returns_expected_string_format()
    test_create_weather_section_has_required_fields()
    test_create_weather_section_uses_valid_reference_values()
    test_create_sensor_readings_section_has_base_fields()
    test_create_sensor_readings_section_adds_hydraulic_field()
    test_create_sensor_readings_section_adds_brake_field()
    test_create_sensor_readings_section_adds_engine_field()
    test_create_sensor_readings_section_adds_tire_field()
    test_get_site_by_id_returns_valid_site()
    test_create_common_sections_have_required_shapes()
    test_create_common_sections_use_valid_reference_ids()
    test_create_failure_event_has_expected_structure()
    test_create_failure_event_uses_valid_reference_values()
    test_create_repair_event_has_expected_structure()
    test_create_repair_event_has_parts_used()
    test_create_downtime_event_has_expected_structure()
    test_create_downtime_event_end_timestamp_logic()
    print("All event_builders tests passed.")

if __name__ == "__main__":
    # Entry point for running all tests
    run_all_tests()