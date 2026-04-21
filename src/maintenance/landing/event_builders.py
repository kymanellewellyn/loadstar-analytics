# NOTE:
# This module generates synthetic maintenance event payloads. The goal is to simulate realistic event-time JSON records that a fleet, telematics, or maintenance system might emit.

# Attributes such as truck id and site details come from reference_data.py
# Event-state attributes such as weather, sensor readings, truck status, odometer miles, and engine hours are synthetically generated at event creation time to mimic operational event payloads.


import random
import uuid
from datetime import timedelta, timezone

from src.maintenance.landing.reference_data import (
    FAILURE_TYPES,
    SITES,
    TECHNICIANS,
    TRUCKS,
    VENDORS,
    WEATHER_CONDITIONS,
)


def format_timestamp_as_utc(timestamp_value):
    # Converts a datetime object to UTC ISO 8601 string format
    return timestamp_value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")



# Generates weather section for the event payload.
# Weather condition categories come from reference data, while numeric measurements are simulated.
def create_weather_section():
    selected_weather = random.choice(WEATHER_CONDITIONS)

    return {
        "condition": selected_weather["condition"],
        "temperature_f": round(random.uniform(45.0, 102.0), 1),
        "humidity_pct": round(random.uniform(25.0, 98.0), 1),
        "wind_mph": round(random.uniform(0.0, 30.0), 1),
        "visibility_miles": round(random.uniform(0.5, 10.0), 1),
        "severity_level": selected_weather["severity_level"]
    }



# Generates sensor readings associated with the selected failure type.
# They are event-time simulated readings used to mimic what a diagnostic or telematics payload might contain.
def create_sensor_readings_section(failure_type):
    sensor_readings = {
        "battery_voltage": round(random.uniform(11.8, 14.1), 2),
        "engine_temp_f": round(random.uniform(180.0, 240.0), 1)
    }

    # Add failure-type specific sensor readings
    if failure_type == "HYDRAULIC":
        sensor_readings["hydraulic_pressure_psi"] = round(random.uniform(900.0, 2200.0), 1)
    elif failure_type == "BRAKE":
        sensor_readings["brake_line_pressure_psi"] = round(random.uniform(80.0, 140.0), 1)
    elif failure_type == "ENGINE":
        sensor_readings["oil_pressure_psi"] = round(random.uniform(15.0, 80.0), 1)
    elif failure_type == "TIRE":
        sensor_readings["tire_pressure_psi"] = round(random.uniform(50.0, 120.0), 1)

    return sensor_readings



def get_site_by_id(site_id):
    # Looks up a site in SITES reference data by site_id
    for site in SITES:
        if site["site_id"] == site_id:
            return site
    raise ValueError(f"Site not found for site_id={site_id}")




# Builds the producer, truck, and location sections shared across multiple event types.
# Important:
# - identity fields (truck_id, vin, make, model, site_id, site_name) are pulled from reference data
# - operational state fields (status, odometer_miles, engine_hours) are synthetic event-time values
# - these values are generated to make the raw JSON payloads look realistic for downstream ingestion
def create_common_sections(producer_system="fleet_telematics_gateway", selected_truck=None):
    if selected_truck is None:
        selected_truck = random.choice(TRUCKS)

    # Select a site from the truck's allowed sites
    selected_site_id = random.choice(selected_truck["allowed_site_ids"])
    selected_site = get_site_by_id(selected_site_id)

    producer_section = {
        "system": producer_system,
        "region": "us-central",
        "site_id": selected_site["site_id"],
        "device_id": f"{selected_truck['truck_id']}_device"
    }

    truck_section = {
        "truck_id": selected_truck["truck_id"],
        "vin": selected_truck["vin"],
        "make": selected_truck["make"],
        "model": selected_truck["model"],
        "year": selected_truck["year"],
        "capacity_tons": selected_truck["capacity_tons"],
        "home_site_id": selected_truck["home_site_id"],
        "status": "UNKNOWN",
        # Generate odometer and engine hours within truck's range
        "odometer_miles": round(
            random.uniform(
                selected_truck["odometer_miles_range"][0],
                selected_truck["odometer_miles_range"][1]
            ),
            1
        ),
        "engine_hours": round(
            random.uniform(
                selected_truck["engine_hours_range"][0],
                selected_truck["engine_hours_range"][1]
            ),
            1
        )
    }

    location_section = {
        "site_id": selected_site["site_id"],
        "site_name": selected_site["site_name"],
        "latitude": selected_site["latitude"],
        "longitude": selected_site["longitude"],
        "geofence_zone": random.choice(selected_site["zones"]),
        "city": selected_site["city"],
        "state": selected_site["state"]
    }

    return producer_section, truck_section, location_section, selected_truck


def create_failure_event(base_timestamp, failure_id=None):
    """
    Generate a FAILURE event with metadata for downstream repair generation.
    
    Args:
        base_timestamp: Base time anchor for event generation
        failure_id: Optional pre-generated failure_id (for linkage)
    
    Returns:
        tuple: (failure_event_dict, failure_metadata_dict)
            - failure_event_dict: The complete JSON event payload
            - failure_metadata_dict: Metadata needed to generate linked repair
    """
    # Simulate event timestamp within a week from base_timestamp
    event_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    
    # Generate failure_id if not provided
    if failure_id is None:
        failure_id = f"failure_{uuid.uuid4().hex[:10]}"
    
    selected_failure = random.choice(FAILURE_TYPES)
    selected_vendor = random.choice(VENDORS)

    producer_section, truck_section, location_section, selected_truck = create_common_sections(
        producer_system="fleet_telematics_gateway"
    )
    
    truck_section["status"] = "OUT_OF_SERVICE"

    failure_event = {
        "event_id": f"event_{uuid.uuid4().hex}",
        "event_type": "FAILURE",
        "event_version": "1.0",
        "event_timestamp": format_timestamp_as_utc(event_timestamp),
        "producer": producer_section,
        "truck": truck_section,
        "location": location_section,
        "weather": create_weather_section(),
        "failure": {
            "failure_id": failure_id,
            "failure_timestamp": format_timestamp_as_utc(event_timestamp),  # NEW: Point in time when failure occurred
            "failure_type": selected_failure["failure_type"],
            "failure_code": selected_failure["failure_code"],
            "severity": random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
            "symptoms": selected_failure["symptoms"],
            "diagnostics": {
                "fault_codes": [
                    selected_failure["failure_code"],
                    f"AUX_{random.randint(100, 999)}"
                ],
                "sensor_readings": create_sensor_readings_section(selected_failure["failure_type"])
            }
        },
        "repair": None,
        # REMOVED: downtime section - will be derived in gold layer
        "service": {
            "vendor_id": selected_vendor["vendor_id"],
            "vendor_name": selected_vendor["vendor_name"],
            # Randomly assign 1 or 2 technicians
            "technicians": random.sample(TECHNICIANS, random.randint(1, 2)),
            "parts_used": []
        },
        "notes": {
            "driver_note": "Truck issue detected during haul cycle.",
            "dispatcher_note": "Unit removed from dispatch schedule.",
            "maintenance_note": "Initial inspection pending."
        },
        "tags": ["maintenance", "failure", selected_failure["failure_type"].lower()]
    }
    
    # Return metadata needed to generate linked repair event
    failure_metadata = {
        "failure_id": failure_id,
        "failure_timestamp": event_timestamp,
        "failure_type": selected_failure["failure_type"],
        "failure_code": selected_failure["failure_code"],
        "truck": selected_truck,
        "selected_failure_ref": selected_failure,  # Keep reference for parts selection
    }
    
    return failure_event, failure_metadata


def create_repair_event_for_failure(failure_metadata, base_timestamp):
    """
    Generate a REPAIR event that addresses a specific FAILURE.
    
    Args:
        failure_metadata: Dict containing failure_id, failure_timestamp, failure_type, truck details
        base_timestamp: Base time anchor (for reference)
    
    Returns:
        repair_event_dict: The complete JSON event payload
    """
    failure_timestamp = failure_metadata["failure_timestamp"]
    failure_id = failure_metadata["failure_id"]
    failure_type = failure_metadata["failure_type"]
    selected_truck = failure_metadata["truck"]
    selected_failure_ref = failure_metadata["selected_failure_ref"]
    
    # Repair starts 1-24 hours after failure (wait time for diagnosis, parts, mechanic)
    repair_start_timestamp = failure_timestamp + timedelta(
        hours=random.randint(1, 24),
        minutes=random.randint(0, 59)
    )
    
    # Repair duration: 1-8 hours
    labor_hours = round(random.uniform(1.0, 8.0), 1)
    repair_end_timestamp = repair_start_timestamp + timedelta(hours=labor_hours)
    
    selected_vendor = random.choice(VENDORS)

    producer_section, truck_section, location_section, _ = create_common_sections(
        producer_system="maintenance_management_system",
        selected_truck=selected_truck  # Use same truck as failure
    )
    truck_section["status"] = "IN_SERVICE"

    # Select parts used for repair
    selected_parts = random.sample(selected_failure_ref["parts_used"], min(1, len(selected_failure_ref["parts_used"])))
    parts_used = []

    for selected_part in selected_parts:
        parts_used.append({
            "part_id": selected_part["part_id"],
            "part_name": selected_part["part_name"],
            "quantity": random.randint(1, 3),
            "unit_cost": selected_part["unit_cost"]
        })

    return {
        "event_id": f"event_{uuid.uuid4().hex}",
        "event_type": "REPAIR",
        "event_version": "1.0",
        "event_timestamp": format_timestamp_as_utc(repair_end_timestamp),  # Event time = when repair completed
        "producer": producer_section,
        "truck": truck_section,
        "location": location_section,
        "weather": create_weather_section(),
        "failure": None,  # No failure section in repair events
        "repair": {
            "repair_id": f"repair_{uuid.uuid4().hex[:10]}",
            "addresses_failure_id": failure_id,  # NEW: Links to actual failure event
            "repair_status": "COMPLETED",
            "repair_category": f"{failure_type}_REPAIR",
            "labor_hours": labor_hours,
            "repair_start_timestamp": format_timestamp_as_utc(repair_start_timestamp),  # NEW: When repair work began
            "repair_end_timestamp": format_timestamp_as_utc(repair_end_timestamp),      # NEW: When repair completed
        },
        "service": {
            "vendor_id": selected_vendor["vendor_id"],
            "vendor_name": selected_vendor["vendor_name"],
            # Randomly assign 1-3 technicians
            "technicians": random.sample(TECHNICIANS, random.randint(1, 3)),
            "parts_used": parts_used
        },
        "notes": {
            "driver_note": None,
            "dispatcher_note": None,
            "maintenance_note": "Repair completed and truck returned to service."
        },
        "tags": ["maintenance", "repair", failure_type.lower()]
    }


# REMOVED: create_downtime_event() - downtime will be derived in gold layer from failure → repair linkage
