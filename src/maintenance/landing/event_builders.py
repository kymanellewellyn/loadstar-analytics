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
def create_common_sections(producer_system="fleet_telematics_gateway"):
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

    return producer_section, truck_section, location_section


def create_failure_event(base_timestamp):
    # Simulate event timestamp within a week from base_timestamp
    event_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    selected_failure = random.choice(FAILURE_TYPES)
    selected_vendor = random.choice(VENDORS)

    producer_section, truck_section, location_section = create_common_sections(producer_system="fleet_telematics_gateway")
    
    truck_section["status"] = "OUT_OF_SERVICE"

    return {
        "event_id": f"event_{uuid.uuid4().hex}",
        "event_type": "FAILURE",
        "event_version": "1.0",
        "event_timestamp": format_timestamp_as_utc(event_timestamp),
        "producer": producer_section,
        "truck": truck_section,
        "location": location_section,
        "weather": create_weather_section(),
        "failure": {
            "failure_id": f"failure_{uuid.uuid4().hex[:10]}",
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
        "downtime": {
            "downtime_id": f"downtime_{uuid.uuid4().hex[:10]}",
            "start_timestamp": format_timestamp_as_utc(event_timestamp),
            "end_timestamp": None,
            "reason": "UNPLANNED_FAILURE",
            "is_planned": False
        },
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


def create_repair_event(base_timestamp):
    # Simulate downtime start and repair end timestamps
    downtime_start_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    repair_end_timestamp = downtime_start_timestamp + timedelta(
        hours=random.randint(1, 8),
        minutes=random.randint(0, 59)
    )

    selected_failure = random.choice(FAILURE_TYPES)
    selected_vendor = random.choice(VENDORS)

    producer_section, truck_section, location_section = create_common_sections(
    producer_system="maintenance_management_system"
)
    truck_section["status"] = "IN_SERVICE"

    # Select parts used for repair
    selected_parts = random.sample(selected_failure["parts_used"], 1)
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
        "event_timestamp": format_timestamp_as_utc(repair_end_timestamp),
        "producer": producer_section,
        "truck": truck_section,
        "location": location_section,
        "weather": create_weather_section(),
        "failure": {
            "failure_id": f"failure_{uuid.uuid4().hex[:10]}",
            "failure_type": selected_failure["failure_type"],
            "failure_code": selected_failure["failure_code"],
            "severity": random.choice(["MEDIUM", "HIGH", "CRITICAL"]),
            "symptoms": selected_failure["symptoms"],
            "diagnostics": {
                "fault_codes": [selected_failure["failure_code"]],
                "sensor_readings": create_sensor_readings_section(selected_failure["failure_type"])
            }
        },
        "repair": {
            "repair_id": f"repair_{uuid.uuid4().hex[:10]}",
            "repair_status": "COMPLETED",
            "repair_category": f"{selected_failure['failure_type']}_REPAIR",
            "labor_hours": round(random.uniform(1.0, 8.0), 1),
            "completion_timestamp": format_timestamp_as_utc(repair_end_timestamp)
        },
        "downtime": {
            "downtime_id": f"downtime_{uuid.uuid4().hex[:10]}",
            "start_timestamp": format_timestamp_as_utc(downtime_start_timestamp),
            "end_timestamp": format_timestamp_as_utc(repair_end_timestamp),
            "reason": "UNPLANNED_FAILURE",
            "is_planned": False
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
        "tags": ["maintenance", "repair", selected_failure["failure_type"].lower()]
    }


def create_downtime_event(base_timestamp):
    # Simulate downtime event timestamp within a week from base_timestamp
    event_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    producer_section, truck_section, location_section = create_common_sections(
    producer_system="fleet_operations_platform"
)

    is_planned = random.choice([True, False])
    event_type = random.choice(["DOWNTIME_START", "DOWNTIME_END"])

    # Set truck status based on planned/unplanned downtime
    if is_planned:
        truck_section["status"] = "SCHEDULED_MAINTENANCE"
    else:
        truck_section["status"] = "OUT_OF_SERVICE"

    downtime_end_timestamp = None
    if event_type == "DOWNTIME_END":
        # If downtime ends, simulate end timestamp a few hours after start
        downtime_end_timestamp = format_timestamp_as_utc(
            event_timestamp + timedelta(hours=random.randint(1, 4))
        )

    return {
        "event_id": f"event_{uuid.uuid4().hex}",
        "event_type": event_type,
        "event_version": "1.0",
        "event_timestamp": format_timestamp_as_utc(event_timestamp),
        "producer": producer_section,
        "truck": truck_section,
        "location": location_section,
        "weather": create_weather_section(),
        "failure": None,
        "repair": None,
        "downtime": {
            "downtime_id": f"downtime_{uuid.uuid4().hex[:10]}",
            "start_timestamp": format_timestamp_as_utc(event_timestamp),
            "end_timestamp": downtime_end_timestamp,
            "reason": random.choice(["UNPLANNED_FAILURE", "SCHEDULED_SERVICE", "INSPECTION_HOLD"]),
            "is_planned": is_planned
        },
        "service": {
            "vendor_id": None,
            "vendor_name": None,
            "technicians": [],
            "parts_used": []
        },
        "notes": {
            "driver_note": None,
            "dispatcher_note": "Downtime recorded by fleet operations.",
            "maintenance_note": None
        },
        "tags": ["maintenance", "downtime"]
    }