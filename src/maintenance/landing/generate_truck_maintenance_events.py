import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from src.common.paths import get_volume_path


TRUCKS = [
    {
        "truck_id": "TRK_1001",
        "vin": "1HTMKADN43H561298",
        "make": "Kenworth",
        "model": "T880",
        "year": 2022,
        "capacity_tons": 18.5
    },
    {
        "truck_id": "TRK_1002",
        "vin": "1XKWD49X9NJ487201",
        "make": "Peterbilt",
        "model": "567",
        "year": 2021,
        "capacity_tons": 17.0
    },
    {
        "truck_id": "TRK_1003",
        "vin": "3AKJHHDR8NSMG1123",
        "make": "Freightliner",
        "model": "122SD",
        "year": 2023,
        "capacity_tons": 19.0
    }
]

SITES = [
    {
        "site_id": "SITE_DAL_01",
        "site_name": "Dallas Yard",
        "latitude": 32.7767,
        "longitude": -96.7970,
        "city": "Dallas",
        "state": "TX",
        "zones": ["yard_entry", "maintenance_bay", "repair_bay_1", "repair_bay_2"]
    },
    {
        "site_id": "SITE_FTW_01",
        "site_name": "Fort Worth Yard",
        "latitude": 32.7555,
        "longitude": -97.3308,
        "city": "Fort Worth",
        "state": "TX",
        "zones": ["yard_entry", "maintenance_bay", "repair_bay_1", "parts_area"]
    }
]

VENDORS = [
    {"vendor_id": "VEND_001", "vendor_name": "Lone Star Fleet Repair"},
    {"vendor_id": "VEND_002", "vendor_name": "Texas Heavy Duty Service"},
    {"vendor_id": "VEND_003", "vendor_name": "DFW Truck & Hydraulics"}
]

TECHNICIANS = [
    {"technician_id": "TECH_1001", "name": "Miguel Torres", "role": "Lead Technician"},
    {"technician_id": "TECH_1002", "name": "Chris Walker", "role": "Field Technician"},
    {"technician_id": "TECH_1003", "name": "Isaac Reed", "role": "Hydraulics Specialist"},
    {"technician_id": "TECH_1004", "name": "Dana Brooks", "role": "Brake Specialist"}
]

FAILURE_TYPES = [
    {
        "failure_type": "HYDRAULIC",
        "failure_code": "HYD_214",
        "symptoms": ["slow_bed_raise", "pressure_drop", "fluid_leak_detected"],
        "parts_used": [
            {"part_id": "PART_4501", "part_name": "Hydraulic Hose", "unit_cost": 145.75},
            {"part_id": "PART_7750", "part_name": "Hydraulic Seal Kit", "unit_cost": 89.50}
        ]
    },
    {
        "failure_type": "BRAKE",
        "failure_code": "BRK_110",
        "symptoms": ["soft_brake_pedal", "air_pressure_warning"],
        "parts_used": [
            {"part_id": "PART_2201", "part_name": "Brake Chamber", "unit_cost": 210.00},
            {"part_id": "PART_2202", "part_name": "Brake Pad Set", "unit_cost": 132.45}
        ]
    },
    {
        "failure_type": "ENGINE",
        "failure_code": "ENG_502",
        "symptoms": ["overheat_warning", "rough_idle", "power_loss"],
        "parts_used": [
            {"part_id": "PART_3101", "part_name": "Coolant Hose", "unit_cost": 48.25},
            {"part_id": "PART_3102", "part_name": "Thermostat", "unit_cost": 67.10}
        ]
    },
    {
        "failure_type": "TIRE",
        "failure_code": "TIR_305",
        "symptoms": ["low_tire_pressure", "uneven_wear"],
        "parts_used": [
            {"part_id": "PART_8801", "part_name": "Drive Tire", "unit_cost": 525.00}
        ]
    }
]

WEATHER_CONDITIONS = [
    {"condition": "CLEAR", "severity_level": "LOW"},
    {"condition": "WINDY", "severity_level": "MEDIUM"},
    {"condition": "HEAVY_RAIN", "severity_level": "HIGH"},
    {"condition": "FOG", "severity_level": "MEDIUM"}
]


def format_timestamp_as_utc(timestamp_value):
    return timestamp_value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def create_sensor_readings_section(failure_type):
    sensor_readings = {
        "battery_voltage": round(random.uniform(11.8, 14.1), 2),
        "engine_temp_f": round(random.uniform(180.0, 240.0), 1)
    }

    if failure_type == "HYDRAULIC":
        sensor_readings["hydraulic_pressure_psi"] = round(random.uniform(900.0, 2200.0), 1)
    elif failure_type == "BRAKE":
        sensor_readings["brake_line_pressure_psi"] = round(random.uniform(80.0, 140.0), 1)
    elif failure_type == "ENGINE":
        sensor_readings["oil_pressure_psi"] = round(random.uniform(15.0, 80.0), 1)
    elif failure_type == "TIRE":
        sensor_readings["tire_pressure_psi"] = round(random.uniform(50.0, 120.0), 1)

    return sensor_readings


def create_common_sections():
    selected_truck = random.choice(TRUCKS)
    selected_site = random.choice(SITES)

    producer_section = {
        "system": "loadstar_mock_generator",
        "region": "us-central",
        "site_id": selected_site["site_id"],
        "device_id": f"device_{random.randint(1000, 9999)}"
    }

    truck_section = {
        "truck_id": selected_truck["truck_id"],
        "vin": selected_truck["vin"],
        "make": selected_truck["make"],
        "model": selected_truck["model"],
        "year": selected_truck["year"],
        "capacity_tons": selected_truck["capacity_tons"],
        "status": "UNKNOWN",
        "odometer_miles": round(random.uniform(50000, 250000), 1),
        "engine_hours": round(random.uniform(1500, 12000), 1)
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
    event_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    selected_failure = random.choice(FAILURE_TYPES)
    selected_vendor = random.choice(VENDORS)

    producer_section, truck_section, location_section = create_common_sections()
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
    downtime_start_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    repair_end_timestamp = downtime_start_timestamp + timedelta(
        hours=random.randint(1, 8),
        minutes=random.randint(0, 59)
    )

    selected_failure = random.choice(FAILURE_TYPES)
    selected_vendor = random.choice(VENDORS)

    producer_section, truck_section, location_section = create_common_sections()
    truck_section["status"] = "IN_SERVICE"

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
    event_timestamp = base_timestamp + timedelta(minutes=random.randint(0, 10080))
    producer_section, truck_section, location_section = create_common_sections()

    is_planned = random.choice([True, False])
    event_type = random.choice(["DOWNTIME_START", "DOWNTIME_END"])

    if is_planned:
        truck_section["status"] = "SCHEDULED_MAINTENANCE"
    else:
        truck_section["status"] = "OUT_OF_SERVICE"

    downtime_end_timestamp = None
    if event_type == "DOWNTIME_END":
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


def create_raw_events(number_of_events, random_seed=42):
    random.seed(random_seed)

    base_timestamp = datetime.now(timezone.utc) - timedelta(days=7)
    raw_events = []

    for _ in range(number_of_events):
        selected_event_builder = random.choice([
            create_failure_event,
            create_repair_event,
            create_downtime_event
        ])
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

    json_lines = []
    for raw_event in raw_events:
        json_lines.append((json.dumps(raw_event),))

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