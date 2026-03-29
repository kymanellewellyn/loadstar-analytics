# Reference data used by the synthetic maintenance event generator.
# These records represent reusable master-style attributes and lookup values that are used
# to construct simulated raw JSON maintenance events.


TRUCKS = [
    {
        "truck_id": "TRK_1001",
        "vin": "1HTMKADN43H561298",
        "make": "Kenworth",
        "model": "T880",
        "year": 2022,
        "capacity_tons": 18.5,
        "home_site_id": "SITE_DAL_01",
        "allowed_site_ids": ["SITE_DAL_01", "SITE_FTW_01"],
        "starting_odometer_miles": 121450.0,
        "starting_engine_hours": 6150.0
    },
    {
        "truck_id": "TRK_1002",
        "vin": "1XKWD49X9NJ487201",
        "make": "Peterbilt",
        "model": "567",
        "year": 2021,
        "capacity_tons": 17.0,
        "home_site_id": "SITE_FTW_01",
        "allowed_site_ids": ["SITE_FTW_01"],
        "starting_odometer_miles": 148220.0,
        "starting_engine_hours": 7425.0
    },
    {
        "truck_id": "TRK_1003",
        "vin": "3AKJHHDR8NSMG1123",
        "make": "Freightliner",
        "model": "122SD",
        "year": 2023,
        "capacity_tons": 19.0,
        "home_site_id": "SITE_DAL_01",
        "allowed_site_ids": ["SITE_DAL_01", "SITE_FTW_01"],
        "starting_odometer_miles": 84210.0,
        "starting_engine_hours": 3825.0
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