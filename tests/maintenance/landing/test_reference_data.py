from src.maintenance.landing.reference_data import TRUCKS, SITES

# Test that the TRUCKS list is not empty
def test_trucks_is_not_empty():
    assert TRUCKS, "TRUCKS should not be empty"

# Test that the SITES list is not empty
def test_sites_is_not_empty():
    assert SITES, "SITES should not be empty"

# Test that each truck dictionary contains all required fields
def test_each_truck_has_required_fields():
    required_fields = {
        "truck_id",
        "vin",
        "make",
        "model",
        "year",
        "capacity_tons",
        "home_site_id",
        "allowed_site_ids",
        "odometer_miles_range",
        "engine_hours_range",
    }

    for truck in TRUCKS:
        assert required_fields.issubset(truck.keys()), (
            f"Truck missing required fields: {truck}"
        )

# Test that each site dictionary contains all required fields
def test_each_site_has_required_fields():
    required_fields = {
        "site_id",
        "site_name",
        "latitude",
        "longitude",
        "city",
        "state",
        "zones",
    }

    for site in SITES:
        assert required_fields.issubset(site.keys()), (
            f"Site missing required fields: {site}"
        )

# Test that each truck's home_site_id exists in the SITES list
def test_truck_home_site_ids_exist_in_sites():
    valid_site_ids = {site["site_id"] for site in SITES}

    for truck in TRUCKS:
        assert truck["home_site_id"] in valid_site_ids, (
            f"Invalid home_site_id for truck {truck['truck_id']}: {truck['home_site_id']}"
        )

# Test that all allowed_site_ids for each truck exist in the SITES list
def test_truck_allowed_site_ids_exist_in_sites():
    valid_site_ids = {site["site_id"] for site in SITES}

    for truck in TRUCKS:
        for site_id in truck["allowed_site_ids"]:
            assert site_id in valid_site_ids, (
                f"Invalid allowed_site_id for truck {truck['truck_id']}: {site_id}"
            )

# Test that each truck's home_site_id is included in its allowed_site_ids
def test_home_site_is_in_allowed_site_ids():
    for truck in TRUCKS:
        assert truck["home_site_id"] in truck["allowed_site_ids"], (
            f"Truck {truck['truck_id']} home_site_id must be in allowed_site_ids"
        )

# Run all tests and print a success message if all pass
def run_all_tests():
    test_trucks_is_not_empty()
    test_sites_is_not_empty()
    test_each_truck_has_required_fields()
    test_each_site_has_required_fields()
    test_truck_home_site_ids_exist_in_sites()
    test_truck_allowed_site_ids_exist_in_sites()
    test_home_site_is_in_allowed_site_ids()
    print("All reference_data tests passed.")

if __name__ == "__main__":
    run_all_tests()