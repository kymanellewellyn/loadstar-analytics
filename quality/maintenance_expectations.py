"""
Centralized data quality expectations for maintenance pipeline.

This module defines expectation rules used across Bronze, Silver, and Gold
layers for the maintenance domain.
"""

# ==============================================================================
# CORE EVENT EXPECTATIONS (maintenance_events_clean)
# ==============================================================================

CORE_EVENT_EXPECTATIONS_DROP = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
    "valid_truck_id": "truck_id IS NOT NULL",
}
"""
Critical expectations that drop records if violated.
These ensure the foundational fields required for all downstream processing.
"""

CORE_EVENT_EXPECTATIONS_WARN = {
    "failure_has_data": "event_type != 'FAILURE' OR failure IS NOT NULL",
    "repair_has_data": "event_type != 'REPAIR' OR repair IS NOT NULL",
}
"""
Non-critical expectations that track violations but don't drop records.
Used to monitor data quality issues without blocking pipeline execution.
"""

# ==============================================================================
# FAILURE EVENT EXPECTATIONS (failure_events)
# ==============================================================================

FAILURE_EXPECTATIONS_DROP = {
    "has_failure_details": "failure_id IS NOT NULL",
}
"""
Critical expectations for failure events that drop records if violated.
Ensures every failure event has a valid failure_id.
"""

FAILURE_EXPECTATIONS_WARN = {
    "has_failure_type": "failure_type IS NOT NULL",
    "has_severity": "severity IS NOT NULL",
}
"""
Non-critical expectations for failure events that track violations.
Monitors that failure events have required business fields.
"""

# ==============================================================================
# REPAIR EVENT EXPECTATIONS (repair_events)
# ==============================================================================

REPAIR_EXPECTATIONS_DROP = {
    "has_repair_details": "repair_id IS NOT NULL",
}
"""
Critical expectations for repair events that drop records if violated.
Ensures every repair event has a valid repair_id.
"""

REPAIR_EXPECTATIONS_WARN = {
    "has_addresses_failure_id": "addresses_failure_id IS NOT NULL",
    "has_repair_timestamps": "repair_start_timestamp IS NOT NULL AND repair_end_timestamp IS NOT NULL",
    "valid_repair_time_order": "repair_end_timestamp >= repair_start_timestamp",
}
"""
Non-critical expectations for repair events that track violations.
Monitors repair-to-failure linkage and temporal consistency.
"""

# ==============================================================================
# GOLD LAYER EXPECTATIONS (fact tables, dimensions)
# ==============================================================================

DOWNTIME_EXPECTATIONS = {
    "valid_total_downtime": "total_downtime_hours >= 0 OR total_downtime_hours IS NULL",
    "valid_repair_duration": "repair_duration_hours >= 0 OR repair_duration_hours IS NULL",
    "valid_diagnostic_wait": "diagnostic_wait_hours >= 0 OR diagnostic_wait_hours IS NULL",
}
"""
Expectations for calculated downtime metrics in Gold layer.
Ensures derived metrics are logically valid (non-negative durations).
"""

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def apply_expectations_drop(dp_decorator_func, expectations_dict):
    """
    Apply expect_or_drop expectations from a dictionary.
    
    Args:
        dp_decorator_func: The function being decorated (pass through)
        expectations_dict: Dict mapping expectation names to SQL constraints
    
    Returns:
        Decorated function with all expect_or_drop rules applied
    
    Example:
        @dp.table(name="my_table")
        def my_table():
            return apply_expectations_drop(my_table, CORE_EVENT_EXPECTATIONS_DROP)
    """
    from pyspark import pipelines as dp
    
    func = dp_decorator_func
    for name, constraint in expectations_dict.items():
        func = dp.expect_or_drop(name, constraint)(func)
    return func


def apply_expectations_warn(dp_decorator_func, expectations_dict):
    """
    Apply expect (warn-only) expectations from a dictionary.
    
    Args:
        dp_decorator_func: The function being decorated (pass through)
        expectations_dict: Dict mapping expectation names to SQL constraints
    
    Returns:
        Decorated function with all expect rules applied
    
    Example:
        @dp.table(name="my_table")
        def my_table():
            return apply_expectations_warn(my_table, CORE_EVENT_EXPECTATIONS_WARN)
    """
    from pyspark import pipelines as dp
    
    func = dp_decorator_func
    for name, constraint in expectations_dict.items():
        func = dp.expect(name, constraint)(func)
    return func
