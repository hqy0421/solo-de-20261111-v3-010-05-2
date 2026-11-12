"""Reproduce out-of-bounds edge cases in TimeWindowToSessionPartitionMapping."""
from datetime import datetime
from zoneinfo import ZoneInfo

import dagster as dg

NYC = "America/New_York"
nyc = ZoneInfo(NYC)

# Upstream hourly calendar only covers the afternoon of 2024-03-14.
hourly = dg.HourlyPartitionsDefinition(
    start_date=datetime(2024, 3, 14, 12, 0, tzinfo=nyc),
    end_date=datetime(2024, 3, 15, 0, 0, tzinfo=nyc),
    timezone=NYC,
    fmt="%Y-%m-%d-%H:%M",
)

# Session starts at 09:30 -- before the upstream calendar starts.
session_pd = dg.SessionPartitionsDefinition(
    sessions=[
        dg.TradingSession(
            session_key="2024-03-14",
            timezone=NYC,
            windows=[
                dg.TimeWindow(
                    datetime(2024, 3, 14, 9, 30, tzinfo=nyc),
                    datetime(2024, 3, 14, 12, 0, tzinfo=nyc),
                ),
                dg.TimeWindow(
                    datetime(2024, 3, 14, 13, 0, tzinfo=nyc),
                    datetime(2024, 3, 14, 16, 0, tzinfo=nyc),
                ),
            ],
        )
    ],
    timezone=NYC,
)

mapping = dg.TimeWindowToSessionPartitionMapping()
result = mapping.get_upstream_mapped_partitions_result_for_partitions(
    session_pd.subset_with_partition_keys(["2024-03-14"]),
    session_pd,
    hourly,
)
mapped = sorted(result.partitions_subset.get_partition_keys())
print("mapped keys:", mapped)
print("required_but_nonexistent windows:")
for w in result.required_but_nonexistent_subset.included_time_windows:
    print("  ", w.start, "->", w.end)

first_window = hourly.get_first_partition_window()
print("upstream first window:", first_window.start, "->", first_window.end)

phantom = [k for k in mapped if hourly.time_window_for_partition_key(k).start < first_window.start]
print("phantom pre-start keys:", phantom)

missing_covered = any(
    w.start.timestamp() <= datetime(2024, 3, 14, 9, 30, tzinfo=nyc).timestamp()
    for w in result.required_but_nonexistent_subset.included_time_windows
)
print("missing-subset covers the pre-start trading time:", missing_covered)
