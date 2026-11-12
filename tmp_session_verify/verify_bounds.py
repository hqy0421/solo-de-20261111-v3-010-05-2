"""Exhaustive out-of-bounds checks for TimeWindowToSessionPartitionMapping."""
from datetime import datetime
from zoneinfo import ZoneInfo

import dagster as dg

NYC = "America/New_York"
nyc = ZoneInfo(NYC)
failures = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def session_with_window(h1, m1, h2, m2, key="2024-03-14"):
    return dg.SessionPartitionsDefinition(
        sessions=[
            dg.TradingSession(
                session_key=key,
                timezone=NYC,
                windows=[
                    dg.TimeWindow(
                        datetime(2024, 3, 14, h1, m1, tzinfo=nyc),
                        datetime(2024, 3, 14, h2, m2, tzinfo=nyc),
                    )
                ],
            )
        ],
        timezone=NYC,
    )


def hourly(sh, sm, eh, em):
    return dg.HourlyPartitionsDefinition(
        start_date=datetime(2024, 3, 14, sh, sm, tzinfo=nyc),
        end_date=datetime(2024, 3, 14, eh, em, tzinfo=nyc),
        timezone=NYC,
        fmt="%Y-%m-%d-%H:%M",
    )


# 1. Prefix missing: trading 09:30-16:00, upstream 11:00-17:00.
cal = session_with_window(9, 30, 16, 0)
up = hourly(11, 0, 17, 0)
res = dg.TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
    cal.subset_with_partition_keys(["2024-03-14"]), cal, up
)
missing = res.required_but_nonexistent_subset.included_time_windows
check("prefix: mapped 11:00 present", "2024-03-14-11:00" in set(res.partitions_subset.get_partition_keys()))
check("prefix: one missing window", len(missing) == 1, str(missing))
check(
    "prefix: missing interval is 09:30-11:00",
    missing
    and missing[0].start == datetime(2024, 3, 14, 9, 30, tzinfo=nyc)
    and missing[0].end == datetime(2024, 3, 14, 11, 0, tzinfo=nyc),
    str(missing),
)

# 2. Suffix missing: trading 09:30-16:00, upstream 08:00-14:00.
up2 = hourly(8, 0, 14, 0)
res2 = dg.TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
    cal.subset_with_partition_keys(["2024-03-14"]), cal, up2
)
missing2 = res2.required_but_nonexistent_subset.included_time_windows
check(
    "suffix: missing interval is 14:00-16:00",
    len(missing2) == 1
    and missing2[0].start == datetime(2024, 3, 14, 14, 0, tzinfo=nyc)
    and missing2[0].end == datetime(2024, 3, 14, 16, 0, tzinfo=nyc),
    str(missing2),
)

# 3. Entirely missing: trading window outside the upstream calendar.
cal3 = session_with_window(9, 30, 12, 0)
up3 = hourly(18, 0, 22, 0)
res3 = dg.TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
    cal3.subset_with_partition_keys(["2024-03-14"]), cal3, up3
)
check("entire: no mapped partitions", res3.partitions_subset.is_empty)
missing3 = res3.required_but_nonexistent_subset.included_time_windows
check(
    "entire: full trading window reported missing",
    len(missing3) == 1
    and missing3[0].start == datetime(2024, 3, 14, 9, 30, tzinfo=nyc)
    and missing3[0].end == datetime(2024, 3, 14, 12, 0, tzinfo=nyc),
    str(missing3),
)

# 4. allow_nonexistent_upstream_partitions suppresses the missing subset.
res4 = dg.TimeWindowToSessionPartitionMapping(
    allow_nonexistent_upstream_partitions=True
).get_upstream_mapped_partitions_result_for_partitions(
    cal3.subset_with_partition_keys(["2024-03-14"]), cal3, up3
)
check("allow-nonexistent suppresses missing", res4.required_but_nonexistent_subset.is_empty)

# 5. Fully covered: no missing windows. Session starts partway through the 09:00
#    partition, so 09:00-10:00 overlaps and must be included despite starting earlier.
cal5 = session_with_window(9, 45, 12, 0)
up5 = hourly(8, 0, 18, 0)
res5 = dg.TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
    cal5.subset_with_partition_keys(["2024-03-14"]), cal5, up5
)
check("covered: no missing windows", res5.required_but_nonexistent_subset.is_empty)
check(
    "covered: open-partition 09:00 and in-window keys mapped",
    set(res5.partitions_subset.get_partition_keys())
    == {"2024-03-14-09:00", "2024-03-14-10:00", "2024-03-14-11:00"},
    str(sorted(res5.partitions_subset.get_partition_keys())),
)

# 6. Multi-window session with one covered and one missing.
cal6 = dg.SessionPartitionsDefinition(
    sessions=[
        dg.TradingSession(
            session_key="2024-03-14",
            timezone=NYC,
            windows=[
                dg.TimeWindow(datetime(2024, 3, 14, 9, 30, tzinfo=nyc),
                              datetime(2024, 3, 14, 12, 0, tzinfo=nyc)),
                dg.TimeWindow(datetime(2024, 3, 14, 13, 0, tzinfo=nyc),
                              datetime(2024, 3, 14, 16, 0, tzinfo=nyc)),
            ],
        )
    ],
    timezone=NYC,
)
up6 = hourly(13, 0, 17, 0)
res6 = dg.TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
    cal6.subset_with_partition_keys(["2024-03-14"]), cal6, up6
)
missing6 = res6.required_but_nonexistent_subset.included_time_windows
check(
    "multi-window: only the morning window missing",
    len(missing6) == 1
    and missing6[0].start == datetime(2024, 3, 14, 9, 30, tzinfo=nyc)
    and missing6[0].end == datetime(2024, 3, 14, 12, 0, tzinfo=nyc),
    str(missing6),
)
check(
    "multi-window: afternoon mapped",
    "2024-03-14-13:00" in set(res6.partitions_subset.get_partition_keys())
    and "2024-03-14-15:00" in set(res6.partitions_subset.get_partition_keys()),
)

print()
print("ALL BOUNDS CHECKS PASSED" if not failures else f"FAILURES: {failures}")
raise SystemExit(1 if failures else 0)
