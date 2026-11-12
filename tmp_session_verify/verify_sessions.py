"""Comprehensive functional verification of session partitions."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import dagster as dg
from dagster import (
    HourlyPartitionsDefinition,
    SessionPartitionMapping,
    SessionPartitionsDefinition,
    TimeWindow,
    TimeWindowToSessionPartitionMapping,
    TradingSession,
)
from dagster._core.definitions.partitions.definition.session import PersistedSessionWindow
from dagster._core.definitions.partitions.snap import (
    PartitionsSnap,
    SessionPartitionsSnap,
)
from dagster._core.definitions.partitions.subset import SessionPartitionsSubset
from dagster._core.definitions.timestamp import TimestampWithTimezone
from dagster._serdes import deserialize_value, serialize_value
from dagster._time import create_datetime

NYC = "America/New_York"
UTC = "UTC"
failures = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


# ----------------------------------------------------------------------
# 1. Basic multi-window session: lunch break
# ----------------------------------------------------------------------
nyc = ZoneInfo(NYC)


def equity_session(day: datetime, early_close: bool = False) -> TradingSession:
    if early_close:
        # early-close day: a single 09:30-13:00 window (no afternoon re-open)
        windows = [
            TimeWindow(
                datetime(day.year, day.month, day.day, 9, 30, tzinfo=nyc),
                datetime(day.year, day.month, day.day, 13, 0, tzinfo=nyc),
            )
        ]
    else:
        # 09:30-12:00 and 13:00-16:00, with a lunch break
        windows = [
            TimeWindow(
                datetime(day.year, day.month, day.day, 9, 30, tzinfo=nyc),
                datetime(day.year, day.month, day.day, 12, 0, tzinfo=nyc),
            ),
            TimeWindow(
                datetime(day.year, day.month, day.day, 13, 0, tzinfo=nyc),
                datetime(day.year, day.month, day.day, 16, 0, tzinfo=nyc),
            ),
        ]
    return TradingSession(
        session_key=day.date().isoformat(),
        timezone=NYC,
        windows=windows,
    )


days = [datetime(2024, 3, d, tzinfo=nyc) for d in (14, 15, 18)]
sessions = [
    equity_session(days[0]),
    equity_session(days[1], early_close=True),
    equity_session(days[2]),
]
cal = SessionPartitionsDefinition(sessions=sessions, timezone=NYC)

check("partition keys are session keys", cal.get_partition_keys() == ["2024-03-14", "2024-03-15", "2024-03-18"])
check("num partitions", cal.get_num_partitions() == 3)
check("first/last key", cal.get_first_partition_key() == "2024-03-14" and cal.get_last_partition_key() == "2024-03-18")
check("has key", cal.has_partition_key("2024-03-15") and not cal.has_partition_key("2024-03-16"))

windows = cal.time_windows_for_partition_key("2024-03-14")
check("two windows per session", len(windows) == 2)
check("lunch break excluded", windows[0].end.hour == 12 and windows[1].start.hour == 13)

# timestamp inside lunch break maps to nothing
lunch = datetime(2024, 3, 14, 12, 30, tzinfo=nyc).timestamp()
check("lunch break timestamp -> no partition", cal.get_partition_key_for_timestamp(lunch) is None)
# timestamp inside morning session
morning = datetime(2024, 3, 14, 10, 0, tzinfo=nyc).timestamp()
check("morning timestamp -> correct session", cal.get_partition_key_for_timestamp(morning) == "2024-03-14")
# weekend maps to nothing
saturday = datetime(2024, 3, 16, 10, 0, tzinfo=nyc).timestamp()
check("non-trading day -> no partition", cal.get_partition_key_for_timestamp(saturday) is None)

# ----------------------------------------------------------------------
# 2. Cross-midnight overnight session
# ----------------------------------------------------------------------


def overnight_session(day: datetime) -> TradingSession:
    # evening 20:00-23:59 then 00:00-02:30 the next calendar day, same business-day key
    return TradingSession(
        session_key=day.date().isoformat(),
        timezone=NYC,
        windows=[
            TimeWindow(
                datetime(day.year, day.month, day.day, 20, 0, tzinfo=nyc),
                datetime(day.year, day.month, day.day, 23, 59, tzinfo=nyc),
            ),
            TimeWindow(
                datetime(day.year, day.month, day.day, 0, 0, tzinfo=nyc) + timedelta(days=1),
                datetime(day.year, day.month, day.day, 2, 30, tzinfo=nyc) + timedelta(days=1),
            ),
        ],
    )


overnight = overnight_session(datetime(2024, 3, 14, tzinfo=nyc))
night_cal = SessionPartitionsDefinition(sessions=[overnight], timezone=NYC)
after_midnight = datetime(2024, 3, 15, 1, 0, tzinfo=nyc).timestamp()
check(
    "cross-midnight window belongs to previous business-day key",
    night_cal.get_partition_key_for_timestamp(after_midnight) == "2024-03-14",
)
check("session end is on next calendar day", overnight.end.day == 15 and overnight.start.day == 14)
check("session windows sorted and non-overlapping", overnight.windows[0].end_timestamp <= overnight.windows[1].start_timestamp)

# ----------------------------------------------------------------------
# 3. DST handling: spring forward (2024-03-10) and fall back (2024-11-03) in NYC
# ----------------------------------------------------------------------
# Windows are expressed in local wall time but stored as absolute UTC timestamps, so a window
# that spans a DST transition has an absolute duration differing from its wall-clock duration.
# NYC spring forward: 02:00 EST -> 03:00 EDT on 2024-03-10 (the hour 02:00-03:00 never occurs).
spring_session = TradingSession(
    session_key="2024-03-10",
    timezone=NYC,
    windows=[
        TimeWindow(
            datetime(2024, 3, 10, 0, 0, tzinfo=nyc),
            datetime(2024, 3, 10, 4, 0, tzinfo=nyc),
        )
    ],
)
# wall duration 4h, but 02:00-03:00 is skipped -> absolute duration 3h
abs_duration_h = (spring_session.end_timestamp - spring_session.start_timestamp) / 3600
check("spring-forward absolute duration is 3h", abs(abs_duration_h - 3.0) < 1e-6, f"got {abs_duration_h}")
check("spring window start offset is -05:00", spring_session.start.utcoffset() == timedelta(hours=-5))
check("spring window end offset is -04:00", spring_session.end.utcoffset() == timedelta(hours=-4))

# NYC fall back: 02:00 EDT -> 01:00 EST on 2024-11-03 (the hour 01:00-02:00 occurs twice).
fall_session = TradingSession(
    session_key="2024-11-03",
    timezone=NYC,
    windows=[
        TimeWindow(
            datetime(2024, 11, 3, 0, 0, tzinfo=nyc),
            datetime(2024, 11, 3, 4, 0, tzinfo=nyc),
        )
    ],
)
# wall duration 4h, plus one repeated hour -> absolute duration 5h
fall_duration_h = (fall_session.end_timestamp - fall_session.start_timestamp) / 3600
check("fall-back absolute duration is 5h", abs(fall_duration_h - 5.0) < 1e-6, f"got {fall_duration_h}")
check("fall window start offset is -04:00 (EDT)", fall_session.start.utcoffset() == timedelta(hours=-4))
check("fall window end offset is -05:00 (EST)", fall_session.end.utcoffset() == timedelta(hours=-5))

# ----------------------------------------------------------------------
# 4. Validation errors
# ----------------------------------------------------------------------

def expect_error(name, fn):
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        check(name, True)
        return
    check(name, False, "expected an error but none raised")


expect_error(
    "overlapping windows within a session rejected",
    lambda: TradingSession(
        session_key="x",
        timezone=UTC,
        windows=[
            TimeWindow(datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 1, 2, tzinfo=timezone.utc)),
            TimeWindow(datetime(2024, 1, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 1, 3, tzinfo=timezone.utc)),
        ],
    ),
)
expect_error(
    "reversed window rejected",
    lambda: TradingSession(
        session_key="x",
        timezone=UTC,
        windows=[TimeWindow(datetime(2024, 1, 1, 2, tzinfo=timezone.utc), datetime(2024, 1, 1, tzinfo=timezone.utc))],
    ),
)
expect_error(
    "naive datetime rejected",
    lambda: TradingSession(
        session_key="x",
        timezone=UTC,
        windows=[TimeWindow(datetime(2024, 1, 1), datetime(2024, 1, 1, 2))],
    ),
)
expect_error(
    "duplicate session keys rejected",
    lambda: SessionPartitionsDefinition(
        timezone=UTC,
        sessions=[
            TradingSession("k", [TimeWindow(datetime(2024, 1, d, tzinfo=timezone.utc), datetime(2024, 1, d, 1, tzinfo=timezone.utc))], UTC)
            for d in (1, 1)
        ],
    ),
)
expect_error(
    "overlapping sessions rejected",
    lambda: SessionPartitionsDefinition(
        timezone=UTC,
        sessions=[
            TradingSession("a", [TimeWindow(datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 1, 3, tzinfo=timezone.utc))], UTC),
            TradingSession("b", [TimeWindow(datetime(2024, 1, 1, 2, tzinfo=timezone.utc), datetime(2024, 1, 1, 4, tzinfo=timezone.utc))], UTC),
        ],
    ),
)
expect_error(
    "timezone mismatch rejected",
    lambda: SessionPartitionsDefinition(
        timezone=UTC,
        sessions=[TradingSession("a", [TimeWindow(datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 1, 3, tzinfo=timezone.utc))], NYC)],
    ),
)

# ----------------------------------------------------------------------
# 5. Subset operations + ranges + serialization
# ----------------------------------------------------------------------
subset = cal.empty_subset()
check("empty subset", subset.is_empty and len(subset) == 0)
subset = subset.with_partition_keys(["2024-03-14", "2024-03-18"])
check("subset contains keys", "2024-03-14" in subset and "2024-03-15" not in subset and len(subset) == 2)
ranges = subset.get_partition_key_ranges(cal)
check("non-adjacent sessions produce two ranges", len(ranges) == 2, str(ranges))

subset2 = subset | cal.subset_with_partition_keys(["2024-03-15"])
check("union", len(subset2) == 3)
check("intersection", len(subset2 & cal.subset_with_partition_keys(["2024-03-15"])) == 1)
check("difference", len(subset2 - subset) == 1)

serialized = subset.serialize()
restored = cal.deserialize_subset(serialized)
check("subset round-trip", restored == subset)
check(
    "can_deserialize accepts matching id",
    cal.can_deserialize_subset(serialized, cal.get_serializable_unique_identifier(), "SessionPartitionsDefinition"),
)
check(
    "can_deserialize rejects foreign id",
    not cal.can_deserialize_subset(serialized, "deadbeef", "SessionPartitionsDefinition"),
)

# ----------------------------------------------------------------------
# 6. Serdes of the definition itself (whitelist round trip) - stable identity
# ----------------------------------------------------------------------
serialized_def = serialize_value(cal)
restored_def = deserialize_value(serialized_def, SessionPartitionsDefinition)
check("definition serdes equality", restored_def == cal)
check("definition serdes preserves keys", restored_def.get_partition_keys() == cal.get_partition_keys())
check(
    "stable unique identifier across serdes",
    restored_def.get_serializable_unique_identifier() == cal.get_serializable_unique_identifier(),
)
check("window bounds preserved across serdes", restored_def.time_windows_for_partition_key("2024-03-14") == cal.time_windows_for_partition_key("2024-03-14"))

# subset serdes through full serdes framework
serialized_subset = serialize_value(subset)
restored_subset = deserialize_value(serialized_subset)
check("subset full serdes", isinstance(restored_subset, SessionPartitionsSubset) and restored_subset == subset)

# ----------------------------------------------------------------------
# 7. Snap round trip (cross-process representation)
# ----------------------------------------------------------------------
snap = PartitionsSnap.from_def(cal)
check("snap type", isinstance(snap, SessionPartitionsSnap))
snap_restored = snap.get_partitions_definition()
check("snap round trip equality", snap_restored == cal)
snap_serialized = serialize_value(snap)
snap_deserialized = deserialize_value(snap_serialized)
check("snap serdes", snap_deserialized.get_partitions_definition() == cal)

# ----------------------------------------------------------------------
# 8. Session-to-session partition mapping
# ----------------------------------------------------------------------
# Build a finer-grained upstream calendar: split 2024-03-14 into morning/afternoon sessions.
upstream_sessions = [
    TradingSession(
        session_key="2024-03-14-am",
        timezone=NYC,
        windows=[TimeWindow(datetime(2024, 3, 14, 9, 30, tzinfo=nyc), datetime(2024, 3, 14, 12, 0, tzinfo=nyc))],
    ),
    TradingSession(
        session_key="2024-03-14-pm",
        timezone=NYC,
        windows=[TimeWindow(datetime(2024, 3, 14, 13, 0, tzinfo=nyc), datetime(2024, 3, 14, 16, 0, tzinfo=nyc))],
    ),
    TradingSession(
        session_key="2024-03-15-am",
        timezone=NYC,
        windows=[TimeWindow(datetime(2024, 3, 15, 9, 30, tzinfo=nyc), datetime(2024, 3, 15, 12, 0, tzinfo=nyc))],
    ),
]
upstream_cal = SessionPartitionsDefinition(sessions=upstream_sessions, timezone=NYC)
mapping = SessionPartitionMapping()
mapping.validate_partition_mapping(upstream_cal, cal)

downstream_subset = cal.subset_with_partition_keys(["2024-03-14"])
result = mapping.get_upstream_mapped_partitions_result_for_partitions(
    downstream_subset, cal, upstream_cal
)
check(
    "session mapping covers actual windows only (am+pm, not lunch)",
    set(result.partitions_subset.get_partition_keys()) == {"2024-03-14-am", "2024-03-14-pm"},
    str(list(result.partitions_subset.get_partition_keys())),
)
check("no missing upstream when fully covered", result.required_but_nonexistent_subset.is_empty)

# reverse direction
back = mapping.get_downstream_partitions_for_partitions(
    upstream_cal.subset_with_partition_keys(["2024-03-14-pm", "2024-03-15-am"]),
    upstream_cal,
    cal,
)
check("reverse mapping", set(back.get_partition_keys()) == {"2024-03-14", "2024-03-15"}, str(list(back.get_partition_keys())))

# missing upstream partition handling
missing_result = SessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
    cal.subset_with_partition_keys(["2024-03-18"]), cal, upstream_cal
)
check("missing upstream reported", set(missing_result.required_but_nonexistent_subset.get_partition_keys()) == {"2024-03-18"})
check("missing upstream excluded from mapped", missing_result.partitions_subset.is_empty)

allowed_result = SessionPartitionMapping(allow_nonexistent_upstream_partitions=True).get_upstream_mapped_partitions_result_for_partitions(
    cal.subset_with_partition_keys(["2024-03-18"]), cal, upstream_cal
)
check("allow_nonexistent suppresses error", allowed_result.required_but_nonexistent_subset.is_empty)

# ----------------------------------------------------------------------
# 9. Time-window (minute) -> session mapping: only trading-time partitions
# ----------------------------------------------------------------------
hourly = HourlyPartitionsDefinition(
    start_date=datetime(2024, 3, 14, 0, 0, tzinfo=nyc),
    timezone=NYC,
    fmt="%Y-%m-%d-%H:%M",
)
tw_mapping = TimeWindowToSessionPartitionMapping()
tw_mapping.validate_partition_mapping(hourly, cal)

tw_result = tw_mapping.get_upstream_mapped_partitions_result_for_partitions(
    cal.subset_with_partition_keys(["2024-03-14"]), cal, hourly
)
mapped_hours = set(tw_result.partitions_subset.get_partition_keys())
# Overlap semantics: the 09:00 hourly partition [09:00,10:00) overlaps the 09:30 session start,
# so it is included. 12:00 [12:00,13:00) is the lunch break and must be excluded.
expected_hours = {
    "2024-03-14-09:00",
    "2024-03-14-10:00",
    "2024-03-14-11:00",
    "2024-03-14-13:00",
    "2024-03-14-14:00",
    "2024-03-14-15:00",
}
check("hourly mapping excludes lunch-break partitions", mapped_hours == expected_hours, str(sorted(mapped_hours)))
check("12:00 lunch partition absent", "2024-03-14-12:00" not in mapped_hours)

# reverse: hourly materializations map back to the session
reverse_tw = tw_mapping.get_downstream_partitions_for_partitions(
    tw_result.partitions_subset, hourly, cal
)
check("hourly->session reverse", set(reverse_tw.get_partition_keys()) == {"2024-03-14"})

# ----------------------------------------------------------------------
# 10. Existing time-window partitions behavior is untouched
# ----------------------------------------------------------------------
daily = dg.DailyPartitionsDefinition(start_date="2024-03-01", timezone=NYC)
check("daily partitions still work", daily.get_partition_keys(current_time=datetime(2024, 3, 5, tzinfo=nyc))[0] == "2024-03-01")
from dagster._core.definitions.partitions.utils.mapping import infer_partition_mapping

inferred = infer_partition_mapping(None, cal, upstream_cal)
check("infer session->session mapping", isinstance(inferred, SessionPartitionMapping))
inferred_tw = infer_partition_mapping(None, cal, hourly)
check("infer session->timewindow mapping", isinstance(inferred_tw, TimeWindowToSessionPartitionMapping))
inferred_daily = infer_partition_mapping(None, daily, dg.DailyPartitionsDefinition(start_date="2024-03-01", timezone=NYC))
check("infer daily->daily still TimeWindowPartitionMapping", isinstance(inferred_daily, dg.TimeWindowPartitionMapping))

# ----------------------------------------------------------------------
# 11. Overnight mapping across midnight
# ----------------------------------------------------------------------
# upstream hourly calendar spanning the overnight session
hourly_overnight = HourlyPartitionsDefinition(
    start_date=datetime(2024, 3, 14, 18, 0, tzinfo=nyc),
    end_date=datetime(2024, 3, 15, 4, 0, tzinfo=nyc),
    timezone=NYC,
    fmt="%Y-%m-%d-%H:%M",
)
night_result = TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
    night_cal.subset_with_partition_keys(["2024-03-14"]), night_cal, hourly_overnight
)
night_hours = set(night_result.partitions_subset.get_partition_keys())
check("overnight mapping includes post-midnight hours", "2024-03-15-01:00" in night_hours, str(sorted(night_hours)))
check("overnight mapping includes evening hours", "2024-03-14-20:00" in night_hours)
# gap 23:59->00:00 is only one minute; hour 23:00 partition starts at 23:00 and overlaps until 23:59
check("overnight mapping excludes pre-session 18:00", "2024-03-14-18:00" not in night_hours)

print()
if failures:
    print(f"{len(failures)} FAILURE(S): {failures}")
    raise SystemExit(1)
print("ALL CHECKS PASSED")
