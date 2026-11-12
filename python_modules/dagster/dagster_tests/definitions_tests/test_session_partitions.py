import pickle
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import dagster as dg
from dagster import (
    HourlyPartitionsDefinition,
    PartitionKeyRange,
    SessionPartitionMapping,
    SessionPartitionsDefinition,
    TimeWindow,
    TimeWindowToSessionPartitionMapping,
    TradingSession,
)
from dagster._core.definitions.partitions.definition.session import (
    PersistedSessionWindow,
)
from dagster._core.definitions.partitions.snap import (
    PartitionsSnap,
    SessionPartitionsSnap,
)
from dagster._core.definitions.partitions.subset import SessionPartitionsSubset
from dagster._core.definitions.partitions.utils.mapping import (
    infer_partition_mapping,
)
from dagster._core.definitions.timestamp import TimestampWithTimezone
from dagster._core.errors import DagsterInvalidDefinitionError
from dagster._serdes import deserialize_value, serialize_value

NYC = "America/New_York"
UTC = "UTC"
nyc = ZoneInfo(NYC)
utc = ZoneInfo(UTC)


def _window(day: datetime, start_h: int, start_m: int, end_h: int, end_m: int) -> TimeWindow:
    return TimeWindow(
        datetime(day.year, day.month, day.day, start_h, start_m, tzinfo=nyc),
        datetime(day.year, day.month, day.day, end_h, end_m, tzinfo=nyc),
    )


def _equity_session(day: datetime, early_close: bool = False) -> TradingSession:
    if early_close:
        windows = [_window(day, 9, 30, 13, 0)]
    else:
        windows = [_window(day, 9, 30, 12, 0), _window(day, 13, 0, 16, 0)]
    return TradingSession(
        session_key=day.date().isoformat(), timezone=NYC, windows=windows
    )


def _calendar() -> SessionPartitionsDefinition:
    days = [datetime(2024, 3, d, tzinfo=nyc) for d in (14, 15, 18)]
    return SessionPartitionsDefinition(
        sessions=[
            _equity_session(days[0]),
            _equity_session(days[1], early_close=True),
            _equity_session(days[2]),
        ],
        timezone=NYC,
    )


# ----------------------------------------------------------------------
# Construction / validation
# ----------------------------------------------------------------------


def test_session_partition_keys_and_windows():
    cal = _calendar()
    assert cal.get_partition_keys() == ["2024-03-14", "2024-03-15", "2024-03-18"]
    assert cal.get_num_partitions() == 3
    assert cal.get_first_partition_key() == "2024-03-14"
    assert cal.get_last_partition_key() == "2024-03-18"
    assert cal.has_partition_key("2024-03-15")
    assert not cal.has_partition_key("2024-03-16")

    windows = cal.time_windows_for_partition_key("2024-03-14")
    assert len(windows) == 2
    # lunch break is a gap, not part of the session
    assert windows[0].end < windows[1].start
    assert (windows[1].start - windows[0].end) == timedelta(hours=1)


def test_rejects_overlapping_windows_within_session():
    with pytest.raises(DagsterInvalidDefinitionError):
        TradingSession(
            session_key="x",
            timezone=UTC,
            windows=[
                TimeWindow(
                    datetime(2024, 1, 1, tzinfo=utc), datetime(2024, 1, 1, 2, tzinfo=utc)
                ),
                TimeWindow(
                    datetime(2024, 1, 1, 1, tzinfo=utc), datetime(2024, 1, 1, 3, tzinfo=utc)
                ),
            ],
        )


def test_rejects_reversed_and_naive_windows():
    with pytest.raises(DagsterInvalidDefinitionError):
        TradingSession(
            session_key="x",
            timezone=UTC,
            windows=[TimeWindow(datetime(2024, 1, 1, 2), datetime(2024, 1, 1))],
        )
    with pytest.raises(DagsterInvalidDefinitionError):
        TradingSession(
            session_key="x",
            timezone=UTC,
            windows=[TimeWindow(datetime(2024, 1, 1), datetime(2024, 1, 1, 2))],
        )


def test_rejects_duplicate_keys_and_overlapping_sessions():
    session = TradingSession(
        session_key="k",
        timezone=UTC,
        windows=[
            TimeWindow(
                datetime(2024, 1, 1, tzinfo=utc), datetime(2024, 1, 1, 1, tzinfo=utc)
            )
        ],
    )
    with pytest.raises(DagsterInvalidDefinitionError):
        SessionPartitionsDefinition(sessions=[session, session], timezone=UTC)

    with pytest.raises(DagsterInvalidDefinitionError):
        SessionPartitionsDefinition(
            timezone=UTC,
            sessions=[
                TradingSession(
                    "a",
                    [
                        TimeWindow(
                            datetime(2024, 1, 1, tzinfo=utc),
                            datetime(2024, 1, 1, 3, tzinfo=utc),
                        )
                    ],
                    UTC,
                ),
                TradingSession(
                    "b",
                    [
                        TimeWindow(
                            datetime(2024, 1, 1, 2, tzinfo=utc),
                            datetime(2024, 1, 1, 4, tzinfo=utc),
                        )
                    ],
                    UTC,
                ),
            ],
        )


def test_rejects_timezone_mismatch():
    with pytest.raises(DagsterInvalidDefinitionError):
        SessionPartitionsDefinition(
            timezone=UTC,
            sessions=[
                TradingSession(
                    "a",
                    [
                        TimeWindow(
                            datetime(2024, 1, 1, tzinfo=utc),
                            datetime(2024, 1, 1, 3, tzinfo=utc),
                        )
                    ],
                    NYC,
                )
            ],
        )


# ----------------------------------------------------------------------
# Timestamp lookup respects actual trading windows
# ----------------------------------------------------------------------


def test_timestamp_lookup_excludes_breaks_and_non_trading_time():
    cal = _calendar()

    morning = datetime(2024, 3, 14, 10, 0, tzinfo=nyc).timestamp()
    assert cal.get_partition_key_for_timestamp(morning) == "2024-03-14"

    lunch = datetime(2024, 3, 14, 12, 30, tzinfo=nyc).timestamp()
    assert cal.get_partition_key_for_timestamp(lunch) is None

    weekend = datetime(2024, 3, 16, 10, 0, tzinfo=nyc).timestamp()
    assert cal.get_partition_key_for_timestamp(weekend) is None

    # window end is exclusive
    end = datetime(2024, 3, 14, 12, 0, tzinfo=nyc).timestamp()
    assert cal.get_partition_key_for_timestamp(end) is None
    assert cal.get_partition_key_for_timestamp(end, end_closed=True) == "2024-03-14"


def test_cross_midnight_overnight_session():
    day = datetime(2024, 3, 14, tzinfo=nyc)
    overnight = TradingSession(
        session_key="2024-03-14",
        timezone=NYC,
        windows=[
            TimeWindow(
                datetime(2024, 3, 14, 20, 0, tzinfo=nyc),
                datetime(2024, 3, 14, 23, 59, tzinfo=nyc),
            ),
            TimeWindow(
                datetime(2024, 3, 15, 0, 0, tzinfo=nyc),
                datetime(2024, 3, 15, 2, 30, tzinfo=nyc),
            ),
        ],
    )
    cal = SessionPartitionsDefinition(sessions=[overnight], timezone=NYC)

    assert overnight.start.day == 14
    assert overnight.end.day == 15
    after_midnight = datetime(2024, 3, 15, 1, 0, tzinfo=nyc).timestamp()
    assert cal.get_partition_key_for_timestamp(after_midnight) == "2024-03-14"


def test_dst_transition_durations_are_absolute():
    # Spring forward 2024-03-10: 02:00 EST -> 03:00 EDT, the 02:00 hour does not occur.
    spring = TradingSession(
        session_key="2024-03-10",
        timezone=NYC,
        windows=[
            TimeWindow(
                datetime(2024, 3, 10, 0, 0, tzinfo=nyc),
                datetime(2024, 3, 10, 4, 0, tzinfo=nyc),
            )
        ],
    )
    assert (spring.end_timestamp - spring.start_timestamp) == 3 * 3600
    assert spring.start.utcoffset() == timedelta(hours=-5)
    assert spring.end.utcoffset() == timedelta(hours=-4)

    # Fall back 2024-11-03: 02:00 EDT -> 01:00 EST, the 01:00 hour repeats.
    fall = TradingSession(
        session_key="2024-11-03",
        timezone=NYC,
        windows=[
            TimeWindow(
                datetime(2024, 11, 3, 0, 0, tzinfo=nyc),
                datetime(2024, 11, 3, 4, 0, tzinfo=nyc),
            )
        ],
    )
    assert (fall.end_timestamp - fall.start_timestamp) == 5 * 3600
    assert fall.start.utcoffset() == timedelta(hours=-4)
    assert fall.end.utcoffset() == timedelta(hours=-5)


# ----------------------------------------------------------------------
# Subset
# ----------------------------------------------------------------------


def test_subset_operations_and_ranges():
    cal = _calendar()
    subset = cal.empty_subset()
    assert isinstance(subset, SessionPartitionsSubset)
    assert subset.is_empty

    subset = subset.with_partition_keys(["2024-03-14", "2024-03-18"])
    assert "2024-03-14" in subset
    assert "2024-03-15" not in subset
    assert len(subset) == 2

    ranges = subset.get_partition_key_ranges(cal)
    assert ranges == [
        PartitionKeyRange("2024-03-14", "2024-03-14"),
        PartitionKeyRange("2024-03-18", "2024-03-18"),
    ]

    full = subset | cal.subset_with_partition_keys(["2024-03-15"])
    assert len(full) == 3
    assert len(full & cal.subset_with_partition_keys(["2024-03-15"])) == 1
    assert len(full - subset) == 1


def test_subset_serialization_round_trip():
    cal = _calendar()
    subset = cal.subset_with_partition_keys(["2024-03-14", "2024-03-15"])

    restored = cal.deserialize_subset(subset.serialize())
    assert restored == subset

    assert cal.can_deserialize_subset(
        subset.serialize(),
        cal.get_serializable_unique_identifier(),
        "SessionPartitionsDefinition",
    )
    assert not cal.can_deserialize_subset(
        subset.serialize(), "foreign-id", "SessionPartitionsDefinition"
    )

    # full framework serdes
    full = deserialize_value(serialize_value(subset))
    assert full == subset


def test_partition_key_range_resolution():
    cal = _calendar()
    keys = cal.get_partition_keys_in_range(
        PartitionKeyRange("2024-03-14", "2024-03-18")
    )
    assert list(keys) == ["2024-03-14", "2024-03-15", "2024-03-18"]


# ----------------------------------------------------------------------
# Definition / snap serdes and stable identity
# ----------------------------------------------------------------------


def test_definition_serdes_preserves_identity_and_windows():
    cal = _calendar()
    restored = deserialize_value(serialize_value(cal), SessionPartitionsDefinition)
    assert restored == cal
    assert restored.get_partition_keys() == cal.get_partition_keys()
    assert (
        restored.get_serializable_unique_identifier()
        == cal.get_serializable_unique_identifier()
    )
    assert (
        restored.time_windows_for_partition_key("2024-03-14")
        == cal.time_windows_for_partition_key("2024-03-14")
    )


def test_pickle_round_trip():
    cal = _calendar()
    restored = pickle.loads(pickle.dumps(cal))
    assert restored == cal


def test_snap_round_trip():
    cal = _calendar()
    snap = PartitionsSnap.from_def(cal)
    assert isinstance(snap, SessionPartitionsSnap)
    assert snap.get_partitions_definition() == cal

    snap_restored = deserialize_value(serialize_value(snap))
    assert snap_restored.get_partitions_definition() == cal


def test_persisted_session_window_serdes():
    window = PersistedSessionWindow(
        TimestampWithTimezone(1.0, NYC), TimestampWithTimezone(2.0, NYC)
    )
    restored = deserialize_value(serialize_value(window), PersistedSessionWindow)
    assert restored == window
    assert restored.start_timestamp == 1.0
    assert restored.end_timestamp == 2.0


# ----------------------------------------------------------------------
# Session -> session mapping
# ----------------------------------------------------------------------


def _split_upstream_calendar() -> SessionPartitionsDefinition:
    return SessionPartitionsDefinition(
        timezone=NYC,
        sessions=[
            TradingSession(
                "2024-03-14-am",
                [_window(datetime(2024, 3, 14, tzinfo=nyc), 9, 30, 12, 0)],
                NYC,
            ),
            TradingSession(
                "2024-03-14-pm",
                [_window(datetime(2024, 3, 14, tzinfo=nyc), 13, 0, 16, 0)],
                NYC,
            ),
            TradingSession(
                "2024-03-15-am",
                [_window(datetime(2024, 3, 15, tzinfo=nyc), 9, 30, 12, 0)],
                NYC,
            ),
        ],
    )


def test_session_to_session_mapping_uses_trading_windows_only():
    downstream = _calendar()
    upstream = _split_upstream_calendar()
    mapping = SessionPartitionMapping()
    mapping.validate_partition_mapping(upstream, downstream)

    result = mapping.get_upstream_mapped_partitions_result_for_partitions(
        downstream.subset_with_partition_keys(["2024-03-14"]),
        downstream,
        upstream,
    )
    assert set(result.partitions_subset.get_partition_keys()) == {
        "2024-03-14-am",
        "2024-03-14-pm",
    }
    assert result.required_but_nonexistent_subset.is_empty

    reverse = mapping.get_downstream_partitions_for_partitions(
        upstream.subset_with_partition_keys(["2024-03-14-pm", "2024-03-15-am"]),
        upstream,
        downstream,
    )
    assert set(reverse.get_partition_keys()) == {"2024-03-14", "2024-03-15"}


def test_session_mapping_reports_missing_upstream_sessions():
    downstream = _calendar()
    upstream = _split_upstream_calendar()

    result = SessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
        downstream.subset_with_partition_keys(["2024-03-18"]),
        downstream,
        upstream,
    )
    assert result.partitions_subset.is_empty
    assert set(result.required_but_nonexistent_subset.get_partition_keys()) == {
        "2024-03-18"
    }

    allowed = SessionPartitionMapping(
        allow_nonexistent_upstream_partitions=True
    ).get_upstream_mapped_partitions_result_for_partitions(
        downstream.subset_with_partition_keys(["2024-03-18"]),
        downstream,
        upstream,
    )
    assert allowed.required_but_nonexistent_subset.is_empty


def test_session_mapping_serdes():
    for mapping in (
        SessionPartitionMapping(),
        SessionPartitionMapping(allow_nonexistent_upstream_partitions=True),
    ):
        restored = deserialize_value(serialize_value(mapping))
        assert restored == mapping


# ----------------------------------------------------------------------
# Time window -> session mapping (minute/hour data feeding session summary)
# ----------------------------------------------------------------------


def _hourly_upstream() -> HourlyPartitionsDefinition:
    return HourlyPartitionsDefinition(
        start_date=datetime(2024, 3, 14, 0, 0, tzinfo=nyc),
        timezone=NYC,
        fmt="%Y-%m-%d-%H:%M",
    )


def test_time_window_to_session_mapping_excludes_breaks():
    cal = _calendar()
    hourly = _hourly_upstream()
    mapping = TimeWindowToSessionPartitionMapping()
    mapping.validate_partition_mapping(hourly, cal)

    result = mapping.get_upstream_mapped_partitions_result_for_partitions(
        cal.subset_with_partition_keys(["2024-03-14"]), cal, hourly
    )
    mapped = set(result.partitions_subset.get_partition_keys())
    assert mapped == {
        "2024-03-14-09:00",
        "2024-03-14-10:00",
        "2024-03-14-11:00",
        "2024-03-14-13:00",
        "2024-03-14-14:00",
        "2024-03-14-15:00",
    }
    # The 09:00 partition overlaps the 09:30 session start; the 12:00 lunch hour is excluded.
    assert "2024-03-14-09:00" in mapped
    assert "2024-03-14-12:00" not in mapped

    reverse = mapping.get_downstream_partitions_for_partitions(
        result.partitions_subset, hourly, cal
    )
    assert set(reverse.get_partition_keys()) == {"2024-03-14"}


def test_time_window_to_session_mapping_reports_missing_upstream_windows():
    # Upstream hourly calendar starts at 12:00; the 09:30-12:00 morning trading window has
    # no upstream coverage and must be reported, while the afternoon window maps normally.
    cal = _calendar()
    hourly = HourlyPartitionsDefinition(
        start_date=datetime(2024, 3, 14, 12, 0, tzinfo=nyc),
        end_date=datetime(2024, 3, 15, 0, 0, tzinfo=nyc),
        timezone=NYC,
        fmt="%Y-%m-%d-%H:%M",
    )
    result = TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
        cal.subset_with_partition_keys(["2024-03-14"]), cal, hourly
    )
    missing = result.required_but_nonexistent_subset.included_time_windows
    assert len(missing) == 1
    assert missing[0].start == datetime(2024, 3, 14, 9, 30, tzinfo=nyc)
    assert missing[0].end == datetime(2024, 3, 14, 12, 0, tzinfo=nyc)
    # The 12:00 partition falls entirely in the lunch break; afternoon trading maps 13:00-15:00.
    assert set(result.partitions_subset.get_partition_keys()) == {
        "2024-03-14-13:00",
        "2024-03-14-14:00",
        "2024-03-14-15:00",
    }

    # Suppressed via allow_nonexistent_upstream_partitions.
    suppressed = TimeWindowToSessionPartitionMapping(
        allow_nonexistent_upstream_partitions=True
    ).get_upstream_mapped_partitions_result_for_partitions(
        cal.subset_with_partition_keys(["2024-03-14"]), cal, hourly
    )
    assert suppressed.required_but_nonexistent_subset.is_empty


def test_time_window_to_session_mapping_partial_prefix_and_suffix_bounds():
    hourly = HourlyPartitionsDefinition(
        start_date=datetime(2024, 3, 14, 11, 0, tzinfo=nyc),
        end_date=datetime(2024, 3, 14, 14, 0, tzinfo=nyc),
        timezone=NYC,
        fmt="%Y-%m-%d-%H:%M",
    )
    cal = SessionPartitionsDefinition(
        sessions=[
            TradingSession(
                session_key="2024-03-14",
                timezone=NYC,
                windows=[
                    TimeWindow(
                        datetime(2024, 3, 14, 9, 30, tzinfo=nyc),
                        datetime(2024, 3, 14, 16, 0, tzinfo=nyc),
                    )
                ],
            )
        ],
        timezone=NYC,
    )
    result = TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
        cal.subset_with_partition_keys(["2024-03-14"]), cal, hourly
    )
    missing = result.required_but_nonexistent_subset.included_time_windows
    assert len(missing) == 2
    assert missing[0].start.timestamp() == datetime(2024, 3, 14, 9, 30, tzinfo=nyc).timestamp()
    assert missing[0].end.timestamp() == datetime(2024, 3, 14, 11, 0, tzinfo=nyc).timestamp()
    assert missing[1].start.timestamp() == datetime(2024, 3, 14, 14, 0, tzinfo=nyc).timestamp()
    assert missing[1].end.timestamp() == datetime(2024, 3, 14, 16, 0, tzinfo=nyc).timestamp()
    # No phantom partition keys earlier than the upstream's first partition survive. The
    # upstream spans 11:00-14:00; every one of its four partitions overlaps the contiguous
    # 09:30-16:00 trading window.
    mapped = set(result.partitions_subset.get_partition_keys())
    assert mapped == {
        "2024-03-14-11:00",
        "2024-03-14-12:00",
        "2024-03-14-13:00",
    }


def test_time_window_to_session_mapping_handles_overnight():
    day = datetime(2024, 3, 14, tzinfo=nyc)
    overnight = TradingSession(
        session_key="2024-03-14",
        timezone=NYC,
        windows=[
            TimeWindow(
                datetime(2024, 3, 14, 20, 0, tzinfo=nyc),
                datetime(2024, 3, 14, 23, 59, tzinfo=nyc),
            ),
            TimeWindow(
                datetime(2024, 3, 15, 0, 0, tzinfo=nyc),
                datetime(2024, 3, 15, 2, 30, tzinfo=nyc),
            ),
        ],
    )
    night_cal = SessionPartitionsDefinition(sessions=[overnight], timezone=NYC)
    hourly = HourlyPartitionsDefinition(
        start_date=datetime(2024, 3, 14, 18, 0, tzinfo=nyc),
        end_date=datetime(2024, 3, 15, 4, 0, tzinfo=nyc),
        timezone=NYC,
        fmt="%Y-%m-%d-%H:%M",
    )

    result = TimeWindowToSessionPartitionMapping().get_upstream_mapped_partitions_result_for_partitions(
        night_cal.subset_with_partition_keys(["2024-03-14"]),
        night_cal,
        hourly,
    )
    mapped = set(result.partitions_subset.get_partition_keys())
    assert "2024-03-14-20:00" in mapped
    assert "2024-03-15-01:00" in mapped  # post-midnight window
    assert "2024-03-14-18:00" not in mapped  # before the session


def test_inferred_partition_mappings():
    cal = _calendar()
    hourly = _hourly_upstream()

    assert isinstance(
        infer_partition_mapping(None, cal, _split_upstream_calendar()),
        SessionPartitionMapping,
    )
    assert isinstance(
        infer_partition_mapping(None, cal, hourly),
        TimeWindowToSessionPartitionMapping,
    )
    # existing time-window behavior unchanged
    daily = dg.DailyPartitionsDefinition(start_date="2024-03-01", timezone=NYC)
    assert isinstance(
        infer_partition_mapping(None, daily, dg.DailyPartitionsDefinition(start_date="2024-03-01", timezone=NYC)),
        dg.TimeWindowPartitionMapping,
    )


def test_session_definition_as_multi_partition_dimension():
    cal = _calendar()
    static = dg.StaticPartitionsDefinition(["eq", "bond"])
    multi = dg.MultiPartitionsDefinition(
        {"session": cal, "asset_class": static}
    )

    keys = multi.get_partition_keys()
    assert "eq|2024-03-14" in keys
    assert "bond|2024-03-18" in keys
    assert len(keys) == 6  # 3 sessions x 2 static values
    assert multi.has_time_window_dimension is False

    # Subset operations and serdes work through the key-based DefaultPartitionsSubset.
    subset = multi.subset_with_partition_keys(keys[:2])
    assert len(subset) == 2
    restored_subset = deserialize_value(serialize_value(subset))
    assert set(restored_subset.get_partition_keys()) == set(keys[:2])

    # Snap round trip preserves the session dimension's explicit windows.
    snap = PartitionsSnap.from_def(multi)
    assert snap.get_partitions_definition().get_partition_keys() == keys
    snap_restored = deserialize_value(serialize_value(snap))
    assert snap_restored.get_partitions_definition().get_partition_keys() == keys


def test_validation_rejects_wrong_mapping_targets():
    cal = _calendar()
    hourly = _hourly_upstream()

    with pytest.raises(DagsterInvalidDefinitionError):
        SessionPartitionMapping().validate_partition_mapping(hourly, cal)

    with pytest.raises(DagsterInvalidDefinitionError):
        TimeWindowToSessionPartitionMapping().validate_partition_mapping(cal, hourly)
