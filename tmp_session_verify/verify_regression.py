"""Regression checks: existing partition behaviors must be untouched."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import dagster as dg
from dagster._core.definitions.partitions.snap import PartitionsSnap
from dagster._core.definitions.partitions.subset import (
    DefaultPartitionsSubset,
    TimeWindowPartitionsSubset,
)
from dagster._core.definitions.partitions.utils.mapping import (
    infer_partition_mapping,
)
from dagster._serdes import deserialize_value, serialize_value

NYC = "America/New_York"
nyc = ZoneInfo(NYC)
fails = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(name)


# 1. Existing time-window partition definitions still construct and enumerate.
daily = dg.DailyPartitionsDefinition(start_date="2024-03-01", timezone=NYC)
hourly = dg.HourlyPartitionsDefinition(
    start_date=datetime(2024, 3, 14, tzinfo=nyc), timezone=NYC
)
static = dg.StaticPartitionsDefinition(["a", "b", "c"])
dynamic = dg.DynamicPartitionsDefinition(name="dyn")
multi = dg.MultiPartitionsDefinition(
    {"day": daily, "static": static}
)

check("daily keys", daily.get_partition_keys(current_time=datetime(2024, 3, 4, tzinfo=nyc))[0] == "2024-03-01")
check("hourly first key", hourly.get_first_partition_key() is not None)
check("static keys", static.get_partition_keys() == ["a", "b", "c"])
check("multi partitions def", multi.get_num_partitions() >= 1)

# 2. Existing time-window mapping still inferred and functional.
daily2 = dg.DailyPartitionsDefinition(start_date="2024-03-01", timezone=NYC)
m = infer_partition_mapping(None, daily2, daily)
check("daily->daily infers TimeWindowPartitionMapping", isinstance(m, dg.TimeWindowPartitionMapping), type(m).__name__)
m_static = infer_partition_mapping(None, static, static)
check("static->static infers IdentityPartitionMapping", isinstance(m_static, dg.IdentityPartitionMapping))

# 3. Time window subset operations and serdes still work.
tw_subset = daily.subset_with_partition_keys(["2024-03-02", "2024-03-03"])
check("time window subset len", len(tw_subset) == 2)
ser = tw_subset.serialize()
restored = daily.deserialize_subset(ser)
check("time window subset serialization", restored == tw_subset)
full_ser = serialize_value(tw_subset.to_serializable_subset())
check("time window subset full serdes", deserialize_value(full_ser) == tw_subset.to_serializable_subset())

# 4. Definition-level serdes for existing types.
for pd_ in (daily, hourly, static, multi):
    snap = PartitionsSnap.from_def(pd_)
    rebuilt = snap.get_partitions_definition()
    check(f"snap round trip {type(pd_).__name__}", rebuilt == pd_ or rebuilt.get_partition_keys() == pd_.get_partition_keys())

# 5. Default subset still works.
d = DefaultPartitionsSubset({"a", "b"})
check("default subset", len(d | DefaultPartitionsSubset({"c"})) == 3)

# 6. TimeWindowPartitionMapping overlap semantics unchanged.
up = dg.HourlyPartitionsDefinition(
    start_date=datetime(2024, 3, 14, 0, tzinfo=nyc),
    end_date=datetime(2024, 3, 15, 0, tzinfo=nyc),
    timezone=NYC,
)
down = dg.DailyPartitionsDefinition(start_date="2024-03-14", timezone=NYC)
twm = dg.TimeWindowPartitionMapping()
res = twm.get_upstream_mapped_partitions_result_for_partitions(
    down.subset_with_partition_keys(["2024-03-14"]), down, up
)
check("time window mapping returns hourly keys", len(list(res.partitions_subset.get_partition_keys())) == 24,
      str(list(res.partitions_subset.get_partition_keys())[:5]))

# 7. Exclusions still work.
with_exclusions = dg.DailyPartitionsDefinition(
    start_date="2024-03-01",
    timezone=NYC,
    exclusions=[datetime(2024, 3, 2, tzinfo=nyc)],
)
keys = with_exclusions.get_partition_keys(current_time=datetime(2024, 3, 5, tzinfo=nyc))
check("exclusions honored", "2024-03-02" not in keys)

# 8. Backfill policy and key ranges.
rng = daily.get_partition_keys_in_range(dg.PartitionKeyRange("2024-03-02", "2024-03-04"))
check("daily key range", list(rng) == ["2024-03-02", "2024-03-03", "2024-03-04"], str(rng))

# 9. Session mapping serdes through the whitelist.
sess_pd = dg.SessionPartitionsDefinition(
    sessions=[
        dg.TradingSession(
            session_key="2024-03-14",
            timezone=NYC,
            windows=[dg.TimeWindow(
                datetime(2024, 3, 14, 9, 30, tzinfo=nyc),
                datetime(2024, 3, 14, 16, 0, tzinfo=nyc),
            )],
        )
    ],
    timezone=NYC,
)
for mapping_obj in (dg.SessionPartitionMapping(), dg.TimeWindowToSessionPartitionMapping(),
                    dg.SessionPartitionMapping(allow_nonexistent_upstream_partitions=True)):
    rt = deserialize_value(serialize_value(mapping_obj))
    check(f"mapping serdes {type(mapping_obj).__name__}", rt == mapping_obj)

# 10. Session def does not collide with time window in the snap dispatch or validation.
snap = PartitionsSnap.from_def(sess_pd)
check("session snap dispatch", type(snap).__name__ == "SessionPartitionsSnap")
tw_snap = PartitionsSnap.from_def(daily)
check("daily still uses TimeWindowPartitionsSnap", type(tw_snap).__name__ == "TimeWindowPartitionsSnap")

print()
print("ALL REGRESSION CHECKS PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
