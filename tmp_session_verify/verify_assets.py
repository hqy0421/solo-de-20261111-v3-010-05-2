"""End-to-end asset materialization test for session partitions."""
import warnings
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import dagster as dg

NYC = "America/New_York"
nyc = ZoneInfo(NYC)


def trading_windows(day: datetime):
    return [
        dg.TimeWindow(
            datetime(day.year, day.month, day.day, 9, 30, tzinfo=nyc),
            datetime(day.year, day.month, day.day, 12, 0, tzinfo=nyc),
        ),
        dg.TimeWindow(
            datetime(day.year, day.month, day.day, 13, 0, tzinfo=nyc),
            datetime(day.year, day.month, day.day, 16, 0, tzinfo=nyc),
        ),
    ]


days = [datetime(2024, 3, d, tzinfo=nyc) for d in (14, 15)]
session_pd = dg.SessionPartitionsDefinition(
    sessions=[
        dg.TradingSession(
            session_key=d.date().isoformat(), timezone=NYC, windows=trading_windows(d)
        )
        for d in days
    ],
    timezone=NYC,
)

hourly_pd = dg.HourlyPartitionsDefinition(
    start_date=datetime(2024, 3, 14, 0, 0, tzinfo=nyc),
    timezone=NYC,
    fmt="%Y-%m-%d-%H:%M",
)


class PersistentIOManager(dg.ConfigurableIOManager):
    """A persistent IO manager keyed by (asset key, partition key), visible across runs."""

    def setup_for_execution(self, context) -> None:
        pass

    def handle_output(self, context, obj):
        _STORE[(context.asset_key, context.partition_key)] = obj

    def load_input(self, context):
        keys = context.asset_partition_keys
        if keys:
            return {k: _STORE[(context.upstream_output.asset_key, k)] for k in keys}
        return _STORE[(context.upstream_output.asset_key, context.upstream_output.partition_key)]


_STORE: dict = {}


@dg.asset(partitions_def=hourly_pd)
def minute_bars(context: dg.AssetExecutionContext) -> int:
    return 60  # 60 bars per hourly partition


@dg.asset(
    partitions_def=session_pd,
    ins={
        "minute_bars": dg.AssetIn(
            partition_mapping=dg.TimeWindowToSessionPartitionMapping()
        )
    },
)
def session_summary(
    context: dg.AssetExecutionContext, minute_bars: dict[str, int]
) -> int:
    partitions_def = context.assets_def.partitions_def
    session = partitions_def.get_session_for_partition_key(context.partition_key)
    assert len(session.to_public_time_windows()) == 2
    return sum(minute_bars.values())


# ----------------------------------------------------------------------
# 1. Definitions validate (repository-level partition type checks pass)
# ----------------------------------------------------------------------
defs = dg.Definitions(
    assets=[minute_bars, session_summary],
    resources={"io_manager": PersistentIOManager()},
)
repo = defs.get_repository_def()
_ = repo.get_all_jobs()
print("Definitions + repository validation: OK")

# ----------------------------------------------------------------------
# 2. Determine exactly which hourly partitions the session needs
# ----------------------------------------------------------------------
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    mapping = dg.TimeWindowToSessionPartitionMapping()
    upstream_result = mapping.get_upstream_mapped_partitions_result_for_partitions(
        session_pd.subset_with_partition_keys(["2024-03-14"]),
        session_pd,
        hourly_pd,
    )
upstream_keys = sorted(upstream_result.partitions_subset.get_partition_keys())
print("upstream hourly keys for 2024-03-14:", upstream_keys)
assert "2024-03-14-12:00" not in upstream_keys, "lunch hour must not be a dependency"
assert "2024-03-14-09:00" in upstream_keys
assert len(upstream_keys) == 6

# ----------------------------------------------------------------------
# 3. Materialize the required upstream hourly partitions (separate runs)
# ----------------------------------------------------------------------
shared_instance = dg.DagsterInstance.ephemeral()
for key in upstream_keys:
    r = dg.materialize(
        [minute_bars],
        partition_key=key,
        resources={"io_manager": PersistentIOManager()},
        instance=shared_instance,
    )
    assert r.success

# ----------------------------------------------------------------------
# 4. Materialize the session summary; upstream supplied as a source spec.
# ----------------------------------------------------------------------
minute_bars_source = dg.AssetSpec("minute_bars", partitions_def=hourly_pd)
instance = shared_instance
result = dg.materialize(
    [session_summary, minute_bars_source],
    partition_key="2024-03-14",
    resources={"io_manager": PersistentIOManager()},
    instance=instance,
)
assert result.success, "session summary materialization failed"

# The summary aggregates all 6 hourly partitions x 60 bars = 360.
assert _STORE[(dg.AssetKey("session_summary"), "2024-03-14")] == 360
print("session 2024-03-14 aggregated value:",
      _STORE[(dg.AssetKey("session_summary"), "2024-03-14")])

event = instance.get_latest_materialization_event(dg.AssetKey("session_summary"))
assert event is not None
assert event.dagster_event.partition == "2024-03-14"
print("session materialization recorded on instance: OK")

# ----------------------------------------------------------------------
# 5. Materialized partition status tracked through the cacheable subset
# ----------------------------------------------------------------------
materialized_keys = instance.get_materialized_partitions(
    dg.AssetKey("session_summary")
)
assert "2024-03-14" in set(materialized_keys)
print("materialized subset:", sorted(materialized_keys))

# ----------------------------------------------------------------------
# 6. Inferred mapping for session -> session dependency
# ----------------------------------------------------------------------
@dg.asset(partitions_def=session_pd)
def downstream_summary(session_summary: int) -> int:
    return session_summary


inferred_defs = dg.Definitions(assets=[minute_bars, session_summary, downstream_summary])
asset_graph = inferred_defs.resolve_asset_graph()
resolved_mapping = asset_graph.get_partition_mapping(
    dg.AssetKey("downstream_summary"), dg.AssetKey("session_summary")
)
print("inferred session->session mapping:", type(resolved_mapping).__name__)
assert type(resolved_mapping).__name__ == "SessionPartitionMapping"

# Also confirm the time-window -> session dependency resolves to the trading-aware mapping.
tw_resolved = asset_graph.get_partition_mapping(
    dg.AssetKey("session_summary"), dg.AssetKey("minute_bars")
)
print("inferred timewindow->session mapping:", type(tw_resolved).__name__)
assert type(tw_resolved).__name__ == "TimeWindowToSessionPartitionMapping"

# ----------------------------------------------------------------------
# 7. Backfill range resolution across sessions
# ----------------------------------------------------------------------
keys_in_range = session_pd.get_partition_keys_in_range(
    dg.PartitionKeyRange("2024-03-14", "2024-03-15")
)
assert list(keys_in_range) == ["2024-03-14", "2024-03-15"]
print("backfill range keys:", list(keys_in_range))

print("END-TO-END ASSET TEST PASSED")
