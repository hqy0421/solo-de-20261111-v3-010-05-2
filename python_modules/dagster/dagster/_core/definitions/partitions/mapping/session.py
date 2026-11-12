from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple, Optional

import dagster._check as check
from dagster._annotations import PublicAttr
from dagster._core.definitions.partitions.context import partition_loading_context
from dagster._core.definitions.partitions.definition import (
    PartitionsDefinition,
    SessionPartitionsDefinition,
)
from dagster._core.definitions.partitions.mapping.partition_mapping import (
    PartitionMapping,
    UpstreamPartitionsResult,
)
from dagster._core.definitions.partitions.subset.partitions_subset import PartitionsSubset
from dagster._core.definitions.partitions.subset.session import SessionPartitionsSubset
from dagster._core.definitions.partitions.utils.time_window import TimeWindow
from dagster._core.errors import DagsterInvalidDefinitionError
from dagster._serdes import whitelist_for_serdes

if TYPE_CHECKING:
    from dagster._core.instance import DynamicPartitionsStore


@whitelist_for_serdes
class SessionPartitionMapping(
    PartitionMapping,
    NamedTuple(
        "_SessionPartitionMapping",
        [
            ("allow_nonexistent_upstream_partitions", PublicAttr[bool]),
        ],
    ),
):
    """The default mapping between two :class:`SessionPartitionsDefinition` s.

    A downstream session partition maps to every upstream session partition whose actual
    trading windows overlap any of the downstream session's trading windows. Gaps inside a
    session (lunch breaks, overnight gaps between sessions) never produce a dependency: only
    real trading time is considered.

    Because the mapping works on absolute timestamps, it correctly handles sessions that span
    midnight (an evening session on business day N whose overnight window extends into calendar
    day N+1 maps to upstream partitions covering that same absolute interval) and DST
    transition days.

    Args:
        allow_nonexistent_upstream_partitions (bool): Defaults to False. If True, an upstream
            calendar that does not contain a session covering overlapping trading time does not
            raise; the missing upstream sessions are simply omitted. If False, the missing
            sessions are tracked in
            ``UpstreamPartitionsResult.required_but_nonexistent_subset`` so the framework can
            surface the gap (e.g. during asset reconciliation).

    Examples:
        .. code-block:: python

            from dagster import AssetIn, SessionPartitionMapping, asset

            @asset(partitions_def=minute_session_partitions)
            def minute_bars():
                ...

            @asset(
                partitions_def=daily_session_partitions,
                ins={
                    "minute_bars": AssetIn(
                        partition_mapping=SessionPartitionMapping(
                            allow_nonexistent_upstream_partitions=True
                        )
                    )
                },
            )
            def session_summary(minute_bars):
                ...
    """

    def __new__(
        cls,
        allow_nonexistent_upstream_partitions: bool = False,
    ):
        return super().__new__(
            cls,
            allow_nonexistent_upstream_partitions=check.bool_param(
                allow_nonexistent_upstream_partitions,
                "allow_nonexistent_upstream_partitions",
            ),
        )

    @property
    def description(self) -> str:
        return (
            "Maps a downstream trading session to every upstream trading session whose actual"
            " trading time windows overlap it. Breaks and overnight gaps do not create"
            " dependencies."
        )

    def validate_partition_mapping(
        self,
        upstream_partitions_def: PartitionsDefinition,
        downstream_partitions_def: PartitionsDefinition | None,
    ) -> None:
        if not isinstance(downstream_partitions_def, SessionPartitionsDefinition):
            raise DagsterInvalidDefinitionError(
                "Downstream partitions definition must be a SessionPartitionsDefinition"
            )
        if not isinstance(upstream_partitions_def, SessionPartitionsDefinition):
            raise DagsterInvalidDefinitionError(
                "Upstream partitions definition must be a SessionPartitionsDefinition"
            )

    # ------------------------------------------------------------------
    # downstream -> upstream
    # ------------------------------------------------------------------

    def get_upstream_mapped_partitions_result_for_partitions(
        self,
        downstream_partitions_subset: PartitionsSubset | None,
        downstream_partitions_def: PartitionsDefinition | None,
        upstream_partitions_def: PartitionsDefinition,
        current_time: datetime | None = None,
        dynamic_partitions_store: Optional["DynamicPartitionsStore"] = None,
    ) -> UpstreamPartitionsResult:
        with partition_loading_context(current_time, dynamic_partitions_store):
            downstream_def = check.inst(
                downstream_partitions_def, SessionPartitionsDefinition
            )
            upstream_def = check.inst(
                upstream_partitions_def, SessionPartitionsDefinition
            )
            downstream_subset = check.inst(
                downstream_partitions_subset, SessionPartitionsSubset
            )

            mapped_upstream_keys: set[str] = set()
            # Sessions that the downstream needs but that do not exist in the upstream
            # calendar. The "identity" of a missing partition is the downstream session key,
            # which is the only stable name available when the upstream has no such session.
            required_but_nonexistent_keys: set[str] = set()

            for downstream_key in downstream_subset.get_partition_keys():
                downstream_session = downstream_def.get_session_for_partition_key(
                    downstream_key
                )
                overlapping = _overlapping_session_keys(
                    upstream_def, downstream_session
                )

                if overlapping:
                    mapped_upstream_keys.update(overlapping)
                elif self._downstream_session_has_trading_time(downstream_session):
                    # The downstream session contains real trading time but the upstream
                    # calendar has no overlapping session at all.
                    if not self.allow_nonexistent_upstream_partitions:
                        required_but_nonexistent_keys.add(downstream_key)

            return UpstreamPartitionsResult(
                partitions_subset=SessionPartitionsSubset(
                    upstream_def, mapped_upstream_keys
                ),
                required_but_nonexistent_subset=SessionPartitionsSubset(
                    downstream_def, required_but_nonexistent_keys
                ),
            )

    @staticmethod
    def _downstream_session_has_trading_time(downstream_session) -> bool:
        return any(
            window.end_timestamp > window.start_timestamp
            for window in downstream_session.windows
        )

    # ------------------------------------------------------------------
    # upstream -> downstream
    # ------------------------------------------------------------------

    def get_downstream_partitions_for_partitions(
        self,
        upstream_partitions_subset: PartitionsSubset,
        upstream_partitions_def: PartitionsDefinition,
        downstream_partitions_def: PartitionsDefinition | None,
        current_time: datetime | None = None,
        dynamic_partitions_store: Optional["DynamicPartitionsStore"] = None,
    ) -> PartitionsSubset:
        with partition_loading_context(current_time, dynamic_partitions_store):
            upstream_def = check.inst(
                upstream_partitions_def, SessionPartitionsDefinition
            )
            downstream_def = check.inst(
                downstream_partitions_def, SessionPartitionsDefinition
            )
            upstream_subset = check.inst(
                upstream_partitions_subset, SessionPartitionsSubset
            )

            mapped_downstream_keys: set[str] = set()
            for upstream_key in upstream_subset.get_partition_keys():
                upstream_session = upstream_def.get_session_for_partition_key(
                    upstream_key
                )
                mapped_downstream_keys.update(
                    _overlapping_session_keys(downstream_def, upstream_session)
                )

            return SessionPartitionsSubset(downstream_def, mapped_downstream_keys)


def _overlapping_session_keys(
    target_def: SessionPartitionsDefinition,
    source_session,
) -> Sequence[str]:
    """Returns the keys of every session in ``target_def`` whose actual trading windows
    overlap any window of ``source_session``.

    Only absolute-timestamp window overlap counts. An interval covering only a break or gap
    produces no matches.
    """
    matches: list[str] = []
    for target_session in target_def.sessions:
        # Both session lists are sorted by start timestamp and sessions do not overlap within
        # a calendar, so stop scanning once the target starts at or after the source ends.
        if target_session.start_timestamp >= source_session.end_timestamp:
            break
        for source_window in source_session.windows:
            if target_session.overlaps_window(
                source_window.start_timestamp, source_window.end_timestamp
            ):
                matches.append(target_session.session_key)
                break
    return matches


def _overlapping_time_window_partition_keys(
    time_window_def,
    window_start: datetime,
    window_end: datetime,
) -> Sequence[str]:
    """Returns every partition key of a TimeWindowPartitionsDefinition whose single
    ``[start, end)`` window overlaps the given ``[window_start, window_end)`` interval.

    Unlike ``get_partition_keys_in_time_window`` (which only returns partitions whose start
    falls inside the interval), this also includes a partition that starts before the interval
    but is still open when it begins -- necessary when a trading session starts partway through
    an upstream minute/hour partition.

    Returned keys are aligned to the upstream definition's own bounds: cron-based lookups can
    name partitions earlier than the definition's ``start_date`` (or later than its
    ``end_date``), and such keys are filtered out here so callers never observe a partition
    that the upstream calendar does not actually contain.
    """
    keys: list[str] = []

    # Include the partition already open at window_start, if any: reverse iteration yields the
    # most recent partition whose start is <= window_start.
    preceding_key = time_window_def.get_partition_key_for_timestamp(
        window_start.timestamp(), end_closed=False
    )
    preceding_window = time_window_def.time_window_for_partition_key(preceding_key)
    if preceding_window.end.timestamp() > window_start.timestamp():
        keys.append(preceding_key)

    # Partitions starting at or after window_start and before window_end.
    for key in time_window_def.get_partition_keys_in_time_window(
        TimeWindow(window_start, window_end)
    ):
        if key not in keys:
            keys.append(key)

    # Cron lookups ignore the definition's explicit bounds; drop partitions outside them. The
    # framework would also filter these when building a subset, but removing them here keeps the
    # returned key list honest and lets the caller compute the missing interval precisely.
    first_window = time_window_def.get_first_partition_window()
    last_window = time_window_def.get_last_partition_window()
    if first_window is None or last_window is None:
        return []

    def _within_bounds(key: str) -> bool:
        candidate = time_window_def.time_window_for_partition_key(key)
        return (
            candidate.start.timestamp() >= first_window.start.timestamp()
            and candidate.end.timestamp() <= last_window.end.timestamp()
        )

    return [key for key in keys if _within_bounds(key)]


@whitelist_for_serdes
class TimeWindowToSessionPartitionMapping(
    PartitionMapping,
    NamedTuple(
        "_TimeWindowToSessionPartitionMapping",
        [
            ("allow_nonexistent_upstream_partitions", PublicAttr[bool]),
        ],
    ),
):
    """Maps a downstream :class:`SessionPartitionsDefinition` to an upstream
    :class:`~dagster.TimeWindowPartitionsDefinition` (e.g. minute bars) using only actual
    trading time.

    Each downstream session partition maps to the upstream time-window partitions that overlap
    any of the session's explicit trading windows. Upstream partitions falling entirely in a
    lunch break, overnight gap, or outside the session are excluded. This is what distinguishes
    it from the generic :class:`~dagster.TimeWindowPartitionMapping`, which would map based on
    the full span from session start to session end and incorrectly pull in break-time
    partitions.
    """

    def __new__(
        cls,
        allow_nonexistent_upstream_partitions: bool = False,
    ):
        return super().__new__(
            cls,
            allow_nonexistent_upstream_partitions=check.bool_param(
                allow_nonexistent_upstream_partitions,
                "allow_nonexistent_upstream_partitions",
            ),
        )

    @property
    def description(self) -> str:
        return (
            "Maps a downstream trading session to upstream time-window partitions overlapping"
            " its explicit trading windows only; partitions in breaks and gaps are excluded."
        )

    def validate_partition_mapping(
        self,
        upstream_partitions_def: PartitionsDefinition,
        downstream_partitions_def: PartitionsDefinition | None,
    ) -> None:
        from dagster._core.definitions.partitions.definition import (
            TimeWindowPartitionsDefinition,
        )

        if not isinstance(downstream_partitions_def, SessionPartitionsDefinition):
            raise DagsterInvalidDefinitionError(
                "Downstream partitions definition must be a SessionPartitionsDefinition"
            )
        if not isinstance(upstream_partitions_def, TimeWindowPartitionsDefinition):
            raise DagsterInvalidDefinitionError(
                "Upstream partitions definition must be a TimeWindowPartitionsDefinition"
            )

    def get_upstream_mapped_partitions_result_for_partitions(
        self,
        downstream_partitions_subset: PartitionsSubset | None,
        downstream_partitions_def: PartitionsDefinition | None,
        upstream_partitions_def: PartitionsDefinition,
        current_time: datetime | None = None,
        dynamic_partitions_store: Optional["DynamicPartitionsStore"] = None,
    ) -> UpstreamPartitionsResult:
        from dagster._core.definitions.partitions.definition import (
            TimeWindowPartitionsDefinition,
        )
        from dagster._core.definitions.partitions.subset.time_window import (
            TimeWindowPartitionsSubset,
        )

        with partition_loading_context(current_time, dynamic_partitions_store):
            downstream_def = check.inst(
                downstream_partitions_def, SessionPartitionsDefinition
            )
            upstream_def = check.inst(
                upstream_partitions_def, TimeWindowPartitionsDefinition
            )
            downstream_subset = check.inst(
                downstream_partitions_subset, SessionPartitionsSubset
            )

            mapped_subset = upstream_def.empty_subset()
            required_but_nonexistent = upstream_def.empty_subset()

            first_upstream_window = upstream_def.get_first_partition_window()
            last_upstream_window = upstream_def.get_last_partition_window()

            for downstream_key in downstream_subset.get_partition_keys():
                session = downstream_def.get_session_for_partition_key(downstream_key)
                for persisted_window in session.windows:
                    trading_window = persisted_window.to_public_time_window()
                    trading_start = trading_window.start
                    trading_end = trading_window.end
                    overlapping_keys = _overlapping_time_window_partition_keys(
                        upstream_def,
                        trading_start,
                        trading_end,
                    )
                    if overlapping_keys:
                        mapped_subset = mapped_subset | upstream_def.subset_with_partition_keys(
                            overlapping_keys
                        )

                    if self.allow_nonexistent_upstream_partitions:
                        continue

                    # Compute the portion of this trading window that the upstream calendar
                    # cannot cover. Mirrors TimeWindowPartitionMapping's out-of-bounds handling:
                    # the whole window when the upstream has no partitions at all, the prefix
                    # before its first partition, and/or the suffix after its last partition.
                    missing_windows: list[TimeWindow] = []
                    if first_upstream_window is None or last_upstream_window is None:
                        missing_windows.append(trading_window)
                    else:
                        if (
                            trading_start.timestamp()
                            < first_upstream_window.start.timestamp()
                        ):
                            missing_windows.append(
                                TimeWindow(
                                    trading_start,
                                    min(
                                        trading_end,
                                        first_upstream_window.start,
                                        key=lambda d: d.timestamp(),
                                    ),
                                )
                            )
                        if (
                            trading_end.timestamp()
                            > last_upstream_window.end.timestamp()
                        ):
                            missing_windows.append(
                                TimeWindow(
                                    max(
                                        trading_start,
                                        last_upstream_window.end,
                                        key=lambda d: d.timestamp(),
                                    ),
                                    trading_end,
                                )
                            )

                    for missing_window in missing_windows:
                        # Defensive: never record a degenerate interval.
                        if (
                            missing_window.start.timestamp()
                            < missing_window.end.timestamp()
                        ):
                            required_but_nonexistent = (
                                required_but_nonexistent
                                | upstream_def.get_partition_subset_in_time_window(
                                    missing_window
                                )
                            )

            return UpstreamPartitionsResult(
                partitions_subset=mapped_subset,
                required_but_nonexistent_subset=required_but_nonexistent,
            )

    def get_downstream_partitions_for_partitions(
        self,
        upstream_partitions_subset: PartitionsSubset,
        upstream_partitions_def: PartitionsDefinition,
        downstream_partitions_def: PartitionsDefinition | None,
        current_time: datetime | None = None,
        dynamic_partitions_store: Optional["DynamicPartitionsStore"] = None,
    ) -> PartitionsSubset:
        from dagster._core.definitions.partitions.definition import (
            TimeWindowPartitionsDefinition,
        )
        from dagster._core.definitions.partitions.subset.time_window import (
            TimeWindowPartitionsSubset,
        )

        with partition_loading_context(current_time, dynamic_partitions_store):
            upstream_def = check.inst(
                upstream_partitions_def, TimeWindowPartitionsDefinition
            )
            downstream_def = check.inst(
                downstream_partitions_def, SessionPartitionsDefinition
            )
            upstream_subset = check.inst(
                upstream_partitions_subset, TimeWindowPartitionsSubset
            )

            downstream_keys: set[str] = set()
            for included_window in upstream_subset.included_time_windows:
                public_window = TimeWindow(
                    included_window.start, included_window.end
                )
                downstream_keys.update(
                    downstream_def.get_partition_keys_overlapping_time_window(
                        public_window
                    )
                )

            return SessionPartitionsSubset(downstream_def, downstream_keys)
