import functools
import hashlib
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple, Optional, Union

import dagster._check as check
from dagster._annotations import PublicAttr, public
from dagster._core.definitions.partitions.context import (
    PartitionLoadingContext,
    partition_loading_context,
)
from dagster._core.definitions.partitions.definition.partitions_definition import (
    PartitionsDefinition,
)
from dagster._core.definitions.partitions.partition_key_range import PartitionKeyRange
from dagster._core.definitions.partitions.utils.time_window import TimeWindow
from dagster._core.definitions.timestamp import TimestampWithTimezone
from dagster._core.errors import DagsterInvalidDefinitionError
from dagster._core.types.pagination import PaginatedResults
from dagster._record import IHaveNew, record_custom
from dagster._serdes import whitelist_for_serdes
from dagster._time import datetime_from_timestamp

if TYPE_CHECKING:
    from dagster._core.definitions.partitions.subset.partitions_subset import PartitionsSubset
    from dagster._core.definitions.partitions.subset.session import SessionPartitionsSubset
    from dagster._core.instance import DynamicPartitionsStore


@whitelist_for_serdes
class PersistedSessionWindow(
    NamedTuple(
        "_PersistedSessionWindow",
        [("start", TimestampWithTimezone), ("end", TimestampWithTimezone)],
    )
):
    """Internal serialized representation of a single time window within a trading session.

    Timestamps are stored as absolute Unix timestamps alongside the IANA timezone so that
    DST rules and the UTC offset at serialization time are both preserved.
    """

    def __new__(cls, start: TimestampWithTimezone, end: TimestampWithTimezone):
        return super(cls, PersistedSessionWindow).__new__(
            cls,
            start=check.inst_param(start, "start", TimestampWithTimezone),
            end=check.inst_param(end, "end", TimestampWithTimezone),
        )

    @property
    def start_timestamp(self) -> float:
        return self._asdict()["start"].timestamp

    @property
    def end_timestamp(self) -> float:
        return self._asdict()["end"].timestamp

    @property
    def start(self) -> datetime:
        start_tw = self._asdict()["start"]
        return datetime_from_timestamp(start_tw.timestamp, start_tw.timezone)

    @property
    def end(self) -> datetime:
        end_tw = self._asdict()["end"]
        return datetime_from_timestamp(end_tw.timestamp, end_tw.timezone)

    def to_public_time_window(self) -> TimeWindow:
        return TimeWindow(start=self.start, end=self.end)


@whitelist_for_serdes
@record_custom
class TradingSession:
    """A single trading session on an explicit trading calendar.

    A session is identified by a business-day key and contains one or more non-overlapping
    time windows (e.g. an overnight session that crosses midnight followed by a day session).
    Windows are stored as timezone-aware absolute timestamps, so cross-midnight sessions and
    daylight-saving-time transitions are handled correctly: each window is an independent
    ``[start, end)`` interval on the UTC timeline.

    Args:
        session_key (str): A stable, unique identifier for the session, typically the business
            date (e.g. ``"2024-03-15"``). Session keys must be unique within a calendar and
            must remain stable across serialization/deserialization.
        windows (Sequence[TimeWindow]): One or more non-overlapping, ``[start, end)`` time
            windows that make up the session, ordered by start time. A window may start on a
            different calendar day than the session key (e.g. an overnight session), and
            windows may be shorter on DST transition days.
        timezone (str): The IANA timezone in which the session windows are interpreted.
    """

    session_key: PublicAttr[str]
    windows: PublicAttr[Sequence[PersistedSessionWindow]]
    timezone: PublicAttr[str]

    def __new__(
        cls,
        session_key: str,
        windows: Sequence[Union[TimeWindow, "PersistedSessionWindow"]],
        timezone: str,
    ):
        check.str_param(session_key, "session_key")
        check.str_param(timezone, "timezone")
        check.sequence_param(windows, "windows")

        if len(windows) == 0:
            raise DagsterInvalidDefinitionError(
                f"Trading session '{session_key}' must contain at least one time window."
            )

        persisted_windows: list[PersistedSessionWindow] = []
        for window in windows:
            if isinstance(window, PersistedSessionWindow):
                persisted_windows.append(window)
            else:
                check.inst_param(window, "window", TimeWindow)
                if window.start.tzinfo is None or window.end.tzinfo is None:
                    raise DagsterInvalidDefinitionError(
                        "Trading session time windows must be timezone-aware datetimes."
                    )
                if window.start.timestamp() >= window.end.timestamp():
                    raise DagsterInvalidDefinitionError(
                        f"Trading session '{session_key}' has an empty or reversed time window: "
                        f"{window.start} -> {window.end}."
                    )
                persisted_windows.append(
                    PersistedSessionWindow(
                        TimestampWithTimezone(window.start.timestamp(), timezone),
                        TimestampWithTimezone(window.end.timestamp(), timezone),
                    )
                )

        persisted_windows.sort(key=lambda w: w.start_timestamp)
        for earlier, later in zip(persisted_windows, persisted_windows[1:]):
            if earlier.end_timestamp > later.start_timestamp:
                raise DagsterInvalidDefinitionError(
                    f"Trading session '{session_key}' has overlapping time windows: "
                    f"{earlier.start} -> {earlier.end} overlaps {later.start} -> {later.end}."
                )

        return super().__new__(
            cls,
            session_key=session_key,
            windows=persisted_windows,
            timezone=timezone,
        )

    @public
    @property
    def start(self) -> datetime:
        """datetime: The start of the earliest window in the session (timezone-aware)."""
        return self.windows[0].start

    @public
    @property
    def end(self) -> datetime:
        """datetime: The end of the latest window in the session (timezone-aware)."""
        return self.windows[-1].end

    @property
    def start_timestamp(self) -> float:
        return self.windows[0].start_timestamp

    @property
    def end_timestamp(self) -> float:
        return self.windows[-1].end_timestamp

    def to_public_time_windows(self) -> Sequence[TimeWindow]:
        """The session's windows as public ``TimeWindow`` objects."""
        return [window.to_public_time_window() for window in self.windows]

    def contains_timestamp(self, timestamp: float) -> bool:
        """Whether the timestamp falls within any of this session's windows (start-closed,
        end-open)."""
        for window in self.windows:
            if window.start_timestamp <= timestamp < window.end_timestamp:
                return True
        return False

    def overlaps_window(self, other_start: float, other_end: float) -> bool:
        """Whether any of this session's windows overlaps the given ``[start, end)`` interval."""
        for window in self.windows:
            if window.start_timestamp < other_end and other_start < window.end_timestamp:
                return True
        return False


@whitelist_for_serdes
@record_custom
class SessionPartitionsDefinition(PartitionsDefinition, IHaveNew):
    r"""A set of partitions defined by an explicit trading calendar, where each partition is a
    trading session that may contain multiple non-overlapping time windows.

    Unlike :class:`TimeWindowPartitionsDefinition`, whose partitions are a single contiguous
    window derived from a cron schedule, a session partition represents one business day of a
    market that can have an overnight session crossing midnight, a lunch break, or an early
    close. Each partition key maps to an ordered list of ``[start, end)`` windows, and all
    time-based operations (partition lookup, upstream mapping, subset membership) consider only
    the actual trading windows -- never the gaps between them.

    The partition set is fully explicit: the provided ``sessions`` define every partition.
    Sessions must have unique, stable keys and must not overlap each other on the absolute
    (UTC) timeline. Because timestamps are stored absolutely with their IANA timezone, sessions
    spanning midnight and days containing DST transitions are represented correctly.

    Args:
        sessions (Sequence[TradingSession]): The trading sessions that define the partition
            set, in chronological order. Each session must have a unique key, and no two
            sessions may have overlapping time windows.
        timezone (str): The IANA timezone in which the calendar is defined. All provided
            session windows must be timezone-aware; this timezone is used when rendering and
            resolving times. Defaults to ``"UTC"``.

    Examples:
        .. code-block:: python

            from datetime import time
            from zoneinfo import ZoneInfo
            from dagster import SessionPartitionsDefinition, TradingSession, TimeWindow

            tz = ZoneInfo("America/New_York")

            def equity_session(day):
                # 09:30-12:00 and 13:00-16:00, with a lunch break
                return TradingSession(
                    session_key=day.isoformat(),
                    timezone="America/New_York",
                    windows=[
                        TimeWindow(
                            datetime.combine(day, time(9, 30), tzinfo=tz),
                            datetime.combine(day, time(12, 0), tzinfo=tz),
                        ),
                        TimeWindow(
                            datetime.combine(day, time(13, 0), tzinfo=tz),
                            datetime.combine(day, time(16, 0), tzinfo=tz),
                        ),
                    ],
                )

            partitions_def = SessionPartitionsDefinition(
                sessions=[equity_session(day) for day in trading_days],
                timezone="America/New_York",
            )
    """

    sessions: PublicAttr[Sequence[TradingSession]]
    timezone: PublicAttr[str]

    def __new__(
        cls,
        sessions: Sequence[TradingSession],
        timezone: str = "UTC",
    ):
        check.str_param(timezone, "timezone")
        check.sequence_param(sessions, "sessions", of_type=TradingSession)

        seen_keys: set[str] = set()
        sorted_sessions = sorted(sessions, key=lambda s: s.start_timestamp)
        for session in sorted_sessions:
            if session.session_key in seen_keys:
                raise DagsterInvalidDefinitionError(
                    f"Duplicate trading session key '{session.session_key}' in session partitions"
                    " definition. Session keys must be unique and stable."
                )
            seen_keys.add(session.session_key)

            if session.timezone != timezone:
                raise DagsterInvalidDefinitionError(
                    f"Trading session '{session.session_key}' uses timezone '{session.timezone}',"
                    f" but the SessionPartitionsDefinition timezone is '{timezone}'. All sessions"
                    " must share the definition's timezone."
                )

        # Sessions must not overlap each other on the absolute timeline. Adjacent sessions (one
        # ending exactly when the next starts) are allowed.
        for earlier, later in zip(sorted_sessions, sorted_sessions[1:]):
            if earlier.end_timestamp > later.start_timestamp:
                raise DagsterInvalidDefinitionError(
                    f"Trading sessions '{earlier.session_key}' and '{later.session_key}' have"
                    " overlapping time windows. Sessions on a trading calendar must not overlap."
                )

        return super().__new__(
            cls,
            sessions=sorted_sessions,
            timezone=timezone,
        )

    # ------------------------------------------------------------------
    # basic partition key access
    # ------------------------------------------------------------------

    @functools.cached_property
    def _sessions_by_key(self) -> dict[str, TradingSession]:
        return {session.session_key: session for session in self.sessions}

    @public
    def get_partition_keys(
        self,
        current_time: datetime | None = None,
        dynamic_partitions_store: Optional["DynamicPartitionsStore"] = None,
    ) -> Sequence[str]:
        """Returns the session keys in chronological order.

        Session partitions are defined explicitly by the trading calendar, so the result does
        not depend on the current time (the calendar itself determines which sessions exist).
        """
        return [session.session_key for session in self.sessions]

    def get_paginated_partition_keys(
        self,
        context: PartitionLoadingContext,
        limit: int,
        ascending: bool,
        cursor: str | None = None,
    ) -> PaginatedResults[str]:
        with partition_loading_context(new_ctx=context):
            partition_keys = list(self.get_partition_keys())
            return PaginatedResults.create_from_sequence(
                partition_keys, limit=limit, ascending=ascending, cursor=cursor
            )

    def get_num_partitions(self) -> int:
        return len(self.sessions)

    def get_first_partition_key(self) -> str | None:
        return self.sessions[0].session_key if self.sessions else None

    def get_last_partition_key(self) -> str | None:
        return self.sessions[-1].session_key if self.sessions else None

    def has_partition_key(self, partition_key: str) -> bool:
        return partition_key in self._sessions_by_key

    def get_partition_keys_in_range(
        self, partition_key_range: PartitionKeyRange
    ) -> Sequence[str]:
        # Reuse the base implementation, which validates both endpoints and slices the
        # chronological key list.
        return super().get_partition_keys_in_range(partition_key_range)

    def less_than(self, partition_key1: str, partition_key2: str) -> bool:
        """Returns True if partition_key1's session starts earlier than partition_key2's."""
        return (
            self._sessions_by_key[partition_key1].start_timestamp
            < self._sessions_by_key[partition_key2].start_timestamp
        )

    # ------------------------------------------------------------------
    # session / window accessors used by assets and partition mappings
    # ------------------------------------------------------------------

    @public
    def get_session_for_partition_key(self, partition_key: str) -> TradingSession:
        """The :class:`TradingSession` associated with the given partition key."""
        session = self._sessions_by_key.get(partition_key)
        if session is None:
            raise DagsterInvalidDefinitionError(
                f"Could not find a trading session with partition key '{partition_key}'."
            )
        return session

    def time_windows_for_partition_key(self, partition_key: str) -> Sequence[TimeWindow]:
        """The ordered, non-overlapping trading windows of the given session partition."""
        return self.get_session_for_partition_key(partition_key).to_public_time_windows()

    def get_partition_key_for_timestamp(
        self, timestamp: float, end_closed: bool = False
    ) -> str | None:
        """Returns the key of the session whose trading windows contain the given Unix
        timestamp.

        Time spent in a break between a session's windows, or outside any session, does not
        belong to any partition and returns ``None``. When ``end_closed`` is True, a timestamp
        exactly at a window's end is attributed to the preceding window instead of being
        treated as outside the session.
        """
        for session in self.sessions:
            for window in session.windows:
                if window.start_timestamp <= timestamp < window.end_timestamp:
                    return session.session_key
                if end_closed and timestamp == window.end_timestamp:
                    return session.session_key
            # sessions are sorted and non-overlapping; stop once we have passed the session
            if session.start_timestamp > timestamp:
                break
        return None

    def get_partition_keys_overlapping_time_window(
        self, time_window: TimeWindow
    ) -> Sequence[str]:
        """Returns the keys of every session whose trading windows overlap the given
        ``[start, end)`` interval.

        Only actual trading time is considered: an interval that covers only a lunch break or
        an overnight gap maps to no partitions.
        """
        start_ts = time_window.start.timestamp()
        end_ts = time_window.end.timestamp()
        result: list[str] = []
        for session in self.sessions:
            if session.start_timestamp >= end_ts:
                break
            if session.overlaps_window(start_ts, end_ts):
                result.append(session.session_key)
        return result

    def get_partition_subset_in_time_window(
        self, time_window: TimeWindow
    ) -> "SessionPartitionsSubset":
        from dagster._core.definitions.partitions.subset.session import (
            SessionPartitionsSubset,
        )

        return SessionPartitionsSubset(
            partitions_def=self,
            included_partition_keys=set(
                self.get_partition_keys_overlapping_time_window(time_window)
            ),
        )

    # ------------------------------------------------------------------
    # identity / serialization
    # ------------------------------------------------------------------

    @property
    def partitions_subset_class(self) -> type["PartitionsSubset"]:
        from dagster._core.definitions.partitions.subset.session import (
            SessionPartitionsSubset,
        )

        return SessionPartitionsSubset

    def get_serializable_unique_identifier(
        self, dynamic_partitions_store: Optional["DynamicPartitionsStore"] = None
    ) -> str:
        # Identity is derived from the full explicit calendar (keys + absolute window bounds +
        # timezone), so adding/removing sessions or editing a window changes the identifier,
        # while reconstructing the same calendar yields a stable identifier.
        return hashlib.sha1(self.__repr__().encode("utf-8")).hexdigest()

    def __repr__(self) -> str:
        sessions_repr = ", ".join(
            f"TradingSession(key={s.session_key!r}, windows=["
            + ", ".join(
                f"({w.start_timestamp}, {w.end_timestamp})" for w in s.windows
            )
            + "])"
            for s in self.sessions
        )
        return (
            f"SessionPartitionsDefinition(timezone={self.timezone!r}, sessions=[{sessions_repr}])"
        )

    def __hash__(self) -> int:
        return hash(tuple(self.__repr__()))

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, SessionPartitionsDefinition)
            and self.timezone == other.timezone
            and [
                (s.session_key, [(w.start_timestamp, w.end_timestamp) for w in s.windows])
                for s in self.sessions
            ]
            == [
                (s.session_key, [(w.start_timestamp, w.end_timestamp) for w in s.windows])
                for s in other.sessions
            ]
        )

    def __str__(self) -> str:
        if not self.sessions:
            return "SessionPartitionsDefinition(no sessions)"
        return (
            f"SessionPartitionsDefinition: {len(self.sessions)} session(s) in {self.timezone},"
            f" from '{self.sessions[0].session_key}' to"
            f" '{self.sessions[-1].session_key}'."
        )
