import json
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, NamedTuple

import dagster._check as check
from dagster._core.definitions.partitions.definition.partitions_definition import (
    PartitionsDefinition,
)
from dagster._core.definitions.partitions.definition.session import (
    SessionPartitionsDefinition,
)
from dagster._core.definitions.partitions.partition_key_range import PartitionKeyRange
from dagster._core.definitions.partitions.subset.partitions_subset import PartitionsSubset
from dagster._core.errors import DagsterInvalidDeserializationVersionError
from dagster._serdes import whitelist_for_serdes

if TYPE_CHECKING:
    pass


@whitelist_for_serdes
class SessionPartitionsSubset(
    PartitionsSubset,
    NamedTuple(
        "_SessionPartitionsSubset",
        [
            ("partitions_def", SessionPartitionsDefinition),
            ("included_partition_keys", set[str]),
        ],
    ),
):
    """A PartitionsSubset for a :class:`SessionPartitionsDefinition`.

    The subset stores explicit session partition keys (rather than merged time windows),
    because a session is an identity defined by the trading calendar, not a contiguous time
    range. A materialized session remains the same partition even if the calendar's window
    bounds are later edited, as long as its stable session key is unchanged.
    """

    SERIALIZATION_VERSION = 1

    def __new__(
        cls,
        partitions_def: SessionPartitionsDefinition,
        included_partition_keys: Iterable[str] | set[str],
    ):
        return super().__new__(
            cls,
            partitions_def=check.inst_param(
                partitions_def, "partitions_def", SessionPartitionsDefinition
            ),
            included_partition_keys=check.set_param(
                set(included_partition_keys), "included_partition_keys", of_type=str
            ),
        )

    @property
    def is_empty(self) -> bool:
        return len(self.included_partition_keys) == 0

    def get_partition_keys_not_in_subset(
        self, partitions_def: PartitionsDefinition
    ) -> Iterable[str]:
        return [
            key
            for key in partitions_def.get_partition_keys()
            if key not in self.included_partition_keys
        ]

    def get_partition_keys(self) -> Iterable[str]:
        # Return keys in the calendar's chronological order rather than arbitrary set order.
        return [
            key
            for key in self.partitions_def.get_partition_keys()
            if key in self.included_partition_keys
        ]

    def get_partition_key_ranges(
        self,
        partitions_def: PartitionsDefinition,
        current_time=None,
        dynamic_partitions_store=None,
        respect_bounds: bool = True,
    ) -> Sequence[PartitionKeyRange]:
        # Group consecutive (in the calendar's chronological key order) included keys into
        # ranges so the UI and backfills can represent them compactly.
        all_keys = list(partitions_def.get_partition_keys())
        ranges: list[PartitionKeyRange] = []
        range_start: str | None = None
        range_end: str | None = None
        for key in all_keys:
            if key in self.included_partition_keys:
                if range_start is None:
                    range_start = key
                range_end = key
            elif range_start is not None and range_end is not None:
                ranges.append(PartitionKeyRange(range_start, range_end))
                range_start = range_end = None

        if range_start is not None and range_end is not None:
            ranges.append(PartitionKeyRange(range_start, range_end))
        return ranges

    def with_partition_keys(
        self, partition_keys: Iterable[str], validate: bool = True
    ) -> "SessionPartitionsSubset":
        new_keys = set(partition_keys)
        if validate:
            unknown = new_keys.difference(self.partitions_def.get_partition_keys())
            if unknown:
                check.failed(
                    "Attempted to add partition keys that do not exist in the"
                    " SessionPartitionsDefinition: "
                    + ", ".join(sorted(unknown))
                )
        return SessionPartitionsSubset(
            self.partitions_def,
            self.included_partition_keys | new_keys,
        )

    def empty_subset(self) -> "SessionPartitionsSubset":
        return SessionPartitionsSubset(self.partitions_def, set())

    @classmethod
    def create_empty_subset(
        cls, partitions_def: PartitionsDefinition | None = None
    ) -> "PartitionsSubset":
        if not isinstance(partitions_def, SessionPartitionsDefinition):
            check.failed(
                "Partitions definition must be a SessionPartitionsDefinition"
            )
        return cls(partitions_def, set())

    def with_partitions_def(
        self, partitions_def: SessionPartitionsDefinition
    ) -> "SessionPartitionsSubset":
        return SessionPartitionsSubset(
            partitions_def=partitions_def,
            included_partition_keys=self.included_partition_keys,
        )

    def __and__(self, other: "PartitionsSubset") -> "PartitionsSubset":
        if not isinstance(other, SessionPartitionsSubset):
            return super().__and__(other)
        return SessionPartitionsSubset(
            self.partitions_def,
            self.included_partition_keys & other.included_partition_keys,
        )

    def __or__(self, other: "PartitionsSubset") -> "PartitionsSubset":
        if not isinstance(other, SessionPartitionsSubset):
            return super().__or__(other)
        return SessionPartitionsSubset(
            self.partitions_def,
            self.included_partition_keys | other.included_partition_keys,
        )

    def __sub__(self, other: "PartitionsSubset") -> "PartitionsSubset":
        if not isinstance(other, SessionPartitionsSubset):
            return super().__sub__(other)
        return SessionPartitionsSubset(
            self.partitions_def,
            self.included_partition_keys - other.included_partition_keys,
        )

    def __contains__(self, partition_key: str | None) -> bool:  # type: ignore[override]
        if partition_key is None:
            return False
        return partition_key in self.included_partition_keys

    def __len__(self) -> int:  # type: ignore[override]
        return len(self.included_partition_keys)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, SessionPartitionsSubset)
            and self.partitions_def == other.partitions_def
            and self.included_partition_keys == other.included_partition_keys
        )

    __hash__ = None

    def __repr__(self) -> str:
        return (
            "SessionPartitionsSubset("
            f"{self.get_partition_key_ranges(self.partitions_def)})"
        )

    def serialize(self) -> str:
        return json.dumps(
            {
                "version": self.SERIALIZATION_VERSION,
                # sort to guarantee a stable serialized form for identical subsets
                "partition_keys": sorted(self.included_partition_keys),
            }
        )

    @classmethod
    def from_serialized(
        cls,
        partitions_def: PartitionsDefinition,
        serialized: str,
    ) -> "PartitionsSubset":
        session_partitions_def = check.inst(
            partitions_def, SessionPartitionsDefinition
        )
        loaded = json.loads(serialized)

        if isinstance(loaded, list):
            # forward/back-compat: a bare list of keys
            return cls(session_partitions_def, set(loaded))

        if not isinstance(loaded, dict) or loaded.get("version") != cls.SERIALIZATION_VERSION:
            raise DagsterInvalidDeserializationVersionError(
                f"Attempted to deserialize session partition subset with version "
                f"{loaded.get('version') if isinstance(loaded, dict) else None}, but only "
                f"version {cls.SERIALIZATION_VERSION} is supported."
            )

        return cls(
            session_partitions_def,
            set(loaded["partition_keys"]),
        )

    @classmethod
    def can_deserialize(
        cls,
        partitions_def: PartitionsDefinition,
        serialized: str,
        serialized_partitions_def_unique_id: str | None,
        serialized_partitions_def_class_name: str | None,
    ) -> bool:
        if serialized_partitions_def_unique_id:
            return (
                partitions_def.get_serializable_unique_identifier()
                == serialized_partitions_def_unique_id
            )

        if (
            serialized_partitions_def_class_name
            and serialized_partitions_def_class_name
            != partitions_def.__class__.__name__
        ):
            return False

        data = json.loads(serialized)
        return isinstance(data, list) or (
            isinstance(data, dict)
            and data.get("partition_keys") is not None
            and data.get("version") == cls.SERIALIZATION_VERSION
        )
