"""Write strategy data models for sink joints."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

VALID_WRITE_STRATEGY_TYPES = frozenset(
    {"append", "replace", "truncate_insert", "merge", "delete_insert", "incremental_append", "scd2"}
)


@dataclass(frozen=True)
class WriteStrategy:
    """Declares how data is written to a sink target.

    type: one of append, replace, truncate_insert, merge, delete_insert,
          incremental_append, scd2
    config: strategy-specific options (key columns, watermark config, etc.)
    """

    type: str
    config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.type not in VALID_WRITE_STRATEGY_TYPES:
            raise ValueError(
                f"Invalid write strategy type '{self.type}'. "
                f"Must be one of: {sorted(VALID_WRITE_STRATEGY_TYPES)}"
            )
