"""DuckDB filesystem sink plugin: write materialized data to local filesystem files."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.plugins import SinkPlugin

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Material

FILESYSTEM_SUPPORTED_STRATEGIES = frozenset({"append", "replace", "partition"})

_FORMAT_EXTENSIONS = {
    "parquet": ".parquet",
    "csv": ".csv",
    "json": ".json",
}

_EXT_TO_FORMAT = {
    ".parquet": "parquet",
    ".pq": "parquet",
    ".csv": "csv",
    ".tsv": "csv",
    ".json": "json",
    ".jsonl": "json",
    ".ndjson": "json",
}


def _detect_format(path: Path, catalog_options: dict[str, Any]) -> str:
    fmt = catalog_options.get("format")
    if fmt:
        return fmt  # type: ignore[no-any-return]
    ext = path.suffix.lower()
    return _EXT_TO_FORMAT.get(ext, "parquet")


def _cast_dict_columns(table: Any) -> Any:
    """Cast dictionary-encoded columns back to their value type."""
    import pyarrow as pa

    new_cols = []
    new_fields = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_dictionary(col.type):
            col = col.cast(col.type.value_type)
            field = field.with_type(col.type)
        new_cols.append(col)
        new_fields.append(field)
    return pa.table({f.name: c for f, c in zip(new_fields, new_cols)})


def _write_arrow_to_file(table: Any, path: Path, fmt: str) -> None:
    """Write a PyArrow table to a file in the given format."""
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    table = _cast_dict_columns(table)
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        pq.write_table(table, str(path))
    elif fmt == "csv":
        pcsv.write_csv(table, str(path))
    elif fmt == "json":
        # PyArrow doesn't have a direct JSON writer; use pandas or manual approach
        import json
        rows = table.to_pydict()
        keys = list(rows.keys())
        n = len(rows[keys[0]]) if keys else 0
        with open(str(path), "w") as f:
            for i in range(n):
                record = {k: rows[k][i] for k in keys}
                f.write(json.dumps(record) + "\n")
    else:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                f"Unsupported format '{fmt}' for filesystem sink.",
                plugin_name="rivet_duckdb",
                plugin_type="sink",
                remediation="Supported formats: parquet, csv, json",
                format=fmt,
            )
        )


class FilesystemSink(SinkPlugin):
    """Sink plugin for filesystem catalog type.

    Writes materialized data to local filesystem files using PyArrow.
    Supports append, replace, and partition write strategies.
    """

    catalog_type = "filesystem"
    supported_strategies = FILESYSTEM_SUPPORTED_STRATEGIES

    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        if strategy not in FILESYSTEM_SUPPORTED_STRATEGIES:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Unsupported write strategy '{strategy}' for filesystem sink.",
                    plugin_name="rivet_duckdb",
                    plugin_type="sink",
                    remediation=f"Supported strategies: {', '.join(sorted(FILESYSTEM_SUPPORTED_STRATEGIES))}",
                    strategy=strategy,
                    catalog=catalog.name,
                )
            )

        arrow_table = material.to_arrow()
        base_path = Path(catalog.options["path"])
        fmt = _detect_format(base_path, catalog.options)

        try:
            if strategy == "replace":
                _do_replace(base_path, arrow_table, fmt, joint)
            elif strategy == "append":
                _do_append(base_path, arrow_table, fmt, joint)
            elif strategy == "partition":
                _do_partition(base_path, arrow_table, fmt, joint)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Filesystem sink write failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="sink",
                    remediation="Check that the output path is writable and the data schema is valid.",
                    strategy=strategy,
                    path=str(base_path),
                )
            ) from exc


def _resolve_file_path(base: Path, joint: Joint, fmt: str) -> Path:
    """Resolve the single output file path."""
    ext = _FORMAT_EXTENSIONS.get(fmt, ".parquet")
    if base.is_dir() or not base.suffix:
        table_name = joint.table or joint.name
        return base / f"{table_name}{ext}"
    return base


def _do_replace(base: Path, table: Any, fmt: str, joint: Joint) -> None:
    """Replace: overwrite the output file entirely."""
    out_path = _resolve_file_path(base, joint, fmt)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _write_arrow_to_file(table, out_path, fmt)


def _do_append(base: Path, table: Any, fmt: str, joint: Joint) -> None:
    """Append: read existing file (if any), concatenate, write back."""
    import pyarrow as pa

    out_path = _resolve_file_path(base, joint, fmt)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        existing = _read_file(out_path, fmt)
        combined = pa.concat_tables([existing, table])
    else:
        combined = table

    _write_arrow_to_file(combined, out_path, fmt)


def _do_partition(base: Path, table: Any, fmt: str, joint: Joint) -> None:
    """Partition: write each partition value to a separate subdirectory/file.

    Replaces existing data for partitions present in the new data.
    If no partition_by columns are configured, falls back to replace.
    """
    ws = getattr(joint, "write_strategy_config", None) or {}
    partition_cols = ws.get("partition_by") or ws.get("partition_columns")

    if not partition_cols:
        # No partition columns: fall back to replace
        _do_replace(base, table, fmt, joint)
        return

    if isinstance(partition_cols, str):
        partition_cols = [partition_cols]


    base.mkdir(parents=True, exist_ok=True)
    ext = _FORMAT_EXTENSIONS.get(fmt, ".parquet")

    # Get unique partition values
    partition_values = _get_partition_values(table, partition_cols)

    for pv in partition_values:
        # Build filter mask
        mask = _build_partition_mask(table, partition_cols, pv)
        partition_data = table.filter(mask)

        # Build partition directory path: base/col=val/col2=val2/...
        part_dir = base
        for col, val in zip(partition_cols, pv):
            part_dir = part_dir / f"{col}={val}"
        part_dir.mkdir(parents=True, exist_ok=True)

        out_file = part_dir / f"data{ext}"
        _write_arrow_to_file(partition_data, out_file, fmt)


def _get_partition_values(table: Any, partition_cols: list[str]) -> list[tuple]:  # type: ignore[type-arg]
    """Get unique combinations of partition column values."""

    cols = [table.column(c).to_pylist() for c in partition_cols]
    n = len(cols[0]) if cols else 0
    seen: set[tuple] = set()  # type: ignore[type-arg]
    result: list[tuple] = []  # type: ignore[type-arg]
    for i in range(n):
        pv = tuple(cols[j][i] for j in range(len(partition_cols)))
        if pv not in seen:
            seen.add(pv)
            result.append(pv)
    return result


def _build_partition_mask(table: Any, partition_cols: list[str], values: tuple) -> Any:  # type: ignore[type-arg]
    """Build a boolean mask for rows matching the given partition values."""
    import pyarrow.compute as pc

    mask = None
    for col, val in zip(partition_cols, values):
        col_arr = table.column(col)
        eq = pc.equal(col_arr, val)
        mask = eq if mask is None else pc.and_(mask, eq)
    return mask


def _read_file(path: Path, fmt: str) -> Any:
    """Read an existing file into a PyArrow table."""
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    if fmt == "parquet":
        return pq.read_table(str(path))
    elif fmt == "csv":
        return pcsv.read_csv(str(path))
    elif fmt == "json":
        import pyarrow.json as pjson
        return pjson.read_json(str(path))
    else:
        raise ValueError(f"Unsupported format for read: {fmt!r}")
