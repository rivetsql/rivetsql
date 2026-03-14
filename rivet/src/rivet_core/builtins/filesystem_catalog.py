"""FilesystemCatalog and FilesystemSource built-in plugins.

Reads local Parquet, CSV, JSON, and IPC files via PyArrow. Only stdlib + PyArrow.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.dataset as ds
import pyarrow.ipc as ipc
import pyarrow.json as pjson
import pyarrow.parquet as pq

from rivet_core.formats import EXT_TO_FORMAT, FormatRegistry
from rivet_core.introspection import (
    CatalogNode,
    ColumnDetail,
    NodeSummary,
    ObjectMetadata,
    ObjectSchema,
)
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import CatalogPlugin, SourcePlugin
from rivet_core.strategies import ArrowMaterialization, MaterializationContext

_ARROW_TYPE_MAP: dict[str, str] = {
    "int8": "int8",
    "int16": "int16",
    "int32": "int32",
    "int64": "int64",
    "uint8": "uint8",
    "uint16": "uint16",
    "uint32": "uint32",
    "uint64": "uint64",
    "float16": "float16",
    "halffloat": "float16",
    "float32": "float32",
    "float": "float32",
    "float64": "float64",
    "double": "float64",
    "bool": "bool",
    "string": "utf8",
    "utf8": "utf8",
    "large_string": "large_utf8",
    "large_utf8": "large_utf8",
    "binary": "binary",
    "large_binary": "large_binary",
    "date32": "date32",
    "date32[day]": "date32",
    "date64": "date64",
}


def _arrow_type_name(arrow_type: pa.DataType) -> str:
    """Convert a PyArrow DataType to its canonical string name."""
    s = str(arrow_type)
    return _ARROW_TYPE_MAP.get(s, s)


def _read_table(path: Path, fmt: str, catalog_options: dict[str, Any]) -> pa.Table:
    """Read a file or directory into a PyArrow Table."""
    if path.is_dir():
        fmt_map = {"parquet": "parquet", "csv": "csv", "json": "json", "ipc": "ipc"}
        ds_fmt = fmt_map.get(fmt, fmt)
        dataset = ds.dataset(str(path), format=ds_fmt)
        return dataset.to_table()

    match fmt:
        case "parquet":
            return pq.read_table(str(path))
        case "csv":
            parse_opts = pcsv.ParseOptions(delimiter=catalog_options.get("csv_delimiter", ","))
            read_opts = pcsv.ReadOptions()
            header = catalog_options.get("csv_header", True)
            if not header:
                read_opts.autogenerate_column_names = True
            return pcsv.read_csv(str(path), parse_options=parse_opts, read_options=read_opts)
        case "json":
            return pjson.read_json(str(path))
        case "ipc":
            reader = ipc.open_file(str(path))
            return reader.read_all()
        case _:
            from rivet_core.errors import ExecutionError, plugin_error

            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Unsupported format: '{fmt}'",
                    plugin_name="filesystem",
                    plugin_type="source",
                    remediation="Use a supported format: parquet, csv, json, ipc",
                )
            )


def _read_schema_lightweight(path: Path, fmt: str, catalog_options: dict[str, Any]) -> pa.Schema:
    """Read schema without loading full data where possible."""
    if path.is_dir():
        dataset = ds.dataset(str(path), format=fmt)
        return dataset.schema

    match fmt:
        case "parquet":
            return pq.read_schema(str(path))
        case "csv":
            parse_opts = pcsv.ParseOptions(delimiter=catalog_options.get("csv_delimiter", ","))
            read_opts = pcsv.ReadOptions()
            header = catalog_options.get("csv_header", True)
            if not header:
                read_opts.autogenerate_column_names = True
            # Read just first batch for schema
            reader = pcsv.open_csv(str(path), parse_options=parse_opts, read_options=read_opts)
            return reader.schema
        case "json":
            reader = pjson.read_json(str(path))
            return reader.schema
        case "ipc":
            reader = ipc.open_file(str(path))
            return reader.schema
        case _:
            from rivet_core.errors import ExecutionError, plugin_error

            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Unsupported format for schema inference: '{fmt}'",
                    plugin_name="filesystem",
                    plugin_type="catalog",
                    remediation="Use a supported format: parquet, csv, json, ipc",
                )
            )


def _resolve_path(catalog: Catalog, table: str | None = None) -> Path:
    """Resolve the filesystem path from catalog options and optional table name.

    If the exact path doesn't exist, searches the base directory for a file
    whose stem matches the table name (e.g. 'raw_orders' finds 'raw_orders.csv').
    """
    base = Path(catalog.options["path"])
    if not table:
        return base
    path = base / table
    if path.exists():
        return path
    # Stem-based fallback: search for a file matching the table name
    if base.is_dir():
        for entry in base.iterdir():
            if entry.is_file() and _file_stem_to_table_name(entry) == table:
                return entry
    return path  # Return original path — caller will get a clear FileNotFoundError


def _file_stem_to_table_name(file_path: Path) -> str:
    """Convert a file path to a table name using reverse naming (stem only)."""
    return file_path.stem


class FilesystemCatalogPlugin(CatalogPlugin):
    """Built-in catalog plugin for local filesystem data files."""

    type = "filesystem"
    required_options = ["path"]
    optional_options: dict[str, Any] = {
        "format": "parquet",
        "csv_delimiter": ",",
        "csv_header": True,
    }
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        """Validate filesystem catalog options, rejecting unrecognized keys."""
        from rivet_core.errors import PluginValidationError, RivetError

        if "path" not in options:
            raise PluginValidationError(
                RivetError(
                    code="RVT-201",
                    message="FilesystemCatalog requires 'path' option.",
                    remediation="Add 'path' to catalog options pointing to a file or directory.",
                )
            )

        recognized = (
            set(self.required_options) | set(self.optional_options) | set(self.credential_options)
        )
        for key in options:
            if key not in recognized:
                raise PluginValidationError(
                    RivetError(
                        code="RVT-201",
                        message=f"Unknown option '{key}' for filesystem catalog.",
                        context={"plugin": "filesystem", "option": key},
                        remediation=f"Valid options: {', '.join(sorted(recognized))}",
                    )
                )

    def test_connection(self, catalog: Catalog) -> None:
        """Verify the configured base path exists and is accessible.

        Raises ``ExecutionError`` with structured error info if the path
        does not exist.
        """
        from rivet_core.errors import ExecutionError, plugin_error

        base = Path(catalog.options["path"])
        if not base.exists():
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Filesystem catalog path does not exist: {base}",
                    plugin_name="filesystem",
                    plugin_type="catalog",
                    remediation="Verify the 'path' option points to an existing file or directory.",
                    path=str(base),
                )
            )

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        return Catalog(name=name, type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        fmt = options.get("format", "parquet")
        return f"{logical_name}.{fmt}"

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        base = Path(catalog.options["path"])
        if not base.exists():
            return []
        if base.is_file():
            stat = base.stat()
            fmt = FormatRegistry.resolve_format(catalog.options.get("format"), path=base)
            table_name = _file_stem_to_table_name(base)
            return [
                CatalogNode(
                    name=table_name,
                    node_type="file",
                    path=[table_name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=stat.st_size,
                        format=fmt,
                        last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                        owner=None,
                        comment=None,
                    ),
                )
            ]
        nodes: list[CatalogNode] = []
        for entry in sorted(base.iterdir()):
            if entry.is_file() and entry.suffix.lower() in EXT_TO_FORMAT:
                stat = entry.stat()
                fmt = FormatRegistry.resolve_format(catalog.options.get("format"), path=entry)
                table_name = _file_stem_to_table_name(entry)
                nodes.append(
                    CatalogNode(
                        name=table_name,
                        node_type="file",
                        path=[table_name],
                        is_container=False,
                        children_count=None,
                        summary=NodeSummary(
                            row_count=None,
                            size_bytes=stat.st_size,
                            format=fmt,
                            last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                            owner=None,
                            comment=None,
                        ),
                    )
                )
        return nodes

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        """Lazy single-level listing for filesystem catalogs.

        - path=[] → list files/directories in the base path
        - path=[file_name] → list columns of that file via schema inference
        """
        depth = len(path)
        base = Path(catalog.options["path"])

        if depth == 0:
            if not base.exists():
                return []
            if base.is_file():
                stat = base.stat()
                fmt = FormatRegistry.resolve_format(catalog.options.get("format"), path=base)
                table_name = _file_stem_to_table_name(base)
                return [
                    CatalogNode(
                        name=table_name,
                        node_type="table",
                        path=[table_name],
                        is_container=False,
                        children_count=None,
                        summary=NodeSummary(
                            row_count=None,
                            size_bytes=stat.st_size,
                            format=fmt,
                            last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                            owner=None,
                            comment=None,
                        ),
                    )
                ]
            nodes: list[CatalogNode] = []
            for entry in sorted(base.iterdir()):
                if entry.is_dir():
                    nodes.append(
                        CatalogNode(
                            name=entry.name,
                            node_type="schema",
                            path=[entry.name],
                            is_container=True,
                            children_count=None,
                            summary=None,
                        )
                    )
                elif entry.is_file() and entry.suffix.lower() in EXT_TO_FORMAT:
                    stat = entry.stat()
                    fmt = FormatRegistry.resolve_format(catalog.options.get("format"), path=entry)
                    table_name = _file_stem_to_table_name(entry)
                    nodes.append(
                        CatalogNode(
                            name=table_name,
                            node_type="table",
                            path=[table_name],
                            is_container=False,
                            children_count=None,
                            summary=NodeSummary(
                                row_count=None,
                                size_bytes=stat.st_size,
                                format=fmt,
                                last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                                owner=None,
                                comment=None,
                            ),
                        )
                    )
            return nodes

        if depth == 1:
            file_name = path[0]
            file_path = _resolve_path(catalog, file_name)
            if not file_path.exists():
                return []
            fmt = FormatRegistry.resolve_format(catalog.options.get("format"), path=file_path)
            try:
                schema = _read_schema_lightweight(file_path, fmt, catalog.options)
            except Exception:
                return []
            return [
                CatalogNode(
                    name=field.name,
                    node_type="column",
                    path=[file_name, field.name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format=_arrow_type_name(field.type),
                        last_modified=None,
                        owner=None,
                        comment=None,
                    ),
                )
                for field in schema
            ]

        return []

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        path = _resolve_path(catalog, table)
        fmt = FormatRegistry.resolve_format(catalog.options.get("format"), path=path)
        schema = _read_schema_lightweight(path, fmt, catalog.options)
        columns = [
            ColumnDetail(
                name=field.name,
                type=_arrow_type_name(field.type),
                native_type=str(field.type),
                nullable=field.nullable,
                default=None,
                comment=None,
                is_primary_key=False,
                is_partition_key=False,
            )
            for field in schema
        ]
        return ObjectSchema(
            path=[str(path)],
            node_type="file",
            columns=columns,
            primary_key=None,
            comment=None,
        )

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        path = _resolve_path(catalog, table)
        if not path.exists():
            base = Path(catalog.options["path"])
            if base.is_dir():
                for entry in base.iterdir():
                    if entry.is_file() and _file_stem_to_table_name(entry) == table:
                        path = entry
                        break
        if not path.exists():
            return None
        fmt = FormatRegistry.resolve_format(catalog.options.get("format"), path=path)
        stat = path.stat()
        row_count: int | None = None
        if fmt == "parquet" and path.is_file():
            try:
                pf = pq.ParquetFile(str(path))
                row_count = pf.metadata.num_rows
            except Exception:
                pass
        return ObjectMetadata(
            path=[str(path)],
            node_type="file",
            row_count=row_count,
            size_bytes=stat.st_size,
            last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
            created_at=datetime.fromtimestamp(os.path.getctime(str(path)), tz=UTC),
            format=fmt,
            compression=None,
            owner=None,
            comment=None,
            location=str(path),
            column_statistics=[],
            partitioning=None,
            properties={},
        )


class FilesystemSource(SourcePlugin):
    """Built-in source plugin reading files from a FilesystemCatalog."""

    catalog_type = "filesystem"

    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None = None) -> Material:
        # Resolve path: joint.path takes precedence, then joint.table, then catalog base path
        if joint.path:
            path = Path(joint.path)
            if not path.is_absolute():
                path = Path(catalog.options["path"]) / path
        elif joint.table:
            path = _resolve_path(catalog, joint.table)
        else:
            path = Path(catalog.options["path"])

        fmt = FormatRegistry.resolve_format(catalog.options.get("format"), path=path)
        table = _read_table(path, fmt, catalog.options)

        ref = ArrowMaterialization().materialize(
            table, MaterializationContext(joint_name=joint.name, strategy_name="arrow", options={})
        )
        schema_dict = {field.name: _arrow_type_name(field.type) for field in table.schema}
        return Material(
            name=joint.name,
            catalog=catalog.name,
            table=joint.table,
            schema=schema_dict,
            state="materialized",
            materialized_ref=ref,
        )
