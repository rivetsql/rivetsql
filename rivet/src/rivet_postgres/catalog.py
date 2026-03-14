"""PostgreSQL catalog plugin for Rivet."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from rivet_core.errors import PluginValidationError, plugin_error
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin
from rivet_core.type_parser import parse_type

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema

_REQUIRED_OPTIONS = ["host", "database"]
_CREDENTIAL_OPTIONS = ["user", "password"]
_OPTIONAL_OPTIONS: dict[str, Any] = {
    "port": 5432,
    "schema": "public",
    "ssl_mode": "prefer",
    "ssl_cert": None,
    "ssl_key": None,
    "ssl_root_cert": None,
    "read_only": False,
}
_KNOWN_OPTIONS = (
    set(_REQUIRED_OPTIONS) | set(_CREDENTIAL_OPTIONS) | set(_OPTIONAL_OPTIONS) | {"table_map"}
)
_SSL_CERT_OPTIONS = ("ssl_cert", "ssl_key", "ssl_root_cert")
_SSL_MODES_REQUIRING_ROOT_CERT = {"verify-ca", "verify-full"}

# Map PostgreSQL native types to Arrow type names
_PG_TO_ARROW: dict[str, str] = {
    "bigint": "int64",
    "int8": "int64",
    "integer": "int32",
    "int": "int32",
    "int4": "int32",
    "smallint": "int16",
    "int2": "int16",
    "real": "float32",
    "float4": "float32",
    "double precision": "float64",
    "float8": "float64",
    "numeric": "float64",
    "decimal": "float64",
    "boolean": "bool",
    "bool": "bool",
    "text": "large_utf8",
    "varchar": "large_utf8",
    "character varying": "large_utf8",
    "character": "large_utf8",
    "char": "large_utf8",
    "bytea": "large_binary",
    "date": "date32",
    "time without time zone": "time64[us]",
    "time": "time64[us]",
    "timestamp without time zone": "timestamp[us]",
    "timestamp": "timestamp[us]",
    "timestamp with time zone": "timestamp[us, UTC]",
    "timestamptz": "timestamp[us, UTC]",
    "interval": "duration[us]",
    "json": "large_utf8",
    "jsonb": "large_utf8",
    "uuid": "large_utf8",
    "oid": "uint32",
}


class PostgresCatalogPlugin(CatalogPlugin):
    type = "postgres"
    required_options: list[str] = _REQUIRED_OPTIONS
    optional_options: dict[str, Any] = _OPTIONAL_OPTIONS
    credential_options: list[str] = _CREDENTIAL_OPTIONS

    def validate(self, options: dict[str, Any]) -> None:
        for key in options:
            if key not in _KNOWN_OPTIONS:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for postgres catalog.",
                        plugin_name="rivet_postgres",
                        plugin_type="catalog",
                        remediation=(
                            f"Valid options: {', '.join(sorted(_KNOWN_OPTIONS - {'table_map'}))}"
                        ),
                        option=key,
                    )
                )
        for key in _REQUIRED_OPTIONS + _CREDENTIAL_OPTIONS:
            if key not in options:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Missing required option '{key}' for postgres catalog.",
                        plugin_name="rivet_postgres",
                        plugin_type="catalog",
                        remediation=f"Provide '{key}' in the catalog options.",
                        missing_option=key,
                    )
                )
        for cert_opt in _SSL_CERT_OPTIONS:
            path = options.get(cert_opt)
            if path is not None and not os.path.exists(path):
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"SSL cert path for '{cert_opt}' does not exist: {path}",
                        plugin_name="rivet_postgres",
                        plugin_type="catalog",
                        remediation=f"Ensure the file at '{path}' exists and is readable.",
                        option=cert_opt,
                        path=path,
                    )
                )
        ssl_mode = options.get("ssl_mode", _OPTIONAL_OPTIONS["ssl_mode"])
        if ssl_mode in _SSL_MODES_REQUIRING_ROOT_CERT and not options.get("ssl_root_cert"):
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"ssl_root_cert is required when ssl_mode is '{ssl_mode}'.",
                    plugin_name="rivet_postgres",
                    plugin_type="catalog",
                    remediation="Provide 'ssl_root_cert' with the path to the CA certificate file.",
                    ssl_mode=ssl_mode,
                )
            )

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        return Catalog(name=name, type="postgres", options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        schema = options.get("schema", "public")
        return f"{schema}.{logical_name}"

    def _connect(self, catalog: Catalog) -> Any:
        import psycopg

        from rivet_core.errors import ExecutionError, plugin_error
        from rivet_postgres.errors import classify_pg_error

        opts = catalog.options
        conninfo = (
            f"host={opts['host']} "
            f"port={opts.get('port', 5432)} "
            f"dbname={opts['database']} "
            f"user={opts['user']} "
            f"password={opts['password']}"
        )
        ssl_mode = opts.get("ssl_mode", "prefer")
        conninfo += f" sslmode={ssl_mode}"
        if opts.get("ssl_cert"):
            conninfo += f" sslcert={opts['ssl_cert']}"
        if opts.get("ssl_key"):
            conninfo += f" sslkey={opts['ssl_key']}"
        if opts.get("ssl_root_cert"):
            conninfo += f" sslrootcert={opts['ssl_root_cert']}"
        try:
            return psycopg.connect(conninfo)
        except Exception as exc:
            code, message, remediation = classify_pg_error(exc, plugin_type="catalog")
            raise ExecutionError(
                plugin_error(
                    code,
                    message,
                    plugin_name="rivet_postgres",
                    plugin_type="catalog",
                    remediation=remediation,
                    host=opts.get("host"),
                    database=opts.get("database"),
                )
            ) from exc

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        from rivet_core.introspection import CatalogNode, NodeSummary

        schema = catalog.options.get("schema", "public")
        conn = self._connect(catalog)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_schema, table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_type IN ('BASE TABLE', 'VIEW')
                    ORDER BY table_schema, table_name
                    """,
                    (schema,),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        nodes = []
        for table_schema, table_name, table_type in rows:
            node_type = "view" if table_type == "VIEW" else "table"
            nodes.append(
                CatalogNode(
                    name=table_name,
                    node_type=node_type,
                    path=[catalog.name, table_schema, table_name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format="postgres",
                        last_modified=None,
                        owner=None,
                        comment=None,
                    ),
                )
            )
        return nodes

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        # Parse schema.table or just table
        parts = table.split(".", 1)
        if len(parts) == 2:
            schema_name, table_name = parts
        else:
            schema_name = catalog.options.get("schema", "public")
            table_name = parts[0]

        conn = self._connect(catalog)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        CASE WHEN kcu.column_name IS NOT NULL THEN true ELSE false END AS is_pk
                    FROM information_schema.columns c
                    LEFT JOIN information_schema.table_constraints tc
                        ON tc.table_schema = c.table_schema
                       AND tc.table_name = c.table_name
                       AND tc.constraint_type = 'PRIMARY KEY'
                    LEFT JOIN information_schema.key_column_usage kcu
                        ON kcu.constraint_name = tc.constraint_name
                       AND kcu.table_schema = c.table_schema
                       AND kcu.table_name = c.table_name
                       AND kcu.column_name = c.column_name
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position
                    """,
                    (schema_name, table_name),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        columns = []
        pk_cols = []
        for col_name, data_type, is_nullable, col_default, is_pk in rows:
            nullable = is_nullable == "YES"
            if is_pk:
                pk_cols.append(col_name)
            columns.append(
                ColumnDetail(
                    name=col_name,
                    type=parse_type(data_type, _PG_TO_ARROW),
                    native_type=data_type,
                    nullable=nullable,
                    default=str(col_default) if col_default is not None else None,
                    comment=None,
                    is_primary_key=bool(is_pk),
                    is_partition_key=False,
                )
            )

        return ObjectSchema(
            path=[catalog.name, schema_name, table_name],
            node_type="table",
            columns=columns,
            primary_key=pk_cols if pk_cols else None,
            comment=None,
        )

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        from rivet_core.introspection import ObjectMetadata

        parts = table.split(".", 1)
        if len(parts) == 2:
            schema_name, table_name = parts
        else:
            schema_name = catalog.options.get("schema", "public")
            table_name = parts[0]

        conn = self._connect(catalog)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        s.n_live_tup,
                        pg_total_relation_size(c.oid),
                        obj_description(c.oid, 'pg_class'),
                        pg_get_userbyid(c.relowner)
                    FROM pg_stat_user_tables s
                    JOIN pg_class c ON c.relname = s.relname
                    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
                    WHERE s.schemaname = %s AND s.relname = %s
                    """,
                    (schema_name, table_name),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if row is None:
            return None

        n_live_tup, total_size, comment, owner = row
        return ObjectMetadata(
            path=[catalog.name, schema_name, table_name],
            node_type="table",
            row_count=n_live_tup,
            size_bytes=total_size,
            last_modified=None,
            created_at=None,
            format="postgres",
            compression=None,
            owner=owner,
            comment=comment,
            location=f"postgres://{catalog.options['host']}/{catalog.options['database']}",
            column_statistics=[],
            partitioning=None,
        )

    def test_connection(self, catalog: Catalog) -> None:
        """Lightweight PostgreSQL connectivity check via ``SELECT 1``.

        Faster than the base-class fallback (which calls ``list_tables``),
        because it avoids querying ``information_schema``.

        Raises ``ExecutionError`` with structured error info on failure.
        """
        from rivet_core.errors import ExecutionError

        conn = self._connect(catalog)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception as exc:
            from rivet_postgres.errors import classify_pg_error

            code, message, remediation = classify_pg_error(exc, plugin_type="catalog")
            raise ExecutionError(
                plugin_error(
                    code,
                    message,
                    plugin_name="rivet_postgres",
                    plugin_type="catalog",
                    remediation=remediation,
                    host=catalog.options.get("host"),
                    database=catalog.options.get("database"),
                )
            ) from exc
        finally:
            conn.close()

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        """Lazy single-level listing for PostgreSQL catalogs.

        - path=[] → list schemas
        - path=[schema] → list tables/views in that schema
        - path=[schema, table] → list columns via get_schema()
        """
        from rivet_core.introspection import CatalogNode, NodeSummary

        depth = len(path)

        if depth == 0:
            # Level 0: list schemas
            conn = self._connect(catalog)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT schema_name
                        FROM information_schema.schemata
                        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                        ORDER BY schema_name
                        """
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()
            return [
                CatalogNode(
                    name=schema_name,
                    node_type="schema",
                    path=[schema_name],
                    is_container=True,
                    children_count=None,
                    summary=None,
                )
                for (schema_name,) in rows
            ]

        if depth == 1:
            # Level 1: list tables/views in a schema
            schema_name = path[0]
            conn = self._connect(catalog)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema = %s
                          AND table_type IN ('BASE TABLE', 'VIEW')
                        ORDER BY table_name
                        """,
                        (schema_name,),
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()
            return [
                CatalogNode(
                    name=table_name,
                    node_type="view" if table_type == "VIEW" else "table",
                    path=[schema_name, table_name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format="postgres",
                        last_modified=None,
                        owner=None,
                        comment=None,
                    ),
                )
                for table_name, table_type in rows
            ]

        if depth == 2:
            # Level 2: list columns of a table
            schema_name, table_name = path[0], path[1]
            qualified = f"{schema_name}.{table_name}"
            try:
                schema = self.get_schema(catalog, qualified)
            except Exception:
                return []
            return [
                CatalogNode(
                    name=col.name,
                    node_type="column",
                    path=[schema_name, table_name, col.name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format=col.type,
                        last_modified=None,
                        owner=None,
                        comment=None,
                    ),
                )
                for col in schema.columns
            ]

        return []
