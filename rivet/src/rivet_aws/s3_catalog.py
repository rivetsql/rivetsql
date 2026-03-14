"""S3 catalog plugin for Rivet."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema

_REQUIRED_OPTIONS = ["bucket"]
_OPTIONAL_OPTIONS: dict[str, Any] = {
    "prefix": "",
    "region": "us-east-1",
    "endpoint_url": None,
    "format": "parquet",
    "path_style_access": False,
}
_CREDENTIAL_OPTIONS = [
    "access_key_id",
    "secret_access_key",
    "session_token",
    "profile",
    "role_arn",
    "role_session_name",
    "web_identity_token_file",
    "credential_cache",
    "auth_type",
]
_VALID_AUTH_TYPES = frozenset(
    {
        "iam_keys",
        "profile",
        "assume_role",
        "web_identity",
        "default",
    }
)

# Maps auth_type → which credential options are relevant
_CREDENTIAL_GROUPS: dict[str, list[str]] = {
    "iam_keys": ["access_key_id", "secret_access_key"],
    "profile": ["profile"],
    "assume_role": ["role_arn"],
    "web_identity": ["role_arn", "web_identity_token_file"],
    "default": [],
}

# Well-known env var names for credential hints
_ENV_VAR_HINTS: dict[str, str] = {
    "access_key_id": "AWS_ACCESS_KEY_ID",
    "secret_access_key": "AWS_SECRET_ACCESS_KEY",
    "session_token": "AWS_SESSION_TOKEN",
    "profile": "AWS_PROFILE",
    "role_arn": "AWS_ROLE_ARN",
    "web_identity_token_file": "AWS_WEB_IDENTITY_TOKEN_FILE",
}

_VALID_FORMATS = {"parquet", "csv", "json", "orc", "delta"}
_RECOGNIZED_EXTENSIONS = {".parquet", ".csv", ".json", ".ipc", ".orc"}
_KNOWN_OPTIONS = (
    set(_REQUIRED_OPTIONS)
    | set(_OPTIONAL_OPTIONS)
    | set(_CREDENTIAL_OPTIONS)
    | {"table_map", "_credential_resolver_factory"}
)


def _credential_resolver_factory(options: dict[str, Any], region: str) -> Any:
    """Create an AWSCredentialResolver from catalog options."""
    from rivet_aws.credentials import AWSCredentialResolver

    return AWSCredentialResolver(options, region)


def _raise_if_s3_client_error(exc: Exception, bucket: str) -> None:
    """Re-raise as mapped ExecutionError if exc wraps a botocore ClientError."""
    cause = exc.__cause__ if exc.__cause__ else exc
    if isinstance(cause, ClientError):
        from rivet_aws.errors import handle_s3_error

        raise handle_s3_error(cause, bucket=bucket, action="s3:ListBucket") from cause
    # Also check if the exception itself carries an error response (ArrowInvalid wrapping)
    for arg in getattr(exc, "args", ()):
        if isinstance(arg, str) and "NoSuchBucket" in arg:
            from rivet_aws.errors import handle_s3_error

            raise handle_s3_error(
                ClientError(
                    {"Error": {"Code": "NoSuchBucket", "Message": str(exc)}}, "ListObjects"
                ),
                bucket=bucket,
                action="s3:ListBucket",
            ) from exc
        if isinstance(arg, str) and "AccessDenied" in arg:
            from rivet_aws.errors import handle_s3_error

            raise handle_s3_error(
                ClientError(
                    {"Error": {"Code": "AccessDenied", "Message": str(exc)}}, "ListObjects"
                ),
                bucket=bucket,
                action="s3:ListBucket",
            ) from exc


def _build_s3fs(options: dict[str, Any]) -> Any:
    """Build a pyarrow S3FileSystem from catalog options."""
    import pyarrow.fs as pafs

    kwargs: dict[str, Any] = {}
    region = options.get("region", _OPTIONAL_OPTIONS["region"])
    if region:
        kwargs["region"] = region

    # Use AWSCredentialResolver if available via factory
    factory = options.get("_credential_resolver_factory")
    if factory:
        try:
            resolver = factory(options, region)
            creds = resolver.resolve()
            kwargs["access_key"] = creds.access_key_id
            kwargs["secret_key"] = creds.secret_access_key
            if creds.session_token:
                kwargs["session_token"] = creds.session_token
        except Exception:
            pass  # Fall through to direct credential extraction
    else:
        access_key = options.get("access_key_id")
        secret_key = options.get("secret_access_key")
        if access_key and secret_key:
            kwargs["access_key"] = access_key
            kwargs["secret_key"] = secret_key
            session_token = options.get("session_token")
            if session_token:
                kwargs["session_token"] = session_token

        role_arn = options.get("role_arn")
        if role_arn and not (access_key and secret_key):
            kwargs["role_arn"] = role_arn
            session_name = options.get("role_session_name", "rivet-session")
            if session_name:
                kwargs["session_name"] = session_name

    endpoint_url = options.get("endpoint_url")
    if endpoint_url:
        # Strip scheme for endpoint_override
        endpoint = endpoint_url
        for scheme in ("https://", "http://"):
            if endpoint.startswith(scheme):
                endpoint = endpoint[len(scheme) :]
                break
        kwargs["endpoint_override"] = endpoint
        if endpoint_url.startswith("http://"):
            kwargs["scheme"] = "http"

    return pafs.S3FileSystem(**kwargs)


_HIVE_PARTITION_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*=.+$")


def _is_hive_partition(name: str) -> bool:
    """Check if a directory name looks like a Hive-style partition (e.g. year=2024)."""
    return bool(_HIVE_PARTITION_RE.match(name))


def _detect_partition_columns(fs: Any, dir_path: str) -> list[str]:
    """Detect Hive-style partition column names from immediate children of a directory."""
    import pyarrow.fs as pafs

    selector = pafs.FileSelector(dir_path, allow_not_found=True, recursive=False)
    try:
        children = fs.get_file_info(selector)
    except Exception:
        return []
    cols = []
    for child in children:
        if child.type == pafs.FileType.Directory and _is_hive_partition(child.base_name):
            col_name = child.base_name.split("=", 1)[0]
            if col_name not in cols:
                cols.append(col_name)
    return cols


def _arrow_type_str(arrow_type: Any) -> str:
    return str(arrow_type)


class S3CatalogPlugin(CatalogPlugin):
    type = "s3"
    required_options: list[str] = _REQUIRED_OPTIONS
    optional_options: dict[str, Any] = _OPTIONAL_OPTIONS
    credential_options: list[str] = _CREDENTIAL_OPTIONS
    credential_groups: dict[str, list[str]] = _CREDENTIAL_GROUPS
    env_var_hints: dict[str, str] = _ENV_VAR_HINTS

    def validate(self, options: dict[str, Any]) -> None:
        for key in options:
            if key not in _KNOWN_OPTIONS:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for s3 catalog.",
                        plugin_name="rivet_aws",
                        plugin_type="catalog",
                        remediation=f"Valid options: {', '.join(sorted(_KNOWN_OPTIONS - {'table_map', '_credential_resolver_factory'}))}",
                    )
                )
        for key in _REQUIRED_OPTIONS:
            if not options.get(key):
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Missing required option '{key}' for s3 catalog.",
                        plugin_name="rivet_aws",
                        plugin_type="catalog",
                        remediation=f"Provide '{key}' in the catalog options.",
                        missing_option=key,
                    )
                )
        fmt = options.get("format", _OPTIONAL_OPTIONS["format"])
        if fmt not in _VALID_FORMATS:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Invalid format '{fmt}' for s3 catalog.",
                    plugin_name="rivet_aws",
                    plugin_type="catalog",
                    remediation=f"Valid formats: {', '.join(sorted(_VALID_FORMATS))}",
                    format=fmt,
                )
            )
        auth_type = options.get("auth_type")
        if auth_type is not None and auth_type not in _VALID_AUTH_TYPES:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Invalid auth_type '{auth_type}' for s3 catalog.",
                    plugin_name="rivet_aws",
                    plugin_type="catalog",
                    remediation=f"Valid auth_type values: {', '.join(sorted(_VALID_AUTH_TYPES))}",
                    auth_type=auth_type,
                )
            )

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        opts = {**options, "_credential_resolver_factory": _credential_resolver_factory}
        return Catalog(name=name, type="s3", options=opts)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        bucket = options["bucket"]
        prefix = options.get("prefix", "")
        # Detect format from file extension if present, otherwise use catalog option
        suffix = logical_name
        dot_idx = logical_name.rfind(".")
        if dot_idx < 0 or logical_name[dot_idx:].lower() not in _RECOGNIZED_EXTENSIONS:
            fmt = options.get("format", _OPTIONAL_OPTIONS["format"])
            if fmt != "delta":
                suffix = f"{logical_name}.{fmt}"
        path = f"{prefix}/{suffix}" if prefix else suffix
        return f"s3://{bucket}/{path}"

    # ── Introspection ─────────────────────────────────────────────────

    def test_connection(self, catalog: Catalog) -> None:
        """Lightweight S3 connectivity check via HeadBucket."""
        options = catalog.options
        bucket = options["bucket"]
        region = options.get("region", _OPTIONAL_OPTIONS["region"])
        factory = options.get("_credential_resolver_factory", _credential_resolver_factory)
        resolver = factory(options, region)
        s3_client = resolver.create_client("s3")
        s3_client.head_bucket(Bucket=bucket)

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        import pyarrow.fs as pafs

        from rivet_core.introspection import CatalogNode, NodeSummary

        options = catalog.options
        bucket = options["bucket"]
        prefix = options.get("prefix", "")
        fmt = options.get("format", _OPTIONAL_OPTIONS["format"])

        fs = _build_s3fs(options)

        base_path = f"{bucket}/{prefix}" if prefix else bucket
        selector = pafs.FileSelector(base_path, allow_not_found=True, recursive=False)
        try:
            file_infos = fs.get_file_info(selector)
        except Exception as exc:
            _raise_if_s3_client_error(exc, bucket)
            return []

        nodes: list[CatalogNode] = []
        ext = f".{fmt}" if fmt != "delta" else ""

        for fi in file_infos:
            if fi.type == pafs.FileType.File:
                if fmt == "delta":
                    continue  # delta tables are directories
                if not fi.base_name.endswith(ext):
                    continue
                table_name = fi.base_name[: -len(ext)]
                summary = NodeSummary(
                    row_count=None,
                    size_bytes=fi.size,
                    format=fmt,
                    last_modified=fi.mtime,
                    owner=None,
                    comment=None,
                )
                nodes.append(
                    CatalogNode(
                        name=table_name,
                        node_type="file",
                        path=[bucket, table_name],
                        is_container=False,
                        children_count=None,
                        summary=summary,
                    )
                )
            elif fi.type == pafs.FileType.Directory:
                if fmt == "delta":
                    # Check for _delta_log subdirectory
                    delta_log_path = f"{fi.path}/_delta_log"
                    try:
                        log_info = fs.get_file_info([delta_log_path])
                        if log_info and log_info[0].type == pafs.FileType.Directory:
                            table_name = fi.base_name
                            nodes.append(
                                CatalogNode(
                                    name=table_name,
                                    node_type="table",
                                    path=[bucket, table_name],
                                    is_container=True,
                                    children_count=None,
                                    summary=None,
                                )
                            )
                    except Exception:
                        pass

        return nodes

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:

        options = catalog.options
        bucket = options["bucket"]
        prefix = options.get("prefix", "")

        # Detect format from table name extension if present
        fmt = options.get("format", _OPTIONAL_OPTIONS["format"])
        dot_idx = table.rfind(".")
        table_for_path = table
        if dot_idx >= 0:
            ext = table[dot_idx:].lower()
            ext_fmt = {".parquet": "parquet", ".csv": "csv", ".json": "json", ".orc": "orc"}.get(
                ext
            )
            if ext_fmt:
                fmt = ext_fmt
                # Table name already has extension — don't append again
                table_for_path = table

        fs = _build_s3fs(options)

        path_parts = [bucket]
        if prefix:
            path_parts.append(prefix)
        path_parts.append(table_for_path)

        # Build file path — only append extension if table name doesn't already have one
        needs_ext = dot_idx < 0 or table[dot_idx:].lower() not in {
            ".parquet",
            ".csv",
            ".json",
            ".orc",
        }

        try:
            if fmt == "parquet":
                file_path = f"{'/'.join(path_parts)}" + (".parquet" if needs_ext else "")
                return self._schema_from_parquet(fs, file_path, bucket, table)
            elif fmt in ("csv", "json"):
                file_path = f"{'/'.join(path_parts)}" + (f".{fmt}" if needs_ext else "")
                n_rows = options.get(
                    "csv_inference_rows" if fmt == "csv" else "json_inference_rows", 1000
                )
                return self._schema_from_text(fs, file_path, fmt, n_rows, bucket, table)
            elif fmt == "orc":
                file_path = f"{'/'.join(path_parts)}" + (".orc" if needs_ext else "")
                return self._schema_from_orc(fs, file_path, bucket, table)
            else:
                # delta or unknown — structured error
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"Schema inference not supported for format '{fmt}'",
                        plugin_name="rivet_aws",
                        plugin_type="catalog",
                        remediation="Use a supported format (parquet, csv, json). Delta format is not yet supported.",
                    )
                )
        except (ExecutionError, PluginValidationError):
            raise
        except Exception as exc:
            _raise_if_s3_client_error(exc, bucket)
            raise

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        from rivet_core.introspection import ObjectMetadata

        options = catalog.options
        bucket = options["bucket"]
        prefix = options.get("prefix", "")

        # Detect format from table name extension if present
        fmt = options.get("format", _OPTIONAL_OPTIONS["format"])
        dot_idx = table.rfind(".")
        has_recognized_ext = False
        if dot_idx >= 0:
            ext_lower = table[dot_idx:].lower()
            ext_fmt = {".parquet": "parquet", ".csv": "csv", ".json": "json", ".orc": "orc"}.get(
                ext_lower
            )
            if ext_fmt:
                fmt = ext_fmt
                has_recognized_ext = True

        fs = _build_s3fs(options)

        path_parts = [bucket]
        if prefix:
            path_parts.append(prefix)
        path_parts.append(table)

        if fmt == "delta":
            dir_path = "/".join(path_parts)
            try:
                infos = fs.get_file_info([dir_path])
                fi = infos[0] if infos else None
            except Exception:
                fi = None
            return ObjectMetadata(
                path=[bucket, table],
                node_type="table",
                row_count=None,
                size_bytes=None,
                last_modified=fi.mtime if fi else None,
                created_at=None,
                format=fmt,
                compression=None,
                owner=None,
                comment=None,
                location=f"s3://{dir_path}",
                column_statistics=[],
                partitioning=None,
            )

        ext = "" if has_recognized_ext else f".{fmt}"
        s3_key = "/".join(path_parts[1:]) + ext  # key without bucket
        file_path = "/".join(path_parts) + ext

        # HeadObject via boto3 for size_bytes, last_modified, etag
        size_bytes: int | None = None
        last_modified = None
        properties: dict[str, str] = {}
        try:
            factory = options.get("_credential_resolver_factory", _credential_resolver_factory)
            region = options.get("region", _OPTIONAL_OPTIONS["region"])
            resolver = factory(options, region)
            s3_client = resolver.create_client("s3")
            head = s3_client.head_object(Bucket=bucket, Key=s3_key)
            size_bytes = head.get("ContentLength")
            last_modified = head.get("LastModified")
            etag = head.get("ETag")
            if etag:
                properties["etag"] = etag
        except ClientError as exc:
            from rivet_aws.errors import handle_s3_error

            raise handle_s3_error(exc, bucket=bucket, action="s3:GetObject") from exc
        except Exception:
            # Fallback to PyArrow file info
            try:
                infos = fs.get_file_info([file_path])
                fi = infos[0] if infos else None
                if fi:
                    size_bytes = fi.size
                    last_modified = fi.mtime
            except Exception:
                pass

        row_count: int | None = None
        num_row_groups: int | None = None

        if fmt == "parquet":
            try:
                import pyarrow.parquet as pq

                with fs.open_input_file(file_path) as f:
                    meta = pq.read_metadata(f)
                row_count = meta.num_rows
                num_row_groups = meta.num_row_groups
                properties["num_row_groups"] = str(num_row_groups)
            except Exception:
                pass

        return ObjectMetadata(
            path=[bucket, table],
            node_type="file",
            row_count=row_count,
            size_bytes=size_bytes,
            last_modified=last_modified,
            created_at=None,
            format=fmt,
            compression=None,
            owner=None,
            comment=None,
            location=f"s3://{file_path}",
            column_statistics=[],
            partitioning=None,
            properties=properties,
        )

    # ── list_children ────────────────────────────────────────────────

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        import pyarrow.fs as pafs

        from rivet_core.introspection import CatalogNode, NodeSummary

        options = catalog.options
        bucket = options["bucket"]

        # Build the S3 prefix from the path segments (path[0] is bucket)
        if len(path) <= 1:
            s3_path = bucket
        else:
            s3_path = "/".join([bucket] + path[1:])

        fs = _build_s3fs(options)
        selector = pafs.FileSelector(s3_path, allow_not_found=True, recursive=False)
        try:
            file_infos = fs.get_file_info(selector)
        except Exception as exc:
            _raise_if_s3_client_error(exc, bucket)
            return []

        nodes: list[CatalogNode] = []
        for fi in file_infos:
            child_path = path + [fi.base_name]
            if fi.type == pafs.FileType.Directory:
                metadata: dict[str, Any] = {}
                if _is_hive_partition(fi.base_name):
                    # Detect partition columns by scanning deeper
                    partition_cols = [fi.base_name.split("=", 1)[0]]
                    deeper = _detect_partition_columns(fs, fi.path)
                    partition_cols.extend(c for c in deeper if c not in partition_cols)
                    metadata["partition_columns"] = partition_cols
                summary = NodeSummary(
                    row_count=None,
                    size_bytes=None,
                    format=None,
                    last_modified=fi.mtime,
                    owner=None,
                    comment=None,
                )
                nodes.append(
                    CatalogNode(
                        name=fi.base_name,
                        node_type="container",
                        path=child_path,
                        is_container=True,
                        children_count=None,
                        summary=summary,
                        metadata=metadata,
                    )
                )
            elif fi.type == pafs.FileType.File:
                # Only include files with recognized extensions
                name = fi.base_name
                dot_idx = name.rfind(".")
                if dot_idx < 0:
                    continue
                ext = name[dot_idx:]
                if ext not in _RECOGNIZED_EXTENSIONS:
                    continue
                fmt = ext[1:]  # strip leading dot
                summary = NodeSummary(
                    row_count=None,
                    size_bytes=fi.size,
                    format=fmt,
                    last_modified=fi.mtime,
                    owner=None,
                    comment=None,
                )
                nodes.append(
                    CatalogNode(
                        name=name,
                        node_type="table",
                        path=child_path,
                        is_container=False,
                        children_count=None,
                        summary=summary,
                    )
                )
        return nodes

    # ── Schema helpers ────────────────────────────────────────────────

    def _schema_from_parquet(
        self, fs: Any, file_path: str, bucket: str, table: str
    ) -> ObjectSchema:
        import pyarrow.parquet as pq

        from rivet_core.introspection import ColumnDetail, ObjectSchema

        with fs.open_input_file(file_path) as f:
            schema = pq.read_schema(f)

        columns = [
            ColumnDetail(
                name=field.name,
                type=_arrow_type_str(field.type),
                native_type=None,
                nullable=field.nullable,
                default=None,
                comment=None,
                is_primary_key=False,
                is_partition_key=False,
            )
            for field in schema
        ]
        return ObjectSchema(
            path=[bucket, table],
            node_type="file",
            columns=columns,
            primary_key=None,
            comment=None,
        )

    def _schema_from_text(
        self, fs: Any, file_path: str, fmt: str, n_rows: int, bucket: str, table: str
    ) -> ObjectSchema:
        import pyarrow.csv as pa_csv
        import pyarrow.json as pa_json

        from rivet_core.introspection import ColumnDetail, ObjectSchema

        with fs.open_input_file(file_path) as f:
            if fmt == "csv":
                read_opts = pa_csv.ReadOptions(block_size=1024 * 1024)
                reader = pa_csv.open_csv(f, read_options=read_opts)
                batch = reader.read_next_batch()
                schema = batch.schema
            else:  # json
                read_opts = pa_json.ReadOptions(block_size=1024 * 1024)
                reader = pa_json.open_json(f, read_options=read_opts)
                batch = reader.read_next_batch()
                schema = batch.schema

        columns = [
            ColumnDetail(
                name=field.name,
                type=_arrow_type_str(field.type),
                native_type=None,
                nullable=field.nullable,
                default=None,
                comment=None,
                is_primary_key=False,
                is_partition_key=False,
            )
            for field in schema
        ]
        return ObjectSchema(
            path=[bucket, table],
            node_type="file",
            columns=columns,
            primary_key=None,
            comment=None,
        )

    def _schema_from_orc(self, fs: Any, file_path: str, bucket: str, table: str) -> ObjectSchema:
        import pyarrow.orc as pa_orc

        from rivet_core.introspection import ColumnDetail, ObjectSchema

        with fs.open_input_file(file_path) as f:
            orc_file = pa_orc.ORCFile(f)
            schema = orc_file.schema

        columns = [
            ColumnDetail(
                name=field.name,
                type=_arrow_type_str(field.type),
                native_type=None,
                nullable=field.nullable,
                default=None,
                comment=None,
                is_primary_key=False,
                is_partition_key=False,
            )
            for field in schema
        ]
        return ObjectSchema(
            path=[bucket, table],
            node_type="file",
            columns=columns,
            primary_key=None,
            comment=None,
        )
