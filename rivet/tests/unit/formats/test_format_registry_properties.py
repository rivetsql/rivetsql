"""Property-based tests for the FormatRegistry.

Covers Properties 1–7 from the generic file format design document.

- Property 1: Extension detection is case-insensitive and correct
  Validates: Requirements 2.1, 2.4
- Property 2: Unrecognized or missing extensions produce the default format
  Validates: Requirements 2.2, 2.3
- Property 3: Directory probing detects format from contents
  Validates: Requirements 2.5, 2.6, 2.7, 2.8
- Property 4: Cascading resolution returns the first non-empty valid candidate
  Validates: Requirements 2.9, 3.1, 3.2, 3.3, 3.4
- Property 5: Format name validation is case-insensitive and rejects invalid names
  Validates: Requirements 4.1, 4.2, 4.3
- Property 6: Plugin capability query and validation are consistent
  Validates: Requirements 5.2, 5.3
- Property 7: Backward compatibility — all previously accepted formats remain accepted
  Validates: Requirements 6.3, 7.4, 8.3, 9.3, 11.3
"""

from __future__ import annotations

from collections import Counter

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.errors import PluginValidationError
from rivet_core.formats import (
    EXT_TO_FORMAT,
    PLUGIN_CAPABILITIES,
    FileFormat,
    FormatRegistry,
)

# ---------------------------------------------------------------------------
# Shared strategies
# ---------------------------------------------------------------------------

_RECOGNIZED_EXTS = list(EXT_TO_FORMAT.keys())
_FORMAT_NAMES = [f.value for f in FileFormat]
_ALL_FORMATS = list(FileFormat)

st_file_format = st.sampled_from(_ALL_FORMATS)


def _random_case(s: str) -> st.SearchStrategy[str]:
    """Strategy that produces a random case variation of *s*."""
    return st.tuples(*(st.sampled_from([c.lower(), c.upper()]) for c in s)).map("".join)


st_case_varied_ext = st.sampled_from(_RECOGNIZED_EXTS).flatmap(
    lambda ext: _random_case(ext).map(lambda varied: (ext, varied))
)
"""Produces (canonical_ext, case_varied_ext) tuples."""


# ---------------------------------------------------------------------------
# Property 1: Extension detection is case-insensitive and correct
# Feature: generic-file-format, Property 1: Extension detection is case-insensitive and correct
# ---------------------------------------------------------------------------


@given(data=st_case_varied_ext)
@settings(max_examples=100)
def test_extension_detection_case_insensitive(data: tuple[str, str]):
    canonical_ext, varied_ext = data
    expected = EXT_TO_FORMAT[canonical_ext]
    path = f"/some/dir/file{varied_ext}"
    result = FormatRegistry.detect_format(path)
    assert result is expected, f"detect_format({path!r}) returned {result}, expected {expected}"


# ---------------------------------------------------------------------------
# Property 2: Unrecognized or missing extensions produce the default format
# Feature: generic-file-format, Property 2: Unrecognized or missing extensions produce the default format
# ---------------------------------------------------------------------------

# Extensions that are NOT in the recognized set
_UNRECOGNIZED_EXTS = [".xlsx", ".txt", ".xml", ".yaml", ".toml", ".md", ".html", ".zip"]

st_unrecognized_path = st.one_of(
    # Path with unrecognized extension
    st.sampled_from(_UNRECOGNIZED_EXTS).map(lambda ext: f"/data/file{ext}"),
    # Path with no extension at all
    st.from_regex(r"[a-z][a-z0-9_/]{0,30}", fullmatch=True).map(lambda s: f"/data/{s}"),
)


@given(path=st_unrecognized_path, default=st_file_format)
@settings(max_examples=100)
def test_unrecognized_extension_returns_default(path: str, default: FileFormat):
    result = FormatRegistry.detect_format(path, default=default)
    assert result is default, f"detect_format({path!r}, default={default}) returned {result}"


# ---------------------------------------------------------------------------
# Property 3: Directory probing detects format from contents
# Feature: generic-file-format, Property 3: Directory probing detects format from contents
# ---------------------------------------------------------------------------

# Strategy for child names with recognized extensions
st_recognized_child = st.sampled_from(_RECOGNIZED_EXTS).map(lambda ext: f"part-0001{ext}")

st_unrecognized_child = st.sampled_from(["_SUCCESS", "README.md", ".crc", "metadata.txt"])


@given(
    children=st.lists(st_recognized_child, min_size=1, max_size=20),
    noise=st.lists(st_unrecognized_child, max_size=5),
    default=st_file_format,
)
@settings(max_examples=100)
def test_directory_probing_most_common_extension(
    children: list[str], noise: list[str], default: FileFormat
):
    """Without _delta_log, probing returns the most common recognized format."""
    all_children = children + noise
    result = FormatRegistry.detect_format(
        "s3://bucket/prefix", default=default, child_names=all_children
    )

    # Compute expected: most common recognized extension, ties broken by enum ordinal
    ext_counts: Counter[FileFormat] = Counter()
    for name in children:
        ext = "." + name.rsplit(".", 1)[-1]
        ext_lower = ext.lower()
        if ext_lower in EXT_TO_FORMAT:
            ext_counts[EXT_TO_FORMAT[ext_lower]] += 1

    if ext_counts:
        max_count = max(ext_counts.values())
        tied = [f for f, c in ext_counts.items() if c == max_count]
        tied.sort(key=lambda f: list(FileFormat).index(f))
        expected = tied[0]
    else:
        expected = default

    assert result is expected


@given(
    extra_children=st.lists(st_recognized_child, max_size=10),
    default=st_file_format,
)
@settings(max_examples=100)
def test_directory_probing_delta_log_takes_priority(extra_children: list[str], default: FileFormat):
    """_delta_log/ in child_names always produces DELTA, regardless of other children."""
    children = ["_delta_log/"] + extra_children
    result = FormatRegistry.detect_format(
        "s3://bucket/prefix", default=default, child_names=children
    )
    assert result is FileFormat.DELTA


@given(default=st_file_format)
@settings(max_examples=100)
def test_directory_probing_no_recognized_returns_default(default: FileFormat):
    """No recognized extensions in children → default."""
    children = ["_SUCCESS", "README.md", ".hidden"]
    result = FormatRegistry.detect_format(
        "s3://bucket/prefix", default=default, child_names=children
    )
    assert result is default


# ---------------------------------------------------------------------------
# Property 4: Cascading resolution returns the first non-empty valid candidate
# Feature: generic-file-format, Property 4: Cascading resolution returns the first non-empty valid candidate
# ---------------------------------------------------------------------------

st_candidate = st.one_of(
    st.none(),
    st.just(""),
    st.sampled_from(_FORMAT_NAMES),
)


@given(
    candidates=st.lists(st_candidate, min_size=1, max_size=5),
    default=st_file_format,
)
@settings(max_examples=100)
def test_cascading_resolution_first_nonempty(candidates: list[str | None], default: FileFormat):
    """resolve_format returns the first non-None, non-empty valid candidate."""
    result = FormatRegistry.resolve_format(*candidates, default=default)

    # Find expected: first non-None, non-empty candidate
    expected = default
    for c in candidates:
        if c is not None and c != "":
            low = c.strip().lower()
            if low in {f.value for f in FileFormat}:
                expected = FileFormat(low)
                break

    assert result is expected


@given(default=st_file_format)
@settings(max_examples=100)
def test_cascading_resolution_all_empty_returns_default(default: FileFormat):
    """All None/empty candidates with no path → default."""
    result = FormatRegistry.resolve_format(None, "", None, default=default)
    assert result is default


@given(
    explicit=st.sampled_from(_FORMAT_NAMES),
    path_ext=st.sampled_from(_RECOGNIZED_EXTS),
)
@settings(max_examples=100)
def test_cascading_resolution_explicit_beats_path(explicit: str, path_ext: str):
    """An explicit candidate always wins over path-based detection."""
    result = FormatRegistry.resolve_format(explicit, path=f"/data/file{path_ext}")
    assert result is FileFormat(explicit)


# ---------------------------------------------------------------------------
# Property 5: Format name validation is case-insensitive and rejects invalid names
# Feature: generic-file-format, Property 5: Format name validation is case-insensitive and rejects invalid names
# ---------------------------------------------------------------------------


@given(
    data=st.sampled_from(_FORMAT_NAMES).flatmap(
        lambda name: _random_case(name).map(lambda varied: (name, varied))
    )
)
@settings(max_examples=100)
def test_format_validation_case_insensitive(data: tuple[str, str]):
    canonical, varied = data
    result = FormatRegistry.validate_format(varied)
    assert result is FileFormat(canonical)


@given(
    name=st.text(min_size=1, max_size=20).filter(
        lambda s: s.strip().lower() not in {f.value for f in FileFormat}
    )
)
@settings(max_examples=100)
def test_format_validation_rejects_invalid(name: str):
    with pytest.raises(PluginValidationError):
        FormatRegistry.validate_format(name)


# ---------------------------------------------------------------------------
# Property 6: Plugin capability query and validation are consistent
# Feature: generic-file-format, Property 6: Plugin capability query and validation are consistent
# ---------------------------------------------------------------------------

_PLUGIN_KEYS = list(PLUGIN_CAPABILITIES.keys())


@given(
    fmt=st_file_format,
    plugin_key=st.sampled_from(_PLUGIN_KEYS),
)
@settings(max_examples=100)
def test_capability_query_validation_consistency(fmt: FileFormat, plugin_key: tuple[str, str]):
    """is_supported returns True iff validate_plugin_support does not raise."""
    plugin_name, plugin_type = plugin_key
    supported = FormatRegistry.is_supported(fmt, plugin_name, plugin_type)

    if supported:
        # Should not raise
        FormatRegistry.validate_plugin_support(fmt, plugin_name, plugin_type)
    else:
        with pytest.raises(PluginValidationError):
            FormatRegistry.validate_plugin_support(fmt, plugin_name, plugin_type)


# ---------------------------------------------------------------------------
# Property 7: Backward compatibility — all previously accepted formats remain accepted
# Feature: generic-file-format, Property 7: Backward compatibility — all previously accepted formats remain accepted
# ---------------------------------------------------------------------------

_LEGACY_PLUGIN_FORMATS: list[tuple[str, str, list[FileFormat]]] = [
    # (plugin_name, plugin_type, previously_accepted_formats)
    (
        "filesystem",
        "catalog",
        [FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON, FileFormat.IPC],
    ),
    ("filesystem", "sink", [FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON]),
    (
        "s3",
        "source",
        [FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON, FileFormat.ORC, FileFormat.DELTA],
    ),
    (
        "s3",
        "sink",
        [FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON, FileFormat.ORC, FileFormat.DELTA],
    ),
]


@pytest.mark.parametrize(
    "plugin_name, plugin_type, old_formats",
    _LEGACY_PLUGIN_FORMATS,
    ids=[f"{p[0]}-{p[1]}" for p in _LEGACY_PLUGIN_FORMATS],
)
def test_backward_compatibility(plugin_name: str, plugin_type: str, old_formats: list[FileFormat]):
    """All previously accepted formats remain accepted after migration."""
    for fmt in old_formats:
        assert FormatRegistry.is_supported(fmt, plugin_name, plugin_type), (
            f"{fmt.value} should be supported by {plugin_name} {plugin_type}"
        )
        # validate_plugin_support should not raise
        FormatRegistry.validate_plugin_support(fmt, plugin_name, plugin_type)
