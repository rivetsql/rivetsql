"""Property-based tests for optimizer E2E behaviour.

Covers Property 2 (Fused group produces single query) from the design document.

**Validates: Requirements 4.4**
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from .conftest import (
    QueryRecorder,
    read_sink_csv,
    run_cli,
    write_joint,
    write_sink,
    write_source,
)

# ---------------------------------------------------------------------------
# Scaffold helpers (replicate rivet_project fixture for use inside Hypothesis)
# ---------------------------------------------------------------------------

_RIVET_YAML = """\
profiles: profiles.yaml
sources: sources
joints: joints
sinks: sinks
tests: tests
quality: quality
"""

_PROFILES_YAML = """\
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
      format: csv
  engines:
    - name: duckdb_primary
      type: duckdb
      catalogs: [local]
  default_engine: duckdb_primary
"""


def _scaffold(tmp: Path) -> Path:
    """Create a bare Rivet project scaffold inside *tmp*."""
    (tmp / "rivet.yaml").write_text(_RIVET_YAML)
    (tmp / "profiles.yaml").write_text(_PROFILES_YAML)
    for d in ("sources", "joints", "sinks", "tests", "quality", "data"):
        (tmp / d).mkdir(exist_ok=True)
    return tmp


# ---------------------------------------------------------------------------
# SQL transform strategies
# ---------------------------------------------------------------------------

# Simple SQL transforms that read from an upstream and produce output.
# Each is a callable: (upstream_name: str) -> str
_TRANSFORMS = [
    lambda up: f"SELECT * FROM {up}",
    lambda up: f"SELECT id, amount FROM {up}",
    lambda up: f"SELECT id, amount FROM {up} WHERE amount > 0",
    lambda up: f"SELECT id, amount FROM {up} WHERE id > 0",
]

_transform_idx_st = st.integers(min_value=0, max_value=len(_TRANSFORMS) - 1)


# ---------------------------------------------------------------------------
# Property 2: Fused group produces single query
# ---------------------------------------------------------------------------


@given(
    chain_length=st.integers(min_value=2, max_value=6),
    transform_indices=st.lists(
        _transform_idx_st, min_size=6, max_size=6,
    ),
)
@settings(max_examples=100)
def test_property2_fused_group_single_query(
    chain_length: int,
    transform_indices: list[int],
) -> None:
    """Feature: e2e-tests, Property 2: Fused group produces single query.

    For any set of adjacent SQL joints on the same engine that the optimizer
    fuses into a single group, the QueryRecorder should capture exactly 1 SQL
    query dispatched to that engine for that group — not N separate queries.

    **Validates: Requirements 4.4**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        project = _scaffold(Path(tmpdir))

        # -- Write seed data --
        csv_data = "id,amount\n1,100\n2,50\n3,200\n"
        (project / "data" / "input_data.csv").write_text(csv_data)

        # -- Source --
        write_source(project, "src_input", catalog="local", table="input_data")

        # -- Chain of N adjacent SQL joints on duckdb_primary --
        prev_name = "src_input"
        for i in range(chain_length):
            joint_name = f"step_{i}"
            transform_fn = _TRANSFORMS[transform_indices[i] % len(_TRANSFORMS)]
            sql = transform_fn(prev_name)
            write_joint(project, joint_name, sql, engine="duckdb_primary")
            prev_name = joint_name

        # -- Sink from the last joint --
        write_sink(
            project,
            "sink_output",
            catalog="local",
            table="output_data",
            upstream=[prev_name],
        )

        # -- Compile --
        import io
        import sys

        # We can't use capsys inside Hypothesis, so capture manually
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()
        try:
            from rivet_cli.app import _main

            exit_code = _main(["compile", "--project", str(project)])
            assert exit_code == 0, f"compile failed (exit {exit_code})"

            # -- Run with QueryRecorder --
            recorder = QueryRecorder()
            with recorder:
                exit_code = _main(["run", "--project", str(project)])
            assert exit_code == 0, f"run failed (exit {exit_code})"
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        # -- Assert: exactly 1 query dispatched to duckdb_primary --
        primary_count = recorder.query_count("duckdb_primary")
        assert primary_count == 1, (
            f"Expected exactly 1 fused query on duckdb_primary for a chain "
            f"of {chain_length} adjacent joints, but got {primary_count} queries: "
            f"{[q.sql[:100] for q in recorder.queries_for_engine('duckdb_primary')]}"
        )
