"""Property test: generate_joint produces correctly annotated file (task 5.3).

Property 6: After execution, generate_joint(name, description) creates file
with correct SQL, engine annotation, upstream annotation, and optional
description annotation.

Validates: Requirements 4.1, 4.2, 4.7
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry

_sql_st = st.text(min_size=1, max_size=200, alphabet=st.characters(blacklist_categories=("Cs",)))
_engine_st = st.sampled_from(["duckdb", "spark", "trino"])
_upstream_st = st.lists(
    st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True),
    min_size=0,
    max_size=4,
)
_description_st = st.one_of(
    st.none(),
    st.text(min_size=1, max_size=100, alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\n\r")),
)
_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)


def _make_session(project_path: Path) -> InteractiveSession:
    session = InteractiveSession(project_path=project_path)
    session.init_from(
        assembly=Assembly([]),
        catalogs={},
        engines={
            "duckdb": ComputeEngine(name="duckdb", engine_type="duckdb"),
            "spark": ComputeEngine(name="spark", engine_type="spark"),
            "trino": ComputeEngine(name="trino", engine_type="trino"),
        },
        registry=PluginRegistry(),
    )
    session.start()
    return session


@given(sql=_sql_st, engine=_engine_st, upstream=_upstream_st, description=_description_st, name=_name_st)
@settings(max_examples=50)
def test_generate_joint_annotations(
    sql: str,
    engine: str,
    upstream: list[str],
    description: str | None,
    name: str,
) -> None:
    """generate_joint creates file with correct SQL, engine, upstream, and optional description."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        (project_path / "rivet.yaml").write_text("joints: joints\n")

        session = _make_session(project_path)
        session._last_query_sql = sql
        session._last_query_engine = engine
        session._last_query_upstream = upstream

        path = session.generate_joint(name, description=description)

        assert path.exists(), "Joint file must be created"
        content = path.read_text(encoding="utf-8")

        # SQL content is present
        assert sql in content, "File must contain the executed SQL"

        # Engine annotation is present
        assert f"-- rivet:engine: {engine}" in content, "File must contain engine annotation"

        # Upstream annotation is present
        upstream_str = "[" + ", ".join(upstream) + "]" if upstream else "[]"
        assert f"-- rivet:upstream: {upstream_str}" in content, "File must contain upstream annotation"

        # Description annotation: present iff description is not None
        if description is not None:
            assert f"-- rivet:description: {description}" in content, "File must contain description annotation"
        else:
            assert "rivet:description" not in content, "File must not contain description annotation when None"
