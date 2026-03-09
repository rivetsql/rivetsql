"""Python joints E2E tests: pipelines mixing Python and SQL joints.

Validates Requirements 4.2, 4.6, 9.1
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tests.e2e.conftest import read_sink_csv, run_cli, write_joint, write_sink, write_source


def _write_python_joint(
    project: Path,
    name: str,
    code: str,
    *,
    upstream: list[str],
) -> None:
    """Write a Python joint file into joints/ with rivet annotations."""
    upstream_str = ", ".join(upstream)
    header = (
        f"# rivet:name: {name}\n"
        f"# rivet:type: python\n"
        f"# rivet:upstream: [{upstream_str}]\n"
    )
    (project / "joints" / f"{name}.py").write_text(header + code)


@pytest.mark.e2e
def test_python_transform_then_sql(rivet_project: Path, capsys) -> None:
    """Pipeline: source -> Python joint (add column) -> SQL joint -> sink."""
    project = rivet_project

    (project / "data" / "items.csv").write_text(
        "id,name,price\n1,Widget,10\n2,Gadget,25\n3,Gizmo,5\n4,Doohickey,30\n"
    )
    write_source(project, "src_items", catalog="local", table="items")

    _write_python_joint(
        project,
        "add_discount",
        (
            "import pyarrow as pa\n"
            "import pyarrow.compute as pc\n"
            "from rivet_core.models import Material\n"
            "\n"
            "def transform(material: Material) -> pa.Table:\n"
            "    table = material.to_arrow()\n"
            "    prices = table.column('price')\n"
            "    discounted = pc.multiply(prices, 0.8)\n"
            "    return table.append_column('discounted_price', discounted)\n"
        ),
        upstream=["src_items"],
    )

    write_joint(
        project, "filter_expensive",
        "SELECT * FROM add_discount WHERE discounted_price > 10",
    )
    write_sink(
        project, "expensive_items",
        catalog="local", table="expensive_items", upstream=["filter_expensive"],
    )

    project_str = str(project)
    sys.path.insert(0, project_str)
    try:
        result = run_cli(project, ["compile"], capsys)
        assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

        result = run_cli(project, ["run"], capsys)
        assert result.exit_code == 0, f"run failed:\n{result.stderr}"

        table = read_sink_csv(project, "expensive_items")
        assert table.num_rows == 2
        names = sorted(table.column("name").to_pylist())
        assert names == ["Doohickey", "Gadget"]
    finally:
        sys.path.remove(project_str)
        to_remove = [k for k in sys.modules if k.startswith("joints")]
        for k in to_remove:
            del sys.modules[k]


@pytest.mark.e2e
def test_sql_then_python_then_sql(rivet_project: Path, capsys) -> None:
    """Pipeline: source -> SQL -> Python -> SQL -> sink."""
    project = rivet_project

    (project / "data" / "scores.csv").write_text(
        "student,math,science\nAlice,85,90\nBob,70,65\nCharlie,95,88\n"
    )
    write_source(project, "src_scores", catalog="local", table="scores")

    write_joint(
        project, "select_scores",
        "SELECT student, math, science FROM src_scores",
    )

    _write_python_joint(
        project,
        "compute_avg",
        (
            "import pyarrow as pa\n"
            "from rivet_core.models import Material\n"
            "\n"
            "def transform(material: Material) -> pa.Table:\n"
            "    table = material.to_arrow()\n"
            "    math_scores = table.column('math').to_pylist()\n"
            "    science_scores = table.column('science').to_pylist()\n"
            "    averages = [(m + s) / 2.0 for m, s in zip(math_scores, science_scores)]\n"
            "    return table.append_column('average', pa.array(averages))\n"
        ),
        upstream=["select_scores"],
    )

    write_joint(
        project, "honor_roll",
        "SELECT student, average FROM compute_avg WHERE average >= 80",
    )
    write_sink(
        project, "honor_students",
        catalog="local", table="honor_students", upstream=["honor_roll"],
    )

    project_str = str(project)
    sys.path.insert(0, project_str)
    try:
        result = run_cli(project, ["compile"], capsys)
        assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

        result = run_cli(project, ["run"], capsys)
        assert result.exit_code == 0, f"run failed:\n{result.stderr}"

        table = read_sink_csv(project, "honor_students")
        assert table.num_rows == 2
        students = sorted(table.column("student").to_pylist())
        assert students == ["Alice", "Charlie"]
    finally:
        sys.path.remove(project_str)
        to_remove = [k for k in sys.modules if k.startswith("joints")]
        for k in to_remove:
            del sys.modules[k]
