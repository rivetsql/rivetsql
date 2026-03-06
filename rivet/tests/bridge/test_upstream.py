"""Tests for UpstreamInferrer."""

from __future__ import annotations

from pathlib import Path

import pytest

from rivet_bridge.upstream import UpstreamInferrer
from rivet_config import JointDeclaration


@pytest.fixture
def inferrer() -> UpstreamInferrer:
    return UpstreamInferrer()


SRC = Path("joints/test.sql")


def _decl(
    name: str = "my_joint",
    joint_type: str = "sql",
    sql: str | None = None,
    upstream: list[str] | None = None,
) -> JointDeclaration:
    return JointDeclaration(
        name=name,
        joint_type=joint_type,
        source_path=SRC,
        sql=sql,
        upstream=upstream,
    )


JOINTS = {"raw_users", "transformed_users", "output_sink"}


class TestExplicitUpstreamBypass:
    """Req 5.5: Explicit upstream (including []) bypasses inference."""

    def test_explicit_list_used_as_is(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(sql="SELECT * FROM raw_users", upstream=["transformed_users"])
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert result == ["transformed_users"]

    def test_empty_list_used_as_is(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(sql="SELECT * FROM raw_users", upstream=[])
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert result == []


class TestSourceJointsSkipped:
    """Req 5.1: Sources never have upstream inferred."""

    def test_source_returns_empty(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(joint_type="source", sql="SELECT * FROM raw_users")
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert result == []


class TestInferFromSQL:
    """Req 5.1–5.4: Parse SQL to extract upstream joint refs."""

    def test_single_from_reference(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(sql="SELECT * FROM raw_users")
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert result == ["raw_users"]

    def test_join_references(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(sql="SELECT * FROM raw_users JOIN transformed_users ON raw_users.id = transformed_users.id")
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert "raw_users" in result
        assert "transformed_users" in result

    def test_external_table_excluded(self, inferrer: UpstreamInferrer) -> None:
        """Req 5.4: Non-matching table refs are external tables."""
        decl = _decl(sql="SELECT * FROM external_table")
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert result == []

    def test_mixed_joint_and_external(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(sql="SELECT * FROM raw_users JOIN external_table ON raw_users.id = external_table.id")
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert result == ["raw_users"]

    def test_no_duplicates(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(sql="SELECT a.id FROM raw_users a JOIN raw_users b ON a.id = b.id")
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert result == ["raw_users"]

    def test_no_sql_returns_empty(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(sql=None)
        result, errors = inferrer.infer(decl, JOINTS)
        assert not errors
        assert result == []


class TestErrorHandling:
    """BRG-101 on SQL parse failure."""

    def test_brg_101_on_bad_sql(self, inferrer: UpstreamInferrer) -> None:
        decl = _decl(sql="NOT VALID SQL (((")
        result, errors = inferrer.infer(decl, JOINTS)
        assert len(errors) == 1
        assert errors[0].code == "BRG-101"
        assert errors[0].joint_name == "my_joint"
        assert result == []
