"""Tests for task 32.7: CTE-based fusion for adjacent SQL joints in PySpark engine."""

from __future__ import annotations

from unittest.mock import MagicMock

from rivet_core.models import Joint, Material
from rivet_pyspark.engine import PySparkComputeEngine, fuse_joints


def _joint(name: str, sql: str) -> Joint:
    return Joint(name=name, joint_type="sql", sql=sql)


class TestFuseJoints:
    """Unit tests for the fuse_joints() SQL builder."""

    def test_single_joint_returns_sql_unchanged(self):
        joints = [_joint("j1", "SELECT * FROM src")]
        result = fuse_joints(joints)
        assert result == "SELECT * FROM src"

    def test_two_joints_produces_cte(self):
        joints = [
            _joint("j1", "SELECT * FROM raw"),
            _joint("j2", "SELECT a FROM j1 WHERE a > 1"),
        ]
        result = fuse_joints(joints)
        assert result == "WITH j1 AS (SELECT * FROM raw) SELECT a FROM j1 WHERE a > 1"

    def test_three_joints_produces_multiple_ctes(self):
        joints = [
            _joint("j1", "SELECT * FROM raw"),
            _joint("j2", "SELECT a FROM j1"),
            _joint("j3", "SELECT a FROM j2 WHERE a > 5"),
        ]
        result = fuse_joints(joints)
        assert result == (
            "WITH j1 AS (SELECT * FROM raw), j2 AS (SELECT a FROM j1) "
            "SELECT a FROM j2 WHERE a > 5"
        )

    def test_terminal_sql_is_last_joint(self):
        joints = [
            _joint("a", "SELECT 1 AS x"),
            _joint("b", "SELECT x + 1 AS y FROM a"),
        ]
        result = fuse_joints(joints)
        assert result.endswith("SELECT x + 1 AS y FROM a")

    def test_cte_names_match_joint_names(self):
        joints = [
            _joint("my_cte", "SELECT 1 AS n"),
            _joint("final", "SELECT n FROM my_cte"),
        ]
        result = fuse_joints(joints)
        assert "my_cte AS (SELECT 1 AS n)" in result


class TestExecuteFusedGroup:
    """Tests for PySparkComputeEngine.execute_fused_group()."""

    def _make_engine_with_mock_session(self):
        """Return (engine, mock_spark_session)."""
        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_session.sql.return_value = mock_df

        engine = PySparkComputeEngine("spark1", {})
        engine._session = mock_session  # inject mock session directly
        return engine, mock_session, mock_df

    def test_single_joint_executes_sql_directly(self):
        engine, mock_session, mock_df = self._make_engine_with_mock_session()
        joints = [_joint("j1", "SELECT * FROM src")]
        result = engine.execute_fused_group(joints)
        mock_session.sql.assert_called_once_with("SELECT * FROM src")
        assert isinstance(result, Material)

    def test_two_joints_executes_cte_sql(self):
        engine, mock_session, mock_df = self._make_engine_with_mock_session()
        joints = [
            _joint("j1", "SELECT * FROM raw"),
            _joint("j2", "SELECT a FROM j1"),
        ]
        engine.execute_fused_group(joints)
        expected_sql = "WITH j1 AS (SELECT * FROM raw) SELECT a FROM j1"
        mock_session.sql.assert_called_once_with(expected_sql)

    def test_result_is_deferred_material(self):
        engine, mock_session, mock_df = self._make_engine_with_mock_session()
        joints = [_joint("j1", "SELECT 1 AS x")]
        result = engine.execute_fused_group(joints)
        assert isinstance(result, Material)
        assert result.state == "deferred"

    def test_result_name_is_terminal_joint_name(self):
        engine, mock_session, mock_df = self._make_engine_with_mock_session()
        joints = [_joint("my_output", "SELECT 1 AS x")]
        result = engine.execute_fused_group(joints)
        assert result.name == "my_output"

    def test_three_joints_cte_sql(self):
        engine, mock_session, mock_df = self._make_engine_with_mock_session()
        joints = [
            _joint("a", "SELECT * FROM raw"),
            _joint("b", "SELECT x FROM a"),
            _joint("c", "SELECT x FROM b WHERE x > 0"),
        ]
        engine.execute_fused_group(joints)
        expected = (
            "WITH a AS (SELECT * FROM raw), b AS (SELECT x FROM a) "
            "SELECT x FROM b WHERE x > 0"
        )
        mock_session.sql.assert_called_once_with(expected)

    def test_execute_fused_group_calls_get_session(self):
        """execute_fused_group uses the engine's session."""
        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_session.sql.return_value = mock_df

        engine = PySparkComputeEngine("spark1", {})
        engine._session = mock_session

        joints = [_joint("j1", "SELECT 1 AS n")]
        engine.execute_fused_group(joints)

        mock_session.sql.assert_called_once()

    def test_materialized_ref_wraps_dataframe(self):
        """The Material's materialized_ref holds the Spark DataFrame."""
        engine, mock_session, mock_df = self._make_engine_with_mock_session()
        joints = [_joint("j1", "SELECT 1 AS x")]
        result = engine.execute_fused_group(joints)
        assert result.materialized_ref is not None
        assert result.materialized_ref._df is mock_df
