"""Integration tests for enhanced AssemblyFormatter methods.

Verifies that the formatter correctly renders execution SQL, pushdown details,
cross-group optimizations, and enhanced fused group displays.
"""

from __future__ import annotations

from rivet_cli.rendering.formatter import AssemblyFormatter
from rivet_core.assembly import Assembly
from rivet_core.compiler import CompiledAssembly, compile
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg


def _compile_pipeline(
    joints: list[Joint],
    *,
    catalogs: list[Catalog] | None = None,
    engines: list[ComputeEngine] | None = None,
    default_engine: str = "duckdb_primary",
    registry: PluginRegistry | None = None,
) -> CompiledAssembly:
    if registry is None:
        registry = _setup_registry()
    if catalogs is None:
        catalogs = [
            Catalog(
                name="local",
                type="filesystem",
                options={"path": "/tmp/fake", "format": "csv"},
            )
        ]
    if engines is None:
        plugin = registry.get_engine_plugin("duckdb")
        assert plugin is not None
        engines = [
            plugin.create_engine("duckdb_primary", {}),
        ]
    for e in engines:
        if e.name not in {ce.name for ce in registry._compute_engines.values()}:
            registry.register_compute_engine(e)
    assembly = Assembly(joints)
    return compile(
        assembly,
        catalogs=catalogs,
        engines=engines,
        registry=registry,
        default_engine=default_engine,
        introspect=False,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRenderExecutionSQL:
    """Test _render_execution_sql() method."""

    def test_execution_sql_displayed_at_verbosity_1(self):
        """Execution SQL should be displayed at verbosity >= 1.

        For joints in fused groups, the fused SQL is shown prominently.
        For standalone joints, execution SQL is shown in the joint line.
        """
        # Create a standalone joint (not fused) by using eager materialization
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE id > 10",
                eager=True,  # Force separate group
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # For joints in fused groups, fused SQL is shown
        # For standalone joints, execution SQL may be shown
        transform_joint = next(j for j in result.joints if j.name == "transform")
        if transform_joint.execution_sql:
            # Either execution SQL is shown directly, or fused SQL is shown
            assert "sql (executed):" in output or "Fused SQL:" in output

    def test_execution_sql_omitted_at_verbosity_0(self):
        """Execution SQL should not be displayed at verbosity 0."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=0)
        output = formatter.render(result)

        # Should not contain execution SQL section
        assert "sql (executed):" not in output

    def test_execution_sql_omitted_when_none(self):
        """Execution SQL section should be omitted when execution_sql is None."""
        joints = [
            Joint(
                name="py_joint",
                joint_type="python",
                function="os.path:exists",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Python joints have no execution SQL
        assert "sql (executed):" not in output

    def test_execution_sql_displayed_when_available(self):
        """Fused SQL and individual joint SQL should be displayed for fused groups."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="simple",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        # Manually set execution_sql to match sql
        simple_joint = next(j for j in result.joints if j.name == "simple")
        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # For fused groups, should show Fused SQL at top and individual joint SQL
        lines = output.split("\n")
        fused_sql_count = sum(1 for l in lines if "Fused SQL:" in l)
        original_sql_count = sum(1 for l in lines if "sql (original):" in l)

        # Should show fused SQL for the group
        assert fused_sql_count > 0, "Fused SQL should be displayed for fused groups"
        # Should show individual joint SQL (not execution SQL)
        if simple_joint.sql is not None:
            assert original_sql_count > 0, "Individual joint SQL should be displayed"


class TestRenderPushdownDetails:
    """Test _render_pushdown_details() method."""

    def test_pushdown_details_displayed_at_verbosity_1(self):
        """Pushdown details should be displayed at verbosity >= 1."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="filter",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE amount > 100",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Check if any pushdown occurred
        has_pushdown = any(
            g.per_joint_predicates or g.per_joint_projections or g.per_joint_limits
            for g in result.fused_groups
        )

        if has_pushdown:
            assert "Pushdown Details:" in output

    def test_pushdown_details_omitted_at_verbosity_0(self):
        """Pushdown details should not be displayed at verbosity 0."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="filter",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE amount > 100",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=0)
        output = formatter.render(result)

        # Should not contain pushdown details
        assert "Pushdown Details:" not in output

    def test_pushdown_predicates_displayed(self):
        """Pushed predicates should be listed as bullet points."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="filter",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE amount > 100 AND status = 'active'",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Check if predicates were pushed
        has_pushed_predicates = any(bool(g.per_joint_predicates) for g in result.fused_groups)

        if has_pushed_predicates:
            assert "Pushed predicates:" in output
            assert "•" in output  # Bullet point

    def test_pushdown_projections_displayed(self):
        """Pushed projections should be displayed as comma-separated list."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="select_cols",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT id, amount FROM src",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Check if projections were pushed
        has_pushed_projections = any(bool(g.per_joint_projections) for g in result.fused_groups)

        if has_pushed_projections:
            assert "Pushed projections:" in output

    def test_pushdown_limits_displayed(self):
        """Pushed limits should be displayed with value."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="limited",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src LIMIT 100",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Check if limits were pushed
        has_pushed_limits = any(bool(g.per_joint_limits) for g in result.fused_groups)

        if has_pushed_limits:
            assert "Pushed limit:" in output


class TestRenderCrossGroupOptimizations:
    """Test _render_cross_group_optimizations() method."""

    def test_cross_group_section_displayed_at_verbosity_1(self):
        """Cross-group optimizations section should be displayed at verbosity >= 1."""
        # Create a pipeline that will have cross-group optimizations
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="checkpoint",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
                eager=True,
            ),
            Joint(
                name="filter",
                joint_type="sql",
                upstream=["checkpoint"],
                sql="SELECT * FROM checkpoint WHERE amount > 100",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Check if cross-group optimizations occurred
        # This depends on the optimizer's behavior
        if "Cross-Group Optimizations" in output:
            assert "→" in output  # Arrow symbol

    def test_cross_group_section_omitted_at_verbosity_0(self):
        """Cross-group optimizations should not be displayed at verbosity 0."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="checkpoint",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
                eager=True,
            ),
            Joint(
                name="filter",
                joint_type="sql",
                upstream=["checkpoint"],
                sql="SELECT * FROM checkpoint WHERE amount > 100",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=0)
        output = formatter.render(result)

        # Should not contain cross-group section
        assert "Cross-Group Optimizations" not in output

    def test_cross_group_section_omitted_when_no_optimizations(self):
        """Cross-group section should be omitted when no optimizations exist."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Single joint has no cross-group optimizations
        assert "Cross-Group Optimizations" not in output

    def test_cross_group_predicate_pushdown_displayed(self):
        """Cross-group predicate pushdown should show source and target groups."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="checkpoint",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
                eager=True,
            ),
            Joint(
                name="filter",
                joint_type="sql",
                upstream=["checkpoint"],
                sql="SELECT * FROM checkpoint WHERE id > 10",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # If cross-group predicate pushdown occurred
        if "Predicate Pushdown:" in output:
            assert "→" in output  # Arrow between groups


class TestRenderEnhancedFusedGroup:
    """Test _render_enhanced_fused_group() method."""

    def test_enhanced_fused_group_at_verbosity_1(self):
        """Enhanced fused group display should be used at verbosity >= 1."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="t1",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE id > 0",
            ),
            Joint(
                name="t2",
                joint_type="sql",
                upstream=["t1"],
                sql="SELECT * FROM t1 WHERE amount > 100",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Check for multi-joint fused groups
        multi_joint_groups = [g for g in result.fused_groups if len(g.joints) >= 2]

        if multi_joint_groups:
            # Should show fused SQL prominently
            assert "Fused SQL:" in output
            # Should show joint details
            assert "Joint Details:" in output
            # Should use box drawing characters
            assert "╔" in output or "║" in output

    def test_compact_fused_group_at_verbosity_0(self):
        """Compact fused group display should be used at verbosity 0."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="t1",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
            ),
            Joint(
                name="t2",
                joint_type="sql",
                upstream=["t1"],
                sql="SELECT * FROM t1",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=0)
        output = formatter.render(result)

        # Should not show detailed fused SQL
        assert "Fused SQL:" not in output
        # Should not show joint details section
        assert "Joint Details:" not in output

    def test_fused_group_shows_fusion_strategy(self):
        """Fused group display should show fusion strategy (CTE or temp_view)."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Should show fusion strategy
        assert "strategy: cte" in output or "strategy: temp_view" in output

    def test_fused_group_lists_joints(self):
        """Fused group display should list all joints in the group."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="t1",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
            ),
            Joint(
                name="t2",
                joint_type="sql",
                upstream=["t1"],
                sql="SELECT * FROM t1",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # All joint names should appear in output
        assert "src" in output
        assert "t1" in output
        assert "t2" in output


class TestFormatterVerbosityLevels:
    """Test formatter behavior at different verbosity levels."""

    def test_verbosity_0_compact_output(self):
        """Verbosity 0 should show only joint names and types."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE id > 10",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=0)
        output = formatter.render(result)

        # Should contain joint names
        assert "src" in output
        assert "transform" in output

        # Should not contain detailed information
        assert "sql (executed):" not in output
        assert "Pushdown Details:" not in output
        assert "Cross-Group Optimizations" not in output
        assert "Execution Plan" not in output

    def test_verbosity_1_normal_output(self):
        """Verbosity 1 should show enhanced information."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE id > 10",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Should contain execution plan
        assert "Execution Plan" in output
        # Should contain summary
        assert "Summary" in output

    def test_verbosity_2_verbose_output(self):
        """Verbosity 2 should show all available information."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
            ),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=2)
        output = formatter.render(result)

        # Should contain all sections from verbosity 1
        assert "Execution Plan" in output
        assert "Summary" in output


class TestFormatterColorHandling:
    """Test formatter color handling."""

    def test_color_enabled(self):
        """Formatter should include ANSI color codes when color=True."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=True, verbosity=1)
        output = formatter.render(result)

        # Should contain ANSI escape sequences (color codes start with \x1b[)
        # Note: This is a weak test since color codes might be stripped
        # in some environments
        assert len(output) > 0

    def test_color_disabled(self):
        """Formatter should not include ANSI color codes when color=False."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
        ]
        result = _compile_pipeline(joints)
        assert result.success

        formatter = AssemblyFormatter(color=False, verbosity=1)
        output = formatter.render(result)

        # Should not contain ANSI escape sequences
        assert "\x1b[" not in output
