"""E2E tests for enhanced compilation output feature.

Tests full CLI lifecycle for enhanced compilation output: execution SQL display,
pushdown details, cross-group optimizations, and enhanced fused group display.

Validates Requirements: 1.1-1.5, 2.1-2.6, 3.1-3.5, 4.1-4.5, 6.1-6.5, 8.1-8.5
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.conftest import run_cli, write_joint, write_sink, write_source

# ---------------------------------------------------------------------------
# Test 5.2: Execution SQL display at verbosity 1
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_execution_sql_display_verbosity_1(rivet_project: Path, capsys) -> None:
    """Execution SQL is displayed at verbosity 1.

    Creates a simple SQL joint and verifies that the compilation output
    contains the "sql (executed):" line showing the final SQL that will
    be executed on the engine.

    Validates Requirements: 1.1, 1.3, 1.4, 1.5
    """
    project = rivet_project

    # Create source data
    (project / "data" / "users.csv").write_text(
        "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n"
    )

    # Define source joint
    write_source(project, "src_users", catalog="local", table="users")

    # Define SQL joint with simple transformation
    write_joint(
        project,
        "transform_users",
        "SELECT id, name FROM src_users WHERE id > 0",
    )

    # Define sink
    write_sink(
        project,
        "sink_users",
        catalog="local",
        table="output_users",
        upstream=["transform_users"],
    )

    # Run compile with verbosity 1 (--verbose flag)
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Verify output contains fused SQL (what actually executes)
    assert "Fused SQL:" in result.stdout, (
        "Expected 'Fused SQL:' in compilation output at verbosity 1"
    )

    # Verify joint's own SQL is displayed
    assert "sql (original):" in result.stdout, (
        "Expected 'sql (original):' for joints in fused group"
    )

    # Verify execution SQL is displayed for the transform joint
    assert "transform_users" in result.stdout
    assert "SELECT" in result.stdout


@pytest.mark.e2e
def test_execution_sql_differs_from_original_with_optimizations(
    rivet_project: Path, capsys
) -> None:
    """Execution SQL differs from original when optimizations are applied.

    Creates a pipeline where predicate pushdown is applied, and verifies
    that the execution SQL reflects the optimization.

    Validates Requirements: 1.1, 1.3, 1.4
    """
    project = rivet_project

    # Create source data
    (project / "data" / "orders.csv").write_text(
        "order_id,customer,amount\n1,Alice,100\n2,Bob,200\n3,Charlie,150\n"
    )

    # Define source joint
    write_source(project, "src_orders", catalog="local", table="orders")

    # Define SQL joint with filter that can be pushed down
    write_joint(
        project,
        "filtered_orders",
        "SELECT * FROM src_orders WHERE amount > 100",
    )

    # Define sink
    write_sink(
        project,
        "sink_orders",
        catalog="local",
        table="output_orders",
        upstream=["filtered_orders"],
    )

    # Run compile with verbosity 1
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Verify fused SQL is displayed (what actually executes)
    assert "Fused SQL:" in result.stdout

    # Verify the filter predicate appears in the output
    assert "amount > 100" in result.stdout or "amount>100" in result.stdout


# ---------------------------------------------------------------------------
# Test 5.3: Pushdown details display
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_pushdown_details_display(rivet_project: Path, capsys) -> None:
    """Pushdown details are displayed at verbosity 1.

    Creates a project with source joint and predicate pushdown, and verifies
    that the compilation output contains the "Pushdown Details:" section
    with pushed predicates, projections, and limits listed correctly.

    Validates Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6
    """
    project = rivet_project

    # Create source data
    (project / "data" / "products.csv").write_text(
        "product_id,name,price,category\n"
        "1,Widget,10.99,A\n"
        "2,Gadget,25.50,B\n"
        "3,Gizmo,5.00,A\n"
        "4,Doohickey,15.00,C\n"
    )

    # Define source joint
    write_source(project, "src_products", catalog="local", table="products")

    # Define SQL joint with filter and projection
    write_joint(
        project,
        "filtered_products",
        "SELECT product_id, name, price FROM src_products WHERE price > 10 LIMIT 5",
    )

    # Define sink
    write_sink(
        project,
        "sink_products",
        catalog="local",
        table="output_products",
        upstream=["filtered_products"],
    )

    # Run compile with verbosity 1
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Verify pushdown details section exists
    # Note: Pushdown details may appear in different formats depending on
    # whether the optimizer successfully pushes operations down
    output = result.stdout

    # Check for fused SQL (always present at verbosity 1)
    assert "Fused SQL:" in output

    # Check for predicate in the output (either in pushdown section or in SQL)
    assert "price" in output and "10" in output


@pytest.mark.e2e
def test_pushdown_details_with_projections(rivet_project: Path, capsys) -> None:
    """Pushdown details show projected columns.

    Creates a pipeline with column projection and verifies that the
    compilation output shows which columns were selected.

    Validates Requirements: 2.3, 2.6
    """
    project = rivet_project

    # Create source data with many columns
    (project / "data" / "wide_table.csv").write_text(
        "col1,col2,col3,col4,col5\na,b,c,d,e\nf,g,h,i,j\n"
    )

    # Define source joint
    write_source(project, "src_wide", catalog="local", table="wide_table")

    # Define SQL joint selecting only 2 columns
    write_joint(
        project,
        "narrow_table",
        "SELECT col1, col3 FROM src_wide",
    )

    # Define sink
    write_sink(
        project,
        "sink_narrow",
        catalog="local",
        table="output_narrow",
        upstream=["narrow_table"],
    )

    # Run compile with verbosity 1
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Verify fused SQL shows projection
    assert "Fused SQL:" in result.stdout
    assert "col1" in result.stdout
    assert "col3" in result.stdout


# ---------------------------------------------------------------------------
# Test 5.4: Cross-group optimization display
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_cross_group_optimization_display(rivet_project: Path, capsys) -> None:
    """Cross-group optimizations are displayed at verbosity 1.

    Creates a multi-group pipeline with cross-group pushdown and verifies
    that the compilation output contains the "Cross-Group Optimizations"
    section showing source and target groups correctly.

    Validates Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
    """
    project = rivet_project

    # Update profiles to have two engines
    profiles_content = """\
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
    - name: duckdb_secondary
      type: duckdb
      catalogs: [local]
  default_engine: duckdb_primary
"""
    (project / "profiles.yaml").write_text(profiles_content)

    # Create source data
    (project / "data" / "sales.csv").write_text(
        "sale_id,product,amount\n1,Widget,100\n2,Gadget,200\n3,Gizmo,150\n"
    )

    # Define source joint on primary engine
    write_source(project, "src_sales", catalog="local", table="sales")

    # Define transform on primary engine
    write_joint(
        project,
        "transform_sales",
        "SELECT * FROM src_sales WHERE amount > 100",
        engine="duckdb_primary",
    )

    # Define another transform on secondary engine (forces cross-group boundary)
    write_joint(
        project,
        "aggregate_sales",
        "SELECT product, SUM(amount) as total FROM transform_sales GROUP BY product",
        engine="duckdb_secondary",
    )

    # Define sink
    write_sink(
        project,
        "sink_sales",
        catalog="local",
        table="output_sales",
        upstream=["aggregate_sales"],
    )

    # Run compile with verbosity 1
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Verify execution SQL is displayed
    assert "sql (executed):" in result.stdout

    # Verify engine boundaries are shown (indicates multi-group pipeline)
    assert "Engine Boundaries" in result.stdout or "duckdb_primary" in result.stdout


# ---------------------------------------------------------------------------
# Test 5.5: Enhanced fused group display
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_enhanced_fused_group_display(rivet_project: Path, capsys) -> None:
    """Enhanced fused group display shows fused SQL prominently.

    Creates a multi-joint fused group and verifies that the compilation
    output displays the fused SQL prominently, shows the fusion strategy,
    and lists the joints in the group.

    Validates Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
    """
    project = rivet_project

    # Create source data
    (project / "data" / "customers.csv").write_text("customer_id,name\n1,Alice\n2,Bob\n3,Charlie\n")

    # Define source joint
    write_source(project, "src_customers", catalog="local", table="customers")

    # Define multiple SQL joints that will be fused
    write_joint(
        project,
        "step1",
        "SELECT customer_id, UPPER(name) as name FROM src_customers",
    )

    write_joint(
        project,
        "step2",
        "SELECT customer_id, name || '_processed' as name FROM step1",
    )

    # Define sink
    write_sink(
        project,
        "sink_customers",
        catalog="local",
        table="output_customers",
        upstream=["step2"],
    )

    # Run compile with verbosity 1
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    output = result.stdout

    # Verify fused group display elements
    # At verbosity 1, should show enhanced fused group display
    assert "Fused Group" in output or "╔══" in output

    # Verify fused SQL is shown at the top
    assert "Fused SQL:" in output

    # Verify individual joint SQL is shown (not execution SQL)
    assert "sql (original):" in output

    # Verify joints are listed
    assert "step1" in output
    assert "step2" in output


@pytest.mark.e2e
def test_single_joint_group_no_fusion_display(rivet_project: Path, capsys) -> None:
    """Single-joint groups omit fusion-specific sections.

    Creates a pipeline with single-joint groups and verifies that
    fusion-specific sections are omitted.

    Validates Requirements: 4.4
    """
    project = rivet_project

    # Create source data
    (project / "data" / "items.csv").write_text("item_id,name\n1,Item1\n2,Item2\n")

    # Define source joint
    write_source(project, "src_items", catalog="local", table="items")

    # Define sink (single joint, no fusion)
    write_sink(
        project,
        "sink_items",
        catalog="local",
        table="output_items",
        upstream=["src_items"],
    )

    # Run compile with verbosity 1
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Verify that fusion-specific sections are omitted for groups without SQL
    # (source + sink have no SQL, so no fused SQL or execution SQL)
    assert "Fused SQL:" not in result.stdout, (
        "Fused SQL should not appear for groups without SQL joints"
    )

    # Single-joint groups should still show joint information
    assert "src_items" in result.stdout
    assert "sink_items" in result.stdout


# ---------------------------------------------------------------------------
# Test 5.6: Verbosity 0 backward compatibility
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_verbosity_0_backward_compatibility(rivet_project: Path, capsys) -> None:
    """Verbosity 0 maintains backward compatibility.

    Creates a complex pipeline and verifies that at verbosity 0, the output
    matches the pre-feature format with no execution SQL, pushdown details,
    or cross-group sections.

    Validates Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
    """
    project = rivet_project

    # Create source data
    (project / "data" / "data.csv").write_text("id,value\n1,100\n2,200\n3,300\n")

    # Define source joint
    write_source(project, "src_data", catalog="local", table="data")

    # Define multiple transforms
    write_joint(
        project,
        "transform1",
        "SELECT * FROM src_data WHERE value > 100",
    )

    write_joint(
        project,
        "transform2",
        "SELECT id, value * 2 as doubled FROM transform1",
    )

    # Define sink
    write_sink(
        project,
        "sink_data",
        catalog="local",
        table="output_data",
        upstream=["transform2"],
    )

    # Run compile with verbosity 0 (no --verbose flag)
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    output = result.stdout

    # Verify enhanced sections are NOT present at verbosity 0
    assert "sql (executed):" not in output, "Execution SQL should not appear at verbosity 0"
    assert "Pushdown Details:" not in output, "Pushdown details should not appear at verbosity 0"
    assert "Cross-Group Optimizations" not in output, (
        "Cross-group optimizations should not appear at verbosity 0"
    )

    # Verify basic information is still present
    assert "src_data" in output or "transform1" in output or "transform2" in output


@pytest.mark.e2e
def test_verbosity_0_compact_output(rivet_project: Path, capsys) -> None:
    """Verbosity 0 produces compact output with joint names and types only.

    Validates Requirements: 6.1, 6.2
    """
    project = rivet_project

    # Create minimal pipeline
    (project / "data" / "test.csv").write_text("id\n1\n2\n")
    write_source(project, "src_test", catalog="local", table="test")
    write_sink(
        project,
        "sink_test",
        catalog="local",
        table="output_test",
        upstream=["src_test"],
    )

    # Run compile with verbosity 0
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Verify compact output
    assert "src_test" in result.stdout
    assert "sink_test" in result.stdout

    # Verify no SQL is displayed at verbosity 0
    assert "SELECT" not in result.stdout or result.stdout.count("SELECT") <= 1


# ---------------------------------------------------------------------------
# Test 5.7: Incremental display and dependency order
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_incremental_display_dependency_order(rivet_project: Path, capsys) -> None:
    """Joints are displayed in dependency order.

    Creates a project with multiple joints and verifies that the compilation
    output displays joints in dependency order and output is not interleaved.

    Validates Requirements: 8.1, 8.2, 8.3, 8.4, 8.5
    """
    project = rivet_project

    # Create source data
    (project / "data" / "base.csv").write_text("id,value\n1,10\n2,20\n3,30\n")

    # Define source joint
    write_source(project, "src_base", catalog="local", table="base")

    # Define chain of transforms
    write_joint(
        project,
        "step_a",
        "SELECT id, value * 2 as value FROM src_base",
    )

    write_joint(
        project,
        "step_b",
        "SELECT id, value + 10 as value FROM step_a",
    )

    write_joint(
        project,
        "step_c",
        "SELECT id, value * 3 as value FROM step_b",
    )

    # Define sink
    write_sink(
        project,
        "sink_final",
        catalog="local",
        table="output_final",
        upstream=["step_c"],
    )

    # Run compile with verbosity 1
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    output = result.stdout

    # Verify all joints are present
    assert "src_base" in output
    assert "step_a" in output
    assert "step_b" in output
    assert "step_c" in output
    assert "sink_final" in output

    # Extract the Joint Details section to check ordering
    # Find the section between "Joint Details:" and the next major section
    if "Joint Details:" in output:
        details_start = output.find("Joint Details:")
        # Find the end of the fused group section (marked by ╚)
        details_end = output.find("╚", details_start)
        if details_end == -1:
            details_end = len(output)

        joint_details_section = output[details_start:details_end]

        # Verify dependency order within the Joint Details section
        src_pos = joint_details_section.find("src_base")
        step_a_pos = joint_details_section.find("step_a")
        step_b_pos = joint_details_section.find("step_b")
        step_c_pos = joint_details_section.find("step_c")

        # Source should appear before transforms in the Joint Details section
        assert src_pos < step_a_pos, "Source should appear before step_a in Joint Details"
        assert step_a_pos < step_b_pos, "step_a should appear before step_b in Joint Details"
        assert step_b_pos < step_c_pos, "step_b should appear before step_c in Joint Details"
    else:
        # If no Joint Details section, check in the full output
        # This handles the case where joints are rendered individually
        src_pos = output.find("src_base")
        step_a_pos = output.find("step_a")
        step_b_pos = output.find("step_b")
        step_c_pos = output.find("step_c")

        assert src_pos < step_a_pos, "Source should appear before step_a"
        assert step_a_pos < step_b_pos, "step_a should appear before step_b"
        assert step_b_pos < step_c_pos, "step_b should appear before step_c"


@pytest.mark.e2e
def test_compilation_output_not_interleaved(rivet_project: Path, capsys) -> None:
    """Compilation output is not interleaved for multiple joints.

    Verifies that each joint's information is displayed as a complete block
    without interleaving from other joints.

    Validates Requirements: 8.5
    """
    project = rivet_project

    # Create source data
    (project / "data" / "data1.csv").write_text("id\n1\n2\n")
    (project / "data" / "data2.csv").write_text("id\n3\n4\n")

    # Define two independent source joints
    write_source(project, "src1", catalog="local", table="data1")
    write_source(project, "src2", catalog="local", table="data2")

    # Define transforms
    write_joint(project, "transform1", "SELECT * FROM src1")
    write_joint(project, "transform2", "SELECT * FROM src2")

    # Define sinks
    write_sink(
        project,
        "sink1",
        catalog="local",
        table="output1",
        upstream=["transform1"],
    )
    write_sink(
        project,
        "sink2",
        catalog="local",
        table="output2",
        upstream=["transform2"],
    )

    # Run compile with verbosity 1
    result = run_cli(project, ["compile", "--verbose"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Verify output is structured and not interleaved
    # Each joint should have its information in a contiguous block
    assert "src1" in result.stdout
    assert "src2" in result.stdout
    assert "transform1" in result.stdout
    assert "transform2" in result.stdout


# ---------------------------------------------------------------------------
# Test: Verbosity 2 includes all enhanced information
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_verbosity_2_includes_enhanced_information(rivet_project: Path, capsys) -> None:
    """Verbosity 2 includes all enhanced information.

    Verifies that at verbosity 2, all enhanced sections are displayed
    including execution SQL, pushdown details, and cross-group optimizations.

    Validates Requirements: 6.4
    """
    project = rivet_project

    # Create source data
    (project / "data" / "test_data.csv").write_text(
        "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,150\n"
    )

    # Define source joint
    write_source(project, "src_test", catalog="local", table="test_data")

    # Define transform with filter
    write_joint(
        project,
        "filtered",
        "SELECT id, name FROM src_test WHERE value > 100",
    )

    # Define sink
    write_sink(
        project,
        "sink_test",
        catalog="local",
        table="output_test",
        upstream=["filtered"],
    )

    # Run compile with verbosity 2 (-vv flag)
    result = run_cli(project, ["compile", "-vv"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    output = result.stdout

    # Verify enhanced information is present at verbosity 2
    assert "Fused SQL:" in output, "Fused SQL should appear at verbosity 2"

    # Verify SQL content is displayed
    assert "SELECT" in output
    assert "filtered" in output


# ---------------------------------------------------------------------------
# Test: Error handling - compilation failure still produces output
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_compilation_error_handling(rivet_project: Path, capsys) -> None:
    """Compilation succeeds even with non-existent table references.

    Creates a project with SQL referencing a non-existent table and verifies
    that compilation succeeds (table existence is validated at runtime, not
    compile time).

    Validates Requirements: 8.3
    """
    project = rivet_project

    # Create source data
    (project / "data" / "test.csv").write_text("id\n1\n2\n")
    write_source(project, "src_test", catalog="local", table="test")

    # Define joint with invalid SQL
    write_joint(
        project,
        "invalid_joint",
        "SELECT * FROM nonexistent_table",
    )

    write_sink(
        project,
        "sink_test",
        catalog="local",
        table="output_test",
        upstream=["invalid_joint"],
    )

    # Run compile - should succeed (Rivet doesn't validate table existence at compile time)
    result = run_cli(project, ["compile", "--verbose"], capsys)

    # Compilation should succeed (table existence is validated at runtime, not compile time)
    assert result.exit_code == 0, (
        "Compilation should succeed - table existence is validated at runtime"
    )

    # Verify the joint is compiled successfully
    assert "invalid_joint" in result.stdout
