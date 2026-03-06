"""Tests for DeclarationGenerator."""

from __future__ import annotations

from rivet_bridge.declarations import DeclarationGenerator
from rivet_core import Assembly, Joint
from rivet_core.checks import Assertion


def _make_assembly(joints: list[Joint]) -> Assembly:
    return Assembly(joints)


class TestFileCountAndPlacement:
    """Test that each joint produces one file in the correct directory."""

    def test_source_in_sources_dir(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        decl_files = [f for f in files if not f.relative_path.startswith("quality/")]
        assert len(decl_files) == 1
        assert decl_files[0].relative_path == "sources/raw_users.yaml"
        assert decl_files[0].joint_name == "raw_users"

    def test_sql_joint_in_joints_dir(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
            Joint(name="clean_users", joint_type="sql", sql="SELECT * FROM raw_users", upstream=["raw_users"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        decl_files = [f for f in files if not f.relative_path.startswith("quality/")]
        joint_file = [f for f in decl_files if f.joint_name == "clean_users"][0]
        assert joint_file.relative_path == "joints/clean_users.yaml"

    def test_python_joint_in_joints_dir(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
            Joint(name="transform", joint_type="python", function="my_module.transform", upstream=["raw_users"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        decl_files = [f for f in files if not f.relative_path.startswith("quality/")]
        joint_file = [f for f in decl_files if f.joint_name == "transform"][0]
        assert joint_file.relative_path == "joints/transform.yaml"

    def test_sink_in_sinks_dir(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
            Joint(name="output", joint_type="sink", sql="SELECT * FROM raw_users", upstream=["raw_users"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        decl_files = [f for f in files if not f.relative_path.startswith("quality/")]
        sink_file = [f for f in decl_files if f.joint_name == "output"][0]
        assert sink_file.relative_path == "sinks/output.yaml"

    def test_one_file_per_joint(self):
        assembly = _make_assembly([
            Joint(name="a_source", joint_type="source", sql="SELECT * FROM t"),
            Joint(name="b_sql", joint_type="sql", sql="SELECT * FROM a_source", upstream=["a_source"]),
            Joint(name="c_sink", joint_type="sink", sql="SELECT * FROM b_sql", upstream=["b_sql"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        decl_files = [f for f in files if not f.relative_path.startswith("quality/")]
        assert len(decl_files) == 3

    def test_sql_format_uses_sql_extension(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="sql")
        decl_files = [f for f in files if not f.relative_path.startswith("quality/")]
        assert decl_files[0].relative_path == "sources/raw_users.sql"


class TestUpstreamAlwaysEmitted:
    """Test that upstream is always present in output."""

    def test_yaml_upstream_emitted_empty(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        content = files[0].content
        assert "upstream:" in content
        assert "upstream: []" in content

    def test_yaml_upstream_emitted_with_values(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
            Joint(name="clean", joint_type="sql", sql="SELECT * FROM raw_users", upstream=["raw_users"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        clean_file = [f for f in files if f.joint_name == "clean"][0]
        assert "upstream: [raw_users]" in clean_file.content

    def test_sql_upstream_emitted(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="sql")
        assert "-- rivet:upstream: []" in files[0].content


class TestSQLAnnotationFormat:
    """Test SQL output format with -- rivet: annotations."""

    def test_annotations_at_top(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM users"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="sql")
        content = files[0].content
        lines = content.strip().split("\n")
        # Annotations come before SQL body
        annotation_lines = [l for l in lines if l.startswith("-- rivet:")]
        assert len(annotation_lines) >= 3  # name, type, upstream at minimum
        assert "-- rivet:name: raw_users" in content
        assert "-- rivet:type: source" in content

    def test_list_bracket_syntax(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
            Joint(name="j", joint_type="sql", sql="SELECT * FROM src", upstream=["src"], tags=["a", "b"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="sql")
        j_file = [f for f in files if f.joint_name == "j"][0]
        assert "-- rivet:upstream: [src]" in j_file.content
        assert "-- rivet:tags: [a, b]" in j_file.content

    def test_boolean_true_false(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t", eager=True),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="sql")
        assert "-- rivet:eager: true" in files[0].content

    def test_sql_body_after_annotations(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="sql")
        content = files[0].content
        lines = content.strip().split("\n")
        # Find last annotation line and first non-annotation line
        last_annotation_idx = max(i for i, l in enumerate(lines) if l.startswith("-- rivet:"))
        sql_line_idx = next(i for i, l in enumerate(lines) if l == "SELECT * FROM t")
        assert sql_line_idx > last_annotation_idx


class TestQualityCheckFileFormat:
    """Test quality check file generation."""

    def test_sink_section_format(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
            Joint(
                name="output",
                joint_type="sink",
                sql="SELECT * FROM src",
                upstream=["src"],
                assertions=[
                    Assertion(type="not_null", config={"column": "id"}, phase="assertion"),
                    Assertion(type="row_count", config={"min": 1}, phase="audit"),
                ],
            ),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        quality_files = [f for f in files if f.relative_path.startswith("quality/")]
        assert len(quality_files) == 1
        qf = quality_files[0]
        assert qf.relative_path == "quality/output.yaml"
        assert "assertions:" in qf.content
        assert "audits:" in qf.content

    def test_non_sink_flat_format(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
            Joint(
                name="clean",
                joint_type="sql",
                sql="SELECT * FROM src",
                upstream=["src"],
                assertions=[
                    Assertion(type="not_null", config={"column": "id"}, phase="assertion"),
                ],
            ),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        quality_files = [f for f in files if f.relative_path.startswith("quality/")]
        assert len(quality_files) == 1
        qf = quality_files[0]
        assert qf.relative_path == "quality/clean.yaml"
        # Flat format: no assertions: or audits: section headers
        assert "assertions:" not in qf.content
        assert "audits:" not in qf.content
        assert "- type: not_null" in qf.content

    def test_no_quality_file_when_no_checks(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        quality_files = [f for f in files if f.relative_path.startswith("quality/")]
        assert len(quality_files) == 0

    def test_quality_not_inline_in_declaration(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
            Joint(
                name="clean",
                joint_type="sql",
                sql="SELECT * FROM src",
                upstream=["src"],
                assertions=[
                    Assertion(type="not_null", config={"column": "id"}, phase="assertion"),
                ],
            ),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        decl_file = [f for f in files if f.joint_name == "clean" and not f.relative_path.startswith("quality/")][0]
        assert "not_null" not in decl_file.content
        assert "quality" not in decl_file.content


class TestFormatResolutionPriority:
    """Test format resolution: override > source_format > default > yaml."""

    def test_per_joint_override_wins(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="yaml", format_overrides={"src": "sql"}, source_formats={"src": "yaml"})
        assert files[0].relative_path.endswith(".sql")

    def test_source_format_over_default(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="yaml", source_formats={"src": "sql"})
        assert files[0].relative_path.endswith(".sql")

    def test_caller_default_over_yaml(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly, format="sql")
        assert files[0].relative_path.endswith(".sql")

    def test_fallback_to_yaml(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        assert files[0].relative_path.endswith(".yaml")


class TestYAMLDecomposition:
    """Test YAML output with SQL decomposition for source/sink."""

    def test_source_decomposable_sql_produces_columns(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT id, name FROM users"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        content = files[0].content
        assert "columns:" in content
        assert "name: id" in content
        assert "name: name" in content
        assert "sql:" not in content

    def test_source_with_filter_decomposed(self):
        assembly = _make_assembly([
            Joint(name="raw_users", joint_type="source", sql="SELECT id FROM users WHERE active = 1"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        content = files[0].content
        assert "columns:" in content
        assert "filter:" in content

    def test_complex_sql_emitted_as_sql_field(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM a JOIN b ON a.id = b.id"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        content = files[0].content
        assert "sql:" in content

    def test_sql_joint_emits_sql_field(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
            Joint(name="j", joint_type="sql", sql="SELECT * FROM src", upstream=["src"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        j_file = [f for f in files if f.joint_name == "j"][0]
        assert "sql:" in j_file.content

    def test_python_joint_emits_function(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
            Joint(name="py", joint_type="python", function="my_mod.func", upstream=["src"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        py_file = [f for f in files if f.joint_name == "py"][0]
        assert "function: my_mod.func" in py_file.content

    def test_expression_columns_decomposed(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT UPPER(name) AS upper_name FROM users"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        content = files[0].content
        assert "columns:" in content
        assert "name: upper_name" in content
        assert "expression:" in content


class TestOutputSortedByJointName:
    """Test that output is sorted by joint name."""

    def test_sorted_output(self):
        assembly = _make_assembly([
            Joint(name="c_source", joint_type="source", sql="SELECT * FROM t1"),
            Joint(name="a_source", joint_type="source", sql="SELECT * FROM t2"),
            Joint(name="b_source", joint_type="source", sql="SELECT * FROM t3"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        decl_files = [f for f in files if not f.relative_path.startswith("quality/")]
        names = [f.joint_name for f in decl_files]
        assert names == ["a_source", "b_source", "c_source"]


class TestOptionalFieldsInYAML:
    """Test that optional fields are emitted when present."""

    def test_catalog_emitted(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t", catalog="my_catalog"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        assert "catalog: my_catalog" in files[0].content

    def test_engine_emitted(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t", engine="duckdb"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        assert "engine: duckdb" in files[0].content

    def test_write_strategy_emitted(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t"),
            Joint(name="out", joint_type="sink", sql="SELECT * FROM src", upstream=["src"], write_strategy="append"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        out_file = [f for f in files if f.joint_name == "out"][0]
        assert "write_strategy:" in out_file.content
        assert "mode: append" in out_file.content

    def test_tags_emitted(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t", tags=["raw", "users"]),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        assert "tags: [raw, users]" in files[0].content

    def test_description_emitted(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t", description="A source"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        assert "description: A source" in files[0].content

    def test_fusion_strategy_emitted(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t", fusion_strategy_override="never"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        assert "fusion_strategy: never" in files[0].content

    def test_materialization_strategy_emitted(self):
        assembly = _make_assembly([
            Joint(name="src", joint_type="source", sql="SELECT * FROM t", materialization_strategy_override="eager"),
        ])
        gen = DeclarationGenerator()
        files = gen.generate(assembly)
        assert "materialization_strategy: eager" in files[0].content
