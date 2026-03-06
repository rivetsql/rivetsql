"""Unit tests for rivet_core.testing.fixtures.load_inline_data and load_fixture."""


import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rivet_core.testing.fixtures import FixtureError, load_fixture, load_inline_data


class TestBasicConstruction:
    def test_columns_and_rows(self):
        t = load_inline_data(["a", "b"], [[1, "x"], [2, "y"]])
        assert t.column_names == ["a", "b"]
        assert t.num_rows == 2

    def test_empty_rows(self):
        t = load_inline_data(["x", "y"], [])
        assert t.num_rows == 0
        assert t.column_names == ["x", "y"]

    def test_single_column(self):
        t = load_inline_data(["v"], [[10], [20]])
        assert t.num_rows == 2
        assert t.column("v").to_pylist() == [10, 20]


class TestExplicitTypes:
    def test_int8(self):
        t = load_inline_data(["n"], [[1], [127]], types=["int8"])
        assert t.schema.field("n").type == pa.int8()

    def test_int16(self):
        t = load_inline_data(["n"], [[1]], types=["int16"])
        assert t.schema.field("n").type == pa.int16()

    def test_int32(self):
        t = load_inline_data(["n"], [[1]], types=["int32"])
        assert t.schema.field("n").type == pa.int32()

    def test_int64(self):
        t = load_inline_data(["n"], [[1]], types=["int64"])
        assert t.schema.field("n").type == pa.int64()

    def test_uint8(self):
        t = load_inline_data(["n"], [[1]], types=["uint8"])
        assert t.schema.field("n").type == pa.uint8()

    def test_uint16(self):
        t = load_inline_data(["n"], [[1]], types=["uint16"])
        assert t.schema.field("n").type == pa.uint16()

    def test_uint32(self):
        t = load_inline_data(["n"], [[1]], types=["uint32"])
        assert t.schema.field("n").type == pa.uint32()

    def test_uint64(self):
        t = load_inline_data(["n"], [[1]], types=["uint64"])
        assert t.schema.field("n").type == pa.uint64()

    def test_float16(self):
        t = load_inline_data(["n"], [[1.0]], types=["float16"])
        assert t.schema.field("n").type == pa.float16()

    def test_float32(self):
        t = load_inline_data(["n"], [[1.5]], types=["float32"])
        assert t.schema.field("n").type == pa.float32()

    def test_float64(self):
        t = load_inline_data(["n"], [[1.5]], types=["float64"])
        assert t.schema.field("n").type == pa.float64()

    def test_string(self):
        t = load_inline_data(["s"], [["hello"]], types=["string"])
        assert t.schema.field("s").type == pa.utf8()

    def test_bool(self):
        t = load_inline_data(["b"], [[True], [False]], types=["bool"])
        assert t.schema.field("b").type == pa.bool_()
        assert t.column("b").to_pylist() == [True, False]

    def test_date32(self):
        t = load_inline_data(["d"], [["2024-01-15"]], types=["date32"])
        assert t.schema.field("d").type == pa.date32()

    def test_timestamp_us(self):
        t = load_inline_data(["ts"], [["2024-01-15T10:30:00"]], types=["timestamp[us]"])
        assert t.schema.field("ts").type == pa.timestamp("us")

    def test_binary(self):
        t = load_inline_data(["b"], [[b"abc"]], types=["binary"])
        assert t.schema.field("b").type == pa.binary()

    def test_decimal128(self):
        t = load_inline_data(["d"], [["3.14"]], types=["decimal128(10, 2)"])
        assert pa.types.is_decimal(t.schema.field("d").type)

    def test_multiple_columns_different_types(self):
        t = load_inline_data(
            ["i", "f", "s"],
            [[1, 1.5, "hello"]],
            types=["int32", "float64", "string"],
        )
        assert t.schema.field("i").type == pa.int32()
        assert t.schema.field("f").type == pa.float64()
        assert t.schema.field("s").type == pa.utf8()


class TestTypeInference:
    def test_infer_int(self):
        t = load_inline_data(["n"], [[1], [2]])
        assert t.schema.field("n").type == pa.int64()

    def test_infer_float(self):
        t = load_inline_data(["n"], [[1.5], [2.5]])
        assert t.schema.field("n").type == pa.float64()

    def test_infer_bool(self):
        t = load_inline_data(["b"], [[True], [False]])
        assert t.schema.field("b").type == pa.bool_()

    def test_infer_datetime(self):
        t = load_inline_data(["ts"], [["2024-01-15T10:30:00"]])
        assert t.schema.field("ts").type == pa.timestamp("us")

    def test_infer_date(self):
        t = load_inline_data(["d"], [["2024-01-15"]])
        assert t.schema.field("d").type == pa.date32()

    def test_infer_string(self):
        t = load_inline_data(["s"], [["hello"], ["world"]])
        assert t.schema.field("s").type == pa.utf8()

    def test_infer_all_null_defaults_to_utf8(self):
        t = load_inline_data(["s"], [[None], [None]])
        assert t.schema.field("s").type == pa.utf8()

    def test_infer_uses_first_non_null(self):
        t = load_inline_data(["n"], [[None], [42]])
        assert t.schema.field("n").type == pa.int64()


class TestNullHandling:
    def test_none_becomes_arrow_null(self):
        t = load_inline_data(["n"], [[1], [None], [3]])
        col = t.column("n").to_pylist()
        assert col[1] is None

    def test_all_nulls(self):
        t = load_inline_data(["n"], [[None], [None]])
        assert t.column("n").null_count == 2

    def test_null_with_explicit_type(self):
        t = load_inline_data(["n"], [[None], [5]], types=["int64"])
        col = t.column("n").to_pylist()
        assert col[0] is None
        assert col[1] == 5


class TestTypesLengthMismatch:
    def test_too_few_types_raises_rvt902(self):
        with pytest.raises(FixtureError) as exc_info:
            load_inline_data(["a", "b"], [[1, 2]], types=["int64"])
        assert exc_info.value.error.code == "RVT-902"

    def test_too_many_types_raises_rvt902(self):
        with pytest.raises(FixtureError) as exc_info:
            load_inline_data(["a"], [[1]], types=["int64", "float64"])
        assert exc_info.value.error.code == "RVT-902"


class TestLoadFixtureDispatcher:
    def test_inline_via_load_fixture(self, tmp_path):
        spec = {"columns": ["x", "y"], "rows": [[1, "a"], [2, "b"]]}
        t = load_fixture(spec, tmp_path)
        assert t.num_rows == 2
        assert t.column_names == ["x", "y"]

    def test_inline_with_types_via_load_fixture(self, tmp_path):
        spec = {"columns": ["n"], "rows": [[1], [2]], "types": ["int32"]}
        t = load_fixture(spec, tmp_path)
        assert t.schema.field("n").type == pa.int32()

    def test_file_via_load_fixture(self, tmp_path):
        table = pa.table({"id": [10, 20]})
        pq.write_table(table, tmp_path / "data.parquet")
        spec = {"file": "data.parquet"}
        t = load_fixture(spec, tmp_path)
        assert t.equals(table)

    def test_file_missing_raises_rvt901(self, tmp_path):
        spec = {"file": "missing.parquet"}
        with pytest.raises(FixtureError) as exc_info:
            load_fixture(spec, tmp_path)
        assert exc_info.value.error.code == "RVT-901"
