"""Tests for catalog command startup helper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.catalog import _startup
from rivet_cli.exit_codes import GENERAL_ERROR


def _globals(**overrides) -> GlobalOptions:
    defaults = dict(profile="default", project_path=Path("."), verbosity=0, color=False)
    defaults.update(overrides)
    return GlobalOptions(**defaults)


class TestStartup:
    """Tests for _startup shared helper."""

    def test_returns_error_on_config_failure(self):
        """Config parse failure → GENERAL_ERROR."""
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.errors = []

        with patch("rivet_cli.commands.catalog.load_config", return_value=mock_result):
            result = _startup(_globals())
        assert result == GENERAL_ERROR

    def test_returns_error_when_no_profile(self):
        """No profile resolved → GENERAL_ERROR."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.profile = None
        mock_result.errors = []

        with patch("rivet_cli.commands.catalog.load_config", return_value=mock_result):
            result = _startup(_globals())
        assert result == GENERAL_ERROR

    def test_returns_error_when_all_catalogs_fail_instantiation(self):
        """All catalogs fail to instantiate → GENERAL_ERROR."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.profile = MagicMock()
        mock_result.errors = []

        with (
            patch("rivet_cli.commands.catalog.load_config", return_value=mock_result),
            patch("rivet_cli.commands.catalog.CatalogInstantiator") as mock_ci,
            patch("rivet_cli.commands.catalog.EngineInstantiator") as mock_ei,
            patch("rivet_cli.commands.catalog.PluginRegistry"),
            patch("rivet_cli.commands.catalog.register_optional_plugins"),
        ):
            mock_ci.return_value.instantiate_all.return_value = ({}, [])
            mock_ei.return_value.instantiate_all.return_value = ({}, [])
            result = _startup(_globals())
        assert result == GENERAL_ERROR

    def test_returns_error_when_all_catalogs_fail_connection(self):
        """All catalogs fail to connect → GENERAL_ERROR."""
        from rivet_core import Catalog

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.profile = MagicMock()
        mock_result.errors = []

        cat = Catalog(name="db", type="duckdb", options={})
        mock_plugin = MagicMock()
        mock_plugin.list_tables.side_effect = ConnectionError("refused")
        mock_plugin.test_connection.side_effect = ConnectionError("refused")

        with (
            patch("rivet_cli.commands.catalog.load_config", return_value=mock_result),
            patch("rivet_cli.commands.catalog.CatalogInstantiator") as mock_ci,
            patch("rivet_cli.commands.catalog.EngineInstantiator") as mock_ei,
            patch("rivet_cli.commands.catalog.PluginRegistry") as mock_reg_cls,
            patch("rivet_cli.commands.catalog.register_optional_plugins"),
        ):
            mock_ci.return_value.instantiate_all.return_value = ({"db": cat}, [])
            mock_ei.return_value.instantiate_all.return_value = ({}, [])
            mock_reg_cls.return_value.get_catalog_plugin.return_value = mock_plugin
            result = _startup(_globals())
        assert result == GENERAL_ERROR

    def test_returns_explorer_on_success(self):
        """At least one catalog connects → CatalogExplorer returned."""
        from rivet_core import Catalog, CatalogExplorer

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.profile = MagicMock()
        mock_result.errors = []

        cat = Catalog(name="db", type="duckdb", options={})
        mock_plugin = MagicMock()
        mock_plugin.list_tables.return_value = []

        with (
            patch("rivet_cli.commands.catalog.load_config", return_value=mock_result),
            patch("rivet_cli.commands.catalog.CatalogInstantiator") as mock_ci,
            patch("rivet_cli.commands.catalog.EngineInstantiator") as mock_ei,
            patch("rivet_cli.commands.catalog.PluginRegistry") as mock_reg_cls,
            patch("rivet_cli.commands.catalog.register_optional_plugins"),
        ):
            mock_ci.return_value.instantiate_all.return_value = ({"db": cat}, [])
            mock_ei.return_value.instantiate_all.return_value = ({}, [])
            mock_reg_cls.return_value.get_catalog_plugin.return_value = mock_plugin
            result = _startup(_globals())
        assert isinstance(result, CatalogExplorer)

    def test_partial_connection_failure_returns_explorer(self):
        """Some catalogs fail, some succeed → CatalogExplorer returned with correct status."""
        from rivet_core import Catalog, CatalogExplorer

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.profile = MagicMock()
        mock_result.errors = []

        good_cat = Catalog(name="good", type="duckdb", options={})
        bad_cat = Catalog(name="bad", type="duckdb", options={})


        def _list_tables(catalog):
            if catalog.name == "bad":
                raise ConnectionError("refused")
            return []

        mock_plugin = MagicMock()
        mock_plugin.list_tables.side_effect = _list_tables
        mock_plugin.test_connection.side_effect = _list_tables

        with (
            patch("rivet_cli.commands.catalog.load_config", return_value=mock_result),
            patch("rivet_cli.commands.catalog.CatalogInstantiator") as mock_ci,
            patch("rivet_cli.commands.catalog.EngineInstantiator") as mock_ei,
            patch("rivet_cli.commands.catalog.PluginRegistry") as mock_reg_cls,
            patch("rivet_cli.commands.catalog.register_optional_plugins"),
        ):
            mock_ci.return_value.instantiate_all.return_value = (
                {"good": good_cat, "bad": bad_cat}, []
            )
            mock_ei.return_value.instantiate_all.return_value = ({}, [])
            mock_reg_cls.return_value.get_catalog_plugin.return_value = mock_plugin
            result = _startup(_globals())

        assert isinstance(result, CatalogExplorer)
        # Verify connection status was injected
        assert result._connection_status["good"] == (True, None)
        assert result._connection_status["bad"][0] is False

    def test_uses_project_path_and_profile(self):
        """Startup passes project_path and profile to load_config."""
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.errors = []

        with patch("rivet_cli.commands.catalog.load_config", return_value=mock_result) as mock_load:
            _startup(_globals(project_path=Path("/my/project"), profile="staging"))
        mock_load.assert_called_once_with(Path("/my/project"), "staging")


class TestCatalogSearch:
    """Tests for catalog_search handler (task 15.1)."""

    def _make_explorer(self, results=None):
        explorer = MagicMock()
        explorer.search.return_value = results or []
        return explorer

    def test_text_format_returns_success(self):
        """catalog_search with text format returns SUCCESS (0)."""
        from rivet_cli.commands.catalog import catalog_search
        from rivet_cli.exit_codes import SUCCESS

        explorer = self._make_explorer()
        result = catalog_search(explorer, query="orders", limit=20, format="text", globals=_globals())
        assert result == SUCCESS

    def test_json_format_returns_success(self):
        """catalog_search with json format returns SUCCESS (0)."""
        from rivet_cli.commands.catalog import catalog_search
        from rivet_cli.exit_codes import SUCCESS

        explorer = self._make_explorer()
        result = catalog_search(explorer, query="orders", limit=20, format="json", globals=_globals())
        assert result == SUCCESS

    def test_calls_search_with_query_and_limit(self):
        """catalog_search passes query and limit to explorer.search()."""
        from rivet_cli.commands.catalog import catalog_search

        explorer = self._make_explorer()
        catalog_search(explorer, query="usr", limit=5, format="text", globals=_globals())
        explorer.search.assert_called_once_with("usr", limit=5)

    def test_json_output_is_valid_json(self, capsys):
        """catalog_search --format json prints valid JSON array."""
        import json

        from rivet_cli.commands.catalog import catalog_search
        from rivet_core.catalog_explorer import SearchResult

        result = SearchResult(
            kind="table",
            qualified_name="db.public.orders",
            short_name="orders",
            parent="db.public",
            match_positions=[0, 1],
            score=0.5,
            node_type="table",
        )
        explorer = self._make_explorer(results=[result])
        catalog_search(explorer, query="ord", limit=20, format="json", globals=_globals())
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert isinstance(parsed, list)
        assert parsed[0]["qualified_name"] == "db.public.orders"
        assert parsed[0]["kind"] == "table"

    def test_text_output_contains_qualified_name(self, capsys):
        """catalog_search text format includes qualified name in output."""
        from rivet_cli.commands.catalog import catalog_search
        from rivet_core.catalog_explorer import SearchResult

        result = SearchResult(
            kind="table",
            qualified_name="db.public.orders",
            short_name="orders",
            parent="db.public",
            match_positions=[0],
            score=0.3,
            node_type="table",
        )
        explorer = self._make_explorer(results=[result])
        catalog_search(explorer, query="ord", limit=20, format="text", globals=_globals())
        captured = capsys.readouterr()
        assert "db.public.orders" in captured.out

    def test_empty_results_text(self, capsys):
        """catalog_search with no results prints 'No results found.'."""
        from rivet_cli.commands.catalog import catalog_search

        explorer = self._make_explorer(results=[])
        catalog_search(explorer, query="zzz", limit=20, format="text", globals=_globals())
        captured = capsys.readouterr()
        assert "No results found." in captured.out

    def test_empty_results_json(self, capsys):
        """catalog_search with no results prints empty JSON array."""
        import json

        from rivet_cli.commands.catalog import catalog_search

        explorer = self._make_explorer(results=[])
        catalog_search(explorer, query="zzz", limit=20, format="json", globals=_globals())
        captured = capsys.readouterr()
        assert json.loads(captured.out) == []


class TestCatalogGenerate:
    """Tests for catalog_generate handler (task 16.1)."""

    def _make_explorer(self, schema_columns=None):
        """Build a mock CatalogExplorer with generate_source configured."""
        from rivet_core.catalog_explorer import GeneratedSource

        if schema_columns is None:
            schema_columns = [("id", "int64"), ("name", "utf8")]

        col_count = len(schema_columns)
        yaml_content = (
            "name: raw_users\ntype: source\ncatalog: mydb\ntable: public.users\n"
            "columns:\n" + "".join(f"  - name: {c}\n    type: {t}\n" for c, t in schema_columns)
            + "upstream: []\n"
        )
        sql_content = (
            "-- rivet:name: raw_users\n-- rivet:type: source\n"
            "-- rivet:catalog: mydb\n-- rivet:table: public.users\n"
        )

        explorer = MagicMock()
        explorer.generate_source.side_effect = lambda path, format="yaml", columns=None: GeneratedSource(
            content=yaml_content if format == "yaml" else sql_content,
            format=format,
            suggested_filename=f"raw_users.{format}",
            catalog_name="mydb",
            table_name="public.users",
            column_count=col_count if columns is None else len(columns),
        )
        return explorer

    def test_stdout_prints_yaml(self, capsys):
        """--stdout prints YAML content to stdout, returns 0."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import SUCCESS

        explorer = self._make_explorer()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="yaml",
            output=None,
            stdout=True,
            name=None,
            columns=None,
            globals=g,
        )
        assert result == SUCCESS
        captured = capsys.readouterr()
        assert "raw_users" in captured.out
        assert "type: source" in captured.out

    def test_stdout_prints_sql(self, capsys):
        """--stdout with --format sql prints SQL content."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import SUCCESS

        explorer = self._make_explorer()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="sql",
            output=None,
            stdout=True,
            name=None,
            columns=None,
            globals=g,
        )
        assert result == SUCCESS
        captured = capsys.readouterr()
        assert "-- rivet:name:" in captured.out
        assert "-- rivet:type: source" in captured.out

    def test_writes_to_default_sources_dir(self, tmp_path, monkeypatch):
        """Default: writes YAML to sources/<name>.yaml."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import SUCCESS

        monkeypatch.chdir(tmp_path)
        explorer = self._make_explorer()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="yaml",
            output=None,
            stdout=False,
            name=None,
            columns=None,
            globals=g,
        )
        assert result == SUCCESS
        out_file = tmp_path / "sources" / "raw_users.yaml"
        assert out_file.exists()
        assert "type: source" in out_file.read_text()

    def test_writes_to_custom_output_path(self, tmp_path):
        """--output <path> writes to specified path."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import SUCCESS

        out_file = tmp_path / "out.yaml"
        explorer = self._make_explorer()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="yaml",
            output=str(out_file),
            stdout=False,
            name=None,
            columns=None,
            globals=g,
        )
        assert result == SUCCESS
        assert out_file.exists()

    def test_invalid_path_returns_usage_error(self, capsys):
        """Single-segment path → USAGE_ERROR."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import USAGE_ERROR

        explorer = MagicMock()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="onlyone",
            format="yaml",
            output=None,
            stdout=True,
            name=None,
            columns=None,
            globals=g,
        )
        assert result == USAGE_ERROR
        captured = capsys.readouterr()
        assert "RVT-874" in captured.err

    def test_generate_source_error_returns_usage_error(self, capsys):
        """CatalogExplorerError from generate_source → USAGE_ERROR."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import USAGE_ERROR
        from rivet_core.catalog_explorer import CatalogExplorerError
        from rivet_core.errors import RivetError

        explorer = MagicMock()
        explorer.generate_source.side_effect = CatalogExplorerError(
            RivetError(code="RVT-874", message="Schema not available")
        )
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="yaml",
            output=None,
            stdout=True,
            name=None,
            columns=None,
            globals=g,
        )
        assert result == USAGE_ERROR
        captured = capsys.readouterr()
        assert "RVT-874" in captured.err

    def test_file_write_failure_returns_usage_error(self, tmp_path, capsys):
        """OSError on file write → USAGE_ERROR with RVT-877."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import USAGE_ERROR

        # Use a path that can't be written (file as directory)
        bad_path = tmp_path / "not_a_dir"
        bad_path.write_text("block")
        out_path = str(bad_path / "out.yaml")

        explorer = self._make_explorer()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="yaml",
            output=out_path,
            stdout=False,
            name=None,
            columns=None,
            globals=g,
        )
        assert result == USAGE_ERROR
        captured = capsys.readouterr()
        assert "RVT-877" in captured.err

    def test_name_override_in_yaml(self, capsys):
        """--name overrides auto-generated name in YAML output."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import SUCCESS

        explorer = self._make_explorer()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="yaml",
            output=None,
            stdout=True,
            name="my_custom_name",
            columns=None,
            globals=g,
        )
        assert result == SUCCESS
        captured = capsys.readouterr()
        assert "my_custom_name" in captured.out

    def test_columns_passed_to_generate_source(self):
        """--columns list is forwarded to explorer.generate_source."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import SUCCESS

        explorer = self._make_explorer()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="yaml",
            output=None,
            stdout=True,
            name=None,
            columns=["id", "name"],
            globals=g,
        )
        assert result == SUCCESS
        explorer.generate_source.assert_called_once_with(
            ["mydb", "public", "users"], format="yaml", columns=["id", "name"]
        )

    def test_confirmation_message_on_file_write(self, tmp_path, monkeypatch, capsys):
        """Successful file write prints confirmation message."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import SUCCESS

        monkeypatch.chdir(tmp_path)
        explorer = self._make_explorer()
        g = _globals()
        result = catalog_generate(
            explorer=explorer,
            path="mydb.public.users",
            format="yaml",
            output=None,
            stdout=False,
            name=None,
            columns=None,
            globals=g,
        )
        assert result == SUCCESS
        captured = capsys.readouterr()
        assert "Generated" in captured.out or "sources" in captured.out


class TestExitCodes:
    """Task 20.1: Exit codes wired across all catalog explorer commands.

    Requirements: 16.1, 16.2, 16.3, 16.4, 16.5
    """

    def _make_explorer(self):
        from rivet_core.catalog_explorer import CatalogInfo
        explorer = MagicMock()
        explorer.list_catalogs.return_value = [
            CatalogInfo(name="db", catalog_type="duckdb", connected=True, error=None)
        ]
        explorer.list_children.return_value = []
        explorer.search.return_value = []
        return explorer

    # ── catalog list ──────────────────────────────────────────────────

    def test_catalog_list_success_exits_0(self, capsys):
        """catalog_list returns 0 on success (Req 16.1)."""
        from rivet_cli.commands.catalog import catalog_list
        from rivet_cli.exit_codes import SUCCESS

        explorer = self._make_explorer()
        result = catalog_list(explorer=explorer, catalog_name=None, depth=0, format="text", globals=_globals())
        assert result == SUCCESS

    def test_catalog_list_unknown_catalog_exits_10(self, capsys):
        """catalog_list with unknown catalog name exits 10 (Req 16.3)."""
        from rivet_cli.commands.catalog import catalog_list
        from rivet_cli.exit_codes import USAGE_ERROR

        explorer = self._make_explorer()
        result = catalog_list(explorer=explorer, catalog_name="nonexistent", depth=0, format="text", globals=_globals())
        assert result == USAGE_ERROR

    def test_catalog_list_partial_failure_exits_0(self, capsys):
        """catalog_list with some disconnected catalogs still exits 0 (Req 16.5)."""
        from rivet_cli.commands.catalog import catalog_list
        from rivet_cli.exit_codes import SUCCESS
        from rivet_core.catalog_explorer import CatalogInfo

        explorer = MagicMock()
        explorer.list_catalogs.return_value = [
            CatalogInfo(name="good", catalog_type="duckdb", connected=True, error=None),
            CatalogInfo(name="bad", catalog_type="postgres", connected=False, error="refused"),
        ]
        explorer.list_children.return_value = []
        result = catalog_list(explorer=explorer, catalog_name=None, depth=0, format="text", globals=_globals())
        assert result == SUCCESS

    # ── catalog describe ──────────────────────────────────────────────

    def test_catalog_describe_invalid_path_exits_10(self, capsys):
        """catalog_describe with single-segment path exits 10 (Req 16.3)."""
        from rivet_cli.commands.catalog import catalog_describe
        from rivet_cli.exit_codes import USAGE_ERROR

        explorer = self._make_explorer()
        result = catalog_describe(explorer=explorer, path="onlyone", stats=False, format="text", globals=_globals())
        assert result == USAGE_ERROR

    def test_catalog_describe_unknown_catalog_exits_10(self, capsys):
        """catalog_describe with unknown catalog exits 10 (Req 16.3)."""
        from rivet_cli.commands.catalog import catalog_describe
        from rivet_cli.exit_codes import USAGE_ERROR

        explorer = self._make_explorer()
        result = catalog_describe(explorer=explorer, path="unknown.schema.table", stats=False, format="text", globals=_globals())
        assert result == USAGE_ERROR

    def test_catalog_describe_success_exits_0(self, capsys):
        """catalog_describe returns 0 on success (Req 16.1)."""
        from rivet_cli.commands.catalog import catalog_describe
        from rivet_cli.exit_codes import SUCCESS
        from rivet_core.catalog_explorer import ExplorerNode, NodeDetail

        node = ExplorerNode(name="users", node_type="table", path=["db", "public", "users"], is_expandable=False, depth=2, summary=None, depth_limit_reached=False)
        detail = NodeDetail(node=node, schema=MagicMock(), metadata=None, children_count=None)
        explorer = self._make_explorer()
        explorer.get_node_detail.return_value = detail
        result = catalog_describe(explorer=explorer, path="db.public.users", stats=False, format="json", globals=_globals())
        assert result == SUCCESS

    # ── catalog search ────────────────────────────────────────────────

    def test_catalog_search_success_exits_0(self):
        """catalog_search returns 0 on success (Req 16.1)."""
        from rivet_cli.commands.catalog import catalog_search
        from rivet_cli.exit_codes import SUCCESS

        explorer = self._make_explorer()
        result = catalog_search(explorer=explorer, query="test", limit=20, format="text", globals=_globals())
        assert result == SUCCESS

    # ── catalog generate ──────────────────────────────────────────────

    def test_catalog_generate_success_exits_0(self, capsys):
        """catalog_generate returns 0 on success (Req 16.1)."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import SUCCESS
        from rivet_core.catalog_explorer import GeneratedSource

        explorer = MagicMock()
        explorer.generate_source.return_value = GeneratedSource(
            content="name: raw_t\ntype: source\n",
            format="yaml",
            suggested_filename="raw_t.yaml",
            catalog_name="db",
            table_name="t",
            column_count=1,
        )
        result = catalog_generate(explorer=explorer, path="db.public.t", format="yaml", output=None, stdout=True, name=None, columns=None, globals=_globals())
        assert result == SUCCESS

    def test_catalog_generate_invalid_path_exits_10(self, capsys):
        """catalog_generate with bad path exits 10 (Req 16.3)."""
        from rivet_cli.commands.catalog import catalog_generate
        from rivet_cli.exit_codes import USAGE_ERROR

        result = catalog_generate(explorer=MagicMock(), path="bad", format="yaml", output=None, stdout=True, name=None, columns=None, globals=_globals())
        assert result == USAGE_ERROR

    # ── _main SIGINT → 130 ────────────────────────────────────────────

    def test_main_sigint_exits_130(self):
        """KeyboardInterrupt in _main returns INTERRUPTED (130) (Req 16.4)."""
        from rivet_cli.app import _main
        from rivet_cli.exit_codes import INTERRUPTED

        with patch("rivet_cli.app._dispatch", side_effect=KeyboardInterrupt):
            result = _main(["catalog", "list"])
        assert result == INTERRUPTED

    # ── explore SIGINT → 130 ─────────────────────────────────────────

    def test_dispatch_explore_sigint_exits_130(self):
        """_dispatch_explore returns INTERRUPTED (130) on KeyboardInterrupt (Req 16.4)."""
        import argparse

        from rivet_cli.app import _dispatch_explore
        from rivet_cli.exit_codes import INTERRUPTED

        args = argparse.Namespace(profile="default", project=None, verbose=0, quiet=False, no_color=False)
        globals_ = _globals()

        with (
            patch("rivet_cli.commands.catalog._startup") as mock_startup,
            patch("rivet_cli.commands.explore.ExploreController") as mock_ctrl_cls,
        ):
            mock_startup.return_value = MagicMock()
            mock_ctrl = MagicMock()
            mock_ctrl.run.side_effect = KeyboardInterrupt
            mock_ctrl_cls.return_value = mock_ctrl
            result = _dispatch_explore(args, globals_)

        assert result == INTERRUPTED

    # ── all catalogs fail → exit 1 ────────────────────────────────────

    def test_startup_all_fail_exits_1(self):
        """_startup returns GENERAL_ERROR (1) when all catalogs fail (Req 16.2)."""
        from rivet_cli.exit_codes import GENERAL_ERROR

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.profile = MagicMock()
        mock_result.errors = []

        with (
            patch("rivet_cli.commands.catalog.load_config", return_value=mock_result),
            patch("rivet_cli.commands.catalog.CatalogInstantiator") as mock_ci,
            patch("rivet_cli.commands.catalog.EngineInstantiator") as mock_ei,
            patch("rivet_cli.commands.catalog.PluginRegistry") as mock_reg,
            patch("rivet_cli.commands.catalog.register_optional_plugins"),
        ):
            from rivet_core import Catalog
            cat = Catalog(name="db", type="duckdb", options={})
            mock_ci.return_value.instantiate_all.return_value = ({"db": cat}, [])
            mock_ei.return_value.instantiate_all.return_value = ({}, [])
            mock_plugin = MagicMock()
            mock_plugin.list_tables.side_effect = Exception("refused")
            mock_plugin.test_connection.side_effect = Exception("refused")
            mock_reg.return_value.get_catalog_plugin.return_value = mock_plugin

            result = _startup(_globals())
        assert result == GENERAL_ERROR
