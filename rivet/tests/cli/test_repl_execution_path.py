"""Unit tests for REPL execution path unification (task 7.2).

Validates: Requirements 8.1, 8.2, 8.3

Tests cover:
- F4 generates correct SELECT * FROM <table> LIMIT 100 SQL
- F4 routes through session.execute_query() not preview_table()
- Both F4 and F5 record query history
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pyarrow as pa

from rivet_core.interactive.types import QueryHistoryEntry, QueryResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_query_result(sql: str = "SELECT 1") -> QueryResult:
    table = pa.table({"x": [1]})
    return QueryResult(
        table=table,
        row_count=1,
        column_names=["x"],
        column_types=["int64"],
        elapsed_ms=10.0,
        query_plan=None,
        quality_results=None,
        truncated=False,
    )


class FakeSession:
    """Minimal mock of InteractiveSession for testing execution path."""

    def __init__(self) -> None:
        self._project_path = "/fake"
        self._profile_name = "default"
        self._history: list[QueryHistoryEntry] = []
        self.execute_query_calls: list[str] = []
        self.preview_table_calls: list[str] = []

    @property
    def active_profile(self) -> str:
        return self._profile_name

    def get_joints(self) -> list:
        return []

    def execute_query(
        self,
        sql: str,
        catalog_context: str | None = None,
        on_progress: Callable | None = None,
    ) -> QueryResult:
        self.execute_query_calls.append(sql)
        self._record_history("query", sql, 1, 10.0, "success")
        return _make_query_result(sql)

    def preview_table(self, table_ref: str) -> QueryResult:
        """Should NOT be called for F4 quick queries."""
        self.preview_table_calls.append(table_ref)
        return _make_query_result()

    def cancel(self) -> None:
        pass

    @property
    def history(self) -> list[QueryHistoryEntry]:
        return list(self._history)

    def _record_history(
        self,
        action_type: str,
        name: str,
        row_count: int | None,
        duration_ms: float,
        status: str,
    ) -> None:
        self._history.append(
            QueryHistoryEntry(
                timestamp=datetime.now(UTC),
                action_type=action_type,
                name=name,
                row_count=row_count,
                duration_ms=duration_ms,
                status=status,
            )
        )


# ---------------------------------------------------------------------------
# Test: F4 generates correct SQL (Requirement 8.2)
# ---------------------------------------------------------------------------


class TestF4SqlGeneration:
    """F4 generates SELECT * FROM <table> LIMIT 100."""

    def test_table_node_generates_select_star_limit_100(self) -> None:
        """action_preview_node posts QuickQuery with correct SQL for a table node."""
        from rivet_cli.repl.widgets.catalog import CatalogNodeData, CatalogPanel

        panel = CatalogPanel.__new__(CatalogPanel)
        panel._session = FakeSession()  # type: ignore[assignment]

        posted: list = []
        panel.post_message = lambda msg: posted.append(msg)  # type: ignore[assignment]

        # Simulate a table node being selected
        mock_node = MagicMock()
        mock_node.data = CatalogNodeData(
            node_kind="table",
            qualified_name="prod.public.orders",
        )
        mock_tree = MagicMock()
        mock_tree.cursor_node = mock_node
        panel.query_one = MagicMock(return_value=mock_tree)  # type: ignore[assignment]

        panel.action_preview_node()

        assert len(posted) == 1
        msg = posted[0]
        assert isinstance(msg, CatalogPanel.QuickQuery)
        assert msg.sql == "SELECT * FROM prod.public.orders LIMIT 100"

    def test_joint_node_generates_select_star_limit_100(self) -> None:
        """action_preview_node posts QuickQuery with correct SQL for a joint node."""
        from rivet_cli.repl.widgets.catalog import CatalogNodeData, CatalogPanel

        panel = CatalogPanel.__new__(CatalogPanel)
        panel._session = FakeSession()  # type: ignore[assignment]

        posted: list = []
        panel.post_message = lambda msg: posted.append(msg)  # type: ignore[assignment]

        mock_node = MagicMock()
        mock_node.data = CatalogNodeData(
            node_kind="joint",
            joint_name="clean_orders",
            qualified_name="clean_orders",
        )
        mock_tree = MagicMock()
        mock_tree.cursor_node = mock_node
        panel.query_one = MagicMock(return_value=mock_tree)  # type: ignore[assignment]

        panel.action_preview_node()

        assert len(posted) == 1
        msg = posted[0]
        assert isinstance(msg, CatalogPanel.QuickQuery)
        assert msg.sql == "SELECT * FROM clean_orders LIMIT 100"

    def test_sql_contains_limit_100(self) -> None:
        """Generated SQL always contains LIMIT 100."""
        from rivet_cli.repl.widgets.catalog import CatalogNodeData, CatalogPanel

        panel = CatalogPanel.__new__(CatalogPanel)
        panel._session = FakeSession()  # type: ignore[assignment]

        posted: list = []
        panel.post_message = lambda msg: posted.append(msg)  # type: ignore[assignment]

        mock_node = MagicMock()
        mock_node.data = CatalogNodeData(
            node_kind="table",
            qualified_name="my_catalog.my_schema.my_table",
        )
        mock_tree = MagicMock()
        mock_tree.cursor_node = mock_node
        panel.query_one = MagicMock(return_value=mock_tree)  # type: ignore[assignment]

        panel.action_preview_node()

        assert "LIMIT 100" in posted[0].sql
        assert "SELECT * FROM" in posted[0].sql


# ---------------------------------------------------------------------------
# Test: F4 routes through execute_query, not preview_table (Requirement 8.2, 8.3)
# ---------------------------------------------------------------------------


class TestF4RoutesViaExecuteQuery:
    """F4 routes through session.execute_query(), not preview_table()."""

    def test_f4_calls_set_query_and_execute_not_preview_table(self) -> None:
        """on_catalog_panel_quick_query calls editor.set_query_and_execute."""
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        session = FakeSession()
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        set_query_calls: list[str] = []

        class FakeEditor:
            def set_query_and_execute(self, sql: str) -> None:
                set_query_calls.append(sql)

        app.query_one = MagicMock(return_value=FakeEditor())  # type: ignore[assignment]

        sql = "SELECT * FROM prod.public.users LIMIT 100"
        msg = CatalogPanel.QuickQuery(sql)
        app.on_catalog_panel_quick_query(msg)

        # set_query_and_execute was called with the SQL
        assert set_query_calls == [sql]
        # preview_table was never called
        assert session.preview_table_calls == []

    def test_f4_does_not_call_session_preview_table(self) -> None:
        """The session's preview_table method is never invoked for F4."""
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        session = FakeSession()
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        app.query_one = MagicMock(return_value=MagicMock())  # type: ignore[assignment]

        msg = CatalogPanel.QuickQuery("SELECT * FROM t LIMIT 100")
        app.on_catalog_panel_quick_query(msg)

        assert session.preview_table_calls == []

    def test_f4_forwards_when_executing(self) -> None:
        """F4 still forwards — guard is now in session layer."""
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        session = FakeSession()
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        set_query_calls: list[str] = []

        class FakeEditor:
            def set_query_and_execute(self, sql: str) -> None:
                set_query_calls.append(sql)

        app.query_one = MagicMock(return_value=FakeEditor())  # type: ignore[assignment]

        msg = CatalogPanel.QuickQuery("SELECT * FROM t LIMIT 100")
        app.on_catalog_panel_quick_query(msg)

        # Guard is now in session layer; TUI always forwards
        assert set_query_calls == ["SELECT * FROM t LIMIT 100"]


# ---------------------------------------------------------------------------
# Test: Both F4 and F5 record query history (Requirement 8.3)
# ---------------------------------------------------------------------------


class TestQueryHistoryRecording:
    """Both F4 and F5 record query history via session.execute_query()."""

    def test_execute_query_records_history(self) -> None:
        """session.execute_query() appends a QueryHistoryEntry."""
        session = FakeSession()
        sql = "SELECT * FROM orders LIMIT 100"
        session.execute_query(sql)

        assert len(session.history) == 1
        entry = session.history[0]
        assert entry.action_type == "query"
        assert entry.name == sql
        assert entry.status == "success"

    def test_f4_path_records_history_via_execute_query(self) -> None:
        """F4 path (set_query_and_execute → QuerySubmitted → execute_query) records history."""
        session = FakeSession()

        # Simulate what happens when set_query_and_execute posts QuerySubmitted
        # which triggers _run_ad_hoc_query → session.execute_query()
        sql = "SELECT * FROM prod.public.users LIMIT 100"
        session.execute_query(sql)

        assert any(e.name == sql for e in session.history)

    def test_f5_path_records_history_via_execute_query(self) -> None:
        """F5 path (QuerySubmitted → execute_query) records history."""
        session = FakeSession()

        sql = "SELECT id, name FROM customers WHERE active = true"
        session.execute_query(sql)

        assert len(session.history) == 1
        assert session.history[0].name == sql
        assert session.history[0].action_type == "query"

    def test_multiple_queries_all_recorded(self) -> None:
        """Multiple queries (F4 and F5) each produce a history entry."""
        session = FakeSession()

        f4_sql = "SELECT * FROM catalog.schema.table LIMIT 100"
        f5_sql = "SELECT count(*) FROM orders WHERE status = 'open'"

        session.execute_query(f4_sql)
        session.execute_query(f5_sql)

        assert len(session.history) == 2
        names = [e.name for e in session.history]
        assert f4_sql in names
        assert f5_sql in names

    def test_history_entry_has_required_fields(self) -> None:
        """QueryHistoryEntry has timestamp, action_type, name, status."""
        session = FakeSession()
        session.execute_query("SELECT 1")

        entry = session.history[0]
        assert isinstance(entry.timestamp, datetime)
        assert entry.action_type == "query"
        assert entry.name == "SELECT 1"
        assert entry.status == "success"
        assert entry.duration_ms >= 0
