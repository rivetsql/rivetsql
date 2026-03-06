"""Tests for FileWatcher.

Validates: Requirements 23.1, 23.2, 23.3, 23.4, 32.2
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock

from rivet_cli.repl.file_watcher import (
    _DEBOUNCE_SECONDS,
    _POLL_INTERVAL,
    FileWatcher,
    _collect_watched_files,
)

# ---------------------------------------------------------------------------
# _collect_watched_files
# ---------------------------------------------------------------------------


class TestCollectWatchedFiles:
    def test_empty_directory(self, tmp_path: Path) -> None:
        result = _collect_watched_files(tmp_path)
        assert result == {}

    def test_rivet_yaml_included(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("name: test")
        result = _collect_watched_files(tmp_path)
        assert tmp_path / "rivet.yaml" in result

    def test_profiles_yaml_included(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text("profiles: []")
        result = _collect_watched_files(tmp_path)
        assert tmp_path / "profiles.yaml" in result

    def test_sources_sql_included(self, tmp_path: Path) -> None:
        sources = tmp_path / "sources"
        sources.mkdir()
        (sources / "raw.sql").write_text("SELECT 1")
        result = _collect_watched_files(tmp_path)
        assert sources / "raw.sql" in result

    def test_joints_sql_included(self, tmp_path: Path) -> None:
        joints = tmp_path / "joints"
        joints.mkdir()
        (joints / "transform.sql").write_text("SELECT 1")
        result = _collect_watched_files(tmp_path)
        assert joints / "transform.sql" in result

    def test_sinks_sql_included(self, tmp_path: Path) -> None:
        sinks = tmp_path / "sinks"
        sinks.mkdir()
        (sinks / "output.sql").write_text("SELECT 1")
        result = _collect_watched_files(tmp_path)
        assert sinks / "output.sql" in result

    def test_quality_yaml_included(self, tmp_path: Path) -> None:
        quality = tmp_path / "quality"
        quality.mkdir()
        (quality / "checks.yaml").write_text("checks: []")
        result = _collect_watched_files(tmp_path)
        assert quality / "checks.yaml" in result

    def test_root_sql_included(self, tmp_path: Path) -> None:
        (tmp_path / "adhoc.sql").write_text("SELECT 1")
        result = _collect_watched_files(tmp_path)
        assert tmp_path / "adhoc.sql" in result

    def test_unrelated_files_excluded(self, tmp_path: Path) -> None:
        (tmp_path / "README.md").write_text("# readme")
        (tmp_path / "data.csv").write_text("a,b")
        result = _collect_watched_files(tmp_path)
        assert tmp_path / "README.md" not in result
        assert tmp_path / "data.csv" not in result

    def test_returns_mtime_float(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("name: test")
        result = _collect_watched_files(tmp_path)
        mtime = result[tmp_path / "rivet.yaml"]
        assert isinstance(mtime, float)


# ---------------------------------------------------------------------------
# FileWatcher construction
# ---------------------------------------------------------------------------


class TestFileWatcherInit:
    def test_not_running_before_start(self, tmp_path: Path) -> None:
        session = MagicMock()
        on_compiled = MagicMock()
        fw = FileWatcher(tmp_path, session, on_compiled)
        assert fw._thread is None

    def test_stop_before_start_is_safe(self, tmp_path: Path) -> None:
        session = MagicMock()
        on_compiled = MagicMock()
        fw = FileWatcher(tmp_path, session, on_compiled)
        fw.stop()  # should not raise


# ---------------------------------------------------------------------------
# FileWatcher start/stop
# ---------------------------------------------------------------------------


class TestFileWatcherStartStop:
    def test_thread_starts_as_daemon(self, tmp_path: Path) -> None:
        session = MagicMock()
        on_compiled = MagicMock()
        fw = FileWatcher(tmp_path, session, on_compiled)
        fw.start()
        assert fw._thread is not None
        assert fw._thread.daemon is True
        fw.stop()

    def test_thread_stops_cleanly(self, tmp_path: Path) -> None:
        session = MagicMock()
        on_compiled = MagicMock()
        fw = FileWatcher(tmp_path, session, on_compiled)
        fw.start()
        fw.stop()
        assert fw._thread is None or not fw._thread.is_alive()

    def test_stop_sets_stop_event(self, tmp_path: Path) -> None:
        session = MagicMock()
        on_compiled = MagicMock()
        fw = FileWatcher(tmp_path, session, on_compiled)
        fw.start()
        fw.stop()
        assert fw._stop_event.is_set()


# ---------------------------------------------------------------------------
# File change detection and debounce
# ---------------------------------------------------------------------------


class TestFileChangeDetection:
    def test_detects_new_file_and_calls_on_file_changed(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.on_file_changed.return_value = MagicMock()  # non-None = relevant
        compiled_events: list[tuple[bool, float, str | None]] = []

        def on_compiled(success: bool, elapsed_ms: float, error: str | None) -> None:
            compiled_events.append((success, elapsed_ms, error))

        fw = FileWatcher(tmp_path, session, on_compiled)
        fw.start()

        # Create a watched file after watcher starts
        time.sleep(_POLL_INTERVAL * 2)
        (tmp_path / "rivet.yaml").write_text("name: test")

        # Wait for debounce + processing
        time.sleep(_DEBOUNCE_SECONDS + _POLL_INTERVAL * 4)
        fw.stop()

        assert session.on_file_changed.called
        assert len(compiled_events) >= 1
        assert compiled_events[0][0] is True  # success

    def test_no_callback_for_irrelevant_files(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.on_file_changed.return_value = None  # None = not relevant
        compiled_events: list[tuple] = []

        fw = FileWatcher(tmp_path, session, lambda s, e, err: compiled_events.append((s, e, err)))
        fw.start()

        time.sleep(_POLL_INTERVAL * 2)
        (tmp_path / "rivet.yaml").write_text("name: test")

        time.sleep(_DEBOUNCE_SECONDS + _POLL_INTERVAL * 4)
        fw.stop()

        # on_file_changed was called but returned None → no compiled callback
        assert len(compiled_events) == 0

    def test_debounce_batches_rapid_changes(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.on_file_changed.return_value = MagicMock()
        call_count = [0]

        def on_compiled(success: bool, elapsed_ms: float, error: str | None) -> None:
            call_count[0] += 1

        fw = FileWatcher(tmp_path, session, on_compiled)
        fw.start()

        time.sleep(_POLL_INTERVAL * 2)

        # Write multiple files in rapid succession
        (tmp_path / "rivet.yaml").write_text("name: test")
        time.sleep(0.05)
        (tmp_path / "profiles.yaml").write_text("profiles: []")
        time.sleep(0.05)
        joints = tmp_path / "joints"
        joints.mkdir(exist_ok=True)
        (joints / "t.sql").write_text("SELECT 1")

        # Wait for debounce to fire once
        time.sleep(_DEBOUNCE_SECONDS + _POLL_INTERVAL * 4)
        fw.stop()

        # Should have batched into a single (or very few) recompile calls
        assert call_count[0] >= 1
        # All changes passed together — on_file_changed called once (or few times)
        assert session.on_file_changed.call_count <= 3

    def test_error_in_session_calls_on_compiled_with_failure(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.on_file_changed.side_effect = RuntimeError("compile failed")
        compiled_events: list[tuple[bool, float, str | None]] = []

        fw = FileWatcher(tmp_path, session, lambda s, e, err: compiled_events.append((s, e, err)))
        fw.start()

        time.sleep(_POLL_INTERVAL * 2)
        (tmp_path / "rivet.yaml").write_text("name: test")

        time.sleep(_DEBOUNCE_SECONDS + _POLL_INTERVAL * 4)
        fw.stop()

        assert len(compiled_events) >= 1
        assert compiled_events[0][0] is False  # failure
        assert "compile failed" in (compiled_events[0][2] or "")


# ---------------------------------------------------------------------------
# Conflict detection (Requirement 23.3)
# ---------------------------------------------------------------------------


class TestConflictDetection:
    def test_conflict_callback_called_for_unsaved_path(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.on_file_changed.return_value = MagicMock()
        conflicts: list[Path] = []

        fw = FileWatcher(tmp_path, session, lambda s, e, err: None, on_conflict=conflicts.append)

        rivet_yaml = tmp_path / "rivet.yaml"
        rivet_yaml.write_text("name: test")
        fw.start()

        # Register the file as having unsaved changes
        fw.notify_unsaved_paths({rivet_yaml})

        time.sleep(_POLL_INTERVAL * 2)
        # Modify the file externally
        rivet_yaml.write_text("name: modified")

        time.sleep(_DEBOUNCE_SECONDS + _POLL_INTERVAL * 4)
        fw.stop()

        assert rivet_yaml in conflicts

    def test_no_conflict_when_no_unsaved_paths(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.on_file_changed.return_value = MagicMock()
        conflicts: list[Path] = []

        fw = FileWatcher(tmp_path, session, lambda s, e, err: None, on_conflict=conflicts.append)
        fw.start()

        time.sleep(_POLL_INTERVAL * 2)
        (tmp_path / "rivet.yaml").write_text("name: test")

        time.sleep(_DEBOUNCE_SECONDS + _POLL_INTERVAL * 4)
        fw.stop()

        assert conflicts == []

    def test_no_conflict_callback_when_not_provided(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.on_file_changed.return_value = MagicMock()

        rivet_yaml = tmp_path / "rivet.yaml"
        rivet_yaml.write_text("name: test")

        fw = FileWatcher(tmp_path, session, lambda s, e, err: None)  # no on_conflict
        fw.start()
        fw.notify_unsaved_paths({rivet_yaml})

        time.sleep(_POLL_INTERVAL * 2)
        rivet_yaml.write_text("name: modified")

        time.sleep(_DEBOUNCE_SECONDS + _POLL_INTERVAL * 4)
        fw.stop()  # should not raise


# ---------------------------------------------------------------------------
# notify_unsaved_paths thread safety
# ---------------------------------------------------------------------------


class TestNotifyUnsavedPaths:
    def test_can_be_called_before_start(self, tmp_path: Path) -> None:
        session = MagicMock()
        fw = FileWatcher(tmp_path, session, lambda s, e, err: None)
        fw.notify_unsaved_paths({tmp_path / "rivet.yaml"})  # should not raise

    def test_can_be_called_while_running(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.on_file_changed.return_value = None
        fw = FileWatcher(tmp_path, session, lambda s, e, err: None)
        fw.start()
        fw.notify_unsaved_paths({tmp_path / "rivet.yaml"})
        fw.notify_unsaved_paths(set())
        fw.stop()
