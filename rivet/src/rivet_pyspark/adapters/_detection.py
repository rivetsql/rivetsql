"""Shared Delta Lake JAR detection logic for PySpark adapters."""

from __future__ import annotations

import logging
from typing import Any

_logger = logging.getLogger(__name__)


def _config_has_delta(session: Any) -> bool:
    """Check Spark config for Delta Lake artifact strings.

    Inspects ``spark.jars.packages`` and ``spark.jars`` for the presence of
    ``delta-spark`` or ``delta-core`` substrings.
    """
    try:
        packages = session.conf.get("spark.jars.packages", "")
        jars = session.conf.get("spark.jars", "")
        combined = packages + jars
        return "delta-spark" in combined or "delta-core" in combined
    except Exception:
        return False


def _has_delta_jars(session: Any) -> bool:
    """Check if Delta Lake support is available to the Spark session.

    Detection strategy (in order):
    1. Config-based — return ``True`` immediately if ``spark.jars.packages``
       or ``spark.jars`` contains a Delta artifact string.
    2. JVM reflection — try ``Class.forName`` for the two well-known Delta
       entry-point classes.
    3. No JVM access (Spark Connect) — return ``False`` since config was
       already checked in step 1.
    """
    # Step 1: config-first detection
    if _config_has_delta(session):
        _logger.debug("Delta detected via Spark config")
        return True

    # Step 2: JVM reflection fallback
    try:
        jvm = session._jvm
        try:
            jvm.java.lang.Class.forName("io.delta.tables.DeltaTable")
            return True
        except Exception:
            pass
        try:
            jvm.java.lang.Class.forName("org.apache.spark.sql.delta.DeltaLog")
            return True
        except Exception:
            pass
        _logger.debug("Delta not found via JVM reflection")
        return False
    except Exception:
        # Step 3: no JVM access (e.g. Spark Connect) — config was negative
        _logger.debug("No JVM access, config check was negative")
        return False
