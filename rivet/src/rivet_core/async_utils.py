"""Utilities for safe async execution in both sync and async contexts."""

import asyncio
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


def safe_run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine safely, handling both sync and async contexts.

    When called from within an existing event loop (e.g., REPL, explore session),
    runs the coroutine in a new thread with its own event loop.
    Otherwise, creates a new event loop with asyncio.run().

    Args:
        coro: The coroutine to execute

    Returns:
        The result of the coroutine execution

    Raises:
        Exception: Any exception raised by the coroutine is propagated
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No event loop running — create one
        return asyncio.run(coro)
    else:
        # Event loop already running — run in a thread pool with its own loop
        result_container: list[T] = []
        exception_container: list[Exception] = []

        def run_in_thread() -> None:
            try:
                result = asyncio.run(coro)
                result_container.append(result)
            except Exception as e:  # noqa: BLE001
                exception_container.append(e)

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join()

        if exception_container:
            raise exception_container[0]
        return result_container[0]
