"""In-memory cache of executed joint Material outputs, keyed by joint name."""

from __future__ import annotations

from rivet_core.models import Material


class MaterialCache:
    """Cache of executed joint Material outputs keyed by joint name.

    Requirements: 20.1, 20.2, 20.3, 20.4, 20.5
    """

    def __init__(self) -> None:
        self._cache: dict[str, Material] = {}

    def get(self, joint_name: str) -> Material | None:
        """Return cached Material for joint_name, or None if not cached."""
        return self._cache.get(joint_name)

    def put(self, joint_name: str, material: Material) -> None:
        """Store material under joint_name."""
        self._cache[joint_name] = material

    def invalidate(self, joint_names: list[str]) -> None:
        """Remove only the specified joints from the cache."""
        for name in joint_names:
            self._cache.pop(name, None)

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._cache.clear()

    def __contains__(self, joint_name: str) -> bool:
        return joint_name in self._cache

    def __len__(self) -> int:
        return len(self._cache)
