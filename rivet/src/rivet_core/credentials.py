"""Abstract credential resolution protocol for cloud catalog adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass
class Credentials:
    """Resolved cloud credentials."""

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None


@runtime_checkable
class CredentialResolver(Protocol):
    """Protocol for resolving cloud credentials from catalog options."""

    def resolve(self) -> Credentials: ...

    def create_client(self, service: str) -> Any: ...
