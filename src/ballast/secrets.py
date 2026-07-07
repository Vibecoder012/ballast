"""Secret storage: OS keyring when available, environment variables always.

Secret *material* should never live in your database, logs, or backups — only the *name*
of a secret should. :class:`SecretStore` reads from the OS keyring (Windows Credential
Manager, macOS Keychain, Freedesktop Secret Service) when the optional ``keyring`` package
is installed, and always falls back to environment variables so dev/CI need no keyring.

Install keyring support with: ``pip install "ballast[keyring]"``.
"""

from __future__ import annotations

import contextlib
import os
from typing import Any

from .errors import SecretsUnavailableError

DEFAULT_SERVICE = "ballast"


class SecretStore:
    """Read/write secrets by name, backed by the OS keyring with an env-var fallback.

    Parameters
    ----------
    service:
        Keyring "service" namespace and the prefix for environment-variable lookups.
    use_keyring:
        Set ``False`` to force env-only mode (e.g. in CI). If ``True`` but ``keyring`` is not
        installed, reads still work via env vars and writes raise
        :class:`~ballast.errors.SecretsUnavailableError`.
    """

    __slots__ = ("_keyring", "service")

    def __init__(self, service: str = DEFAULT_SERVICE, *, use_keyring: bool = True) -> None:
        self.service = service
        self._keyring: Any = None
        if use_keyring:
            try:
                import keyring

                self._keyring = keyring
            except Exception:  # a broken/absent backend must not break imports
                self._keyring = None

    @property
    def keyring_available(self) -> bool:
        """Whether an OS keyring backend is available for writes."""
        return self._keyring is not None

    def _env_names(self, name: str) -> tuple[str, str]:
        # Try the exact name first, then a namespaced upper-case form (BALLAST_MY_KEY).
        return (name, f"{self.service}_{name}".upper().replace("-", "_").replace(".", "_"))

    def get(self, name: str) -> str | None:
        """Return the secret ``name`` from the keyring, else the environment, else ``None``."""
        if self._keyring is not None:
            keyring_value: str | None
            try:
                keyring_value = self._keyring.get_password(self.service, name)
            except Exception:  # fall through to env on any backend error
                keyring_value = None
            if keyring_value is not None:
                return keyring_value
        for env_name in self._env_names(name):
            env_value = os.environ.get(env_name)
            if env_value is not None:
                return env_value
        return None

    def set(self, name: str, value: str) -> None:
        """Store ``value`` under ``name`` in the OS keyring.

        Raises :class:`~ballast.errors.SecretsUnavailableError` if no keyring backend is
        installed (there is nowhere durable to write; set an env var instead, or install the
        ``keyring`` extra).
        """
        if self._keyring is None:
            raise SecretsUnavailableError(
                "no keyring backend available; install the 'keyring' extra "
                '(pip install "ballast[keyring]") or provide the secret via an env var'
            )
        self._keyring.set_password(self.service, name, value)

    def delete(self, name: str) -> None:
        """Delete ``name`` from the keyring if present (no-op otherwise)."""
        if self._keyring is None:
            return
        # Deleting an absent secret (or a flaky backend) is not an error.
        with contextlib.suppress(Exception):
            self._keyring.delete_password(self.service, name)
