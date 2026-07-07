"""Exception hierarchy for :mod:`ballast`."""

from __future__ import annotations


class BallastError(Exception):
    """Base class for every error raised by ballast."""


class DowngradeError(BallastError, RuntimeError):
    """The database schema is newer than the code trying to open it.

    Raised by the migration runner when a module's recorded version exceeds the highest
    version the running code ships — i.e. you are running old code against a database a
    newer build already upgraded. Ballast refuses to run rather than corrupt data.
    """

    def __init__(self, module: str, recorded: int, shipped: int) -> None:
        self.module = module
        self.recorded = recorded
        self.shipped = shipped
        super().__init__(
            f"refusing to run: database schema for module {module!r} is at version "
            f"{recorded}, but this build only ships version {shipped}. You are running "
            f"older code against a newer database. Upgrade the code, or restore a "
            f"snapshot taken before the upgrade (see ballast.snapshots)."
        )


class EventContractError(BallastError, ValueError):
    """A published payload failed the validator declared for its topic."""


class SecretsUnavailableError(BallastError, RuntimeError):
    """A keyring-backed write was attempted but no keyring backend is installed."""
