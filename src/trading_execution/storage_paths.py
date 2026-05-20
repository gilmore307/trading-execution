"""Storage-owned filesystem roots for execution artifacts."""

from __future__ import annotations

import os
from pathlib import Path


def _path_from_env(name: str, default: Path) -> Path:
    value = os.environ.get(name, "").strip()
    return Path(value).expanduser() if value else default


def projects_root() -> Path:
    return _path_from_env("TRADING_PROJECTS_ROOT", Path("/root/projects"))


def trading_storage_root() -> Path:
    return _path_from_env("TRADING_STORAGE_ROOT", projects_root() / "trading-storage")


def execution_storage_root() -> Path:
    return _path_from_env("TRADING_EXECUTION_STORAGE_ROOT", trading_storage_root() / "storage" / "execution")


def resolve_output_root(output_root: str | None, *, default_task_id: str) -> Path:
    path = Path(str(output_root or f"storage/{default_task_id}"))
    if path.is_absolute():
        return path
    parts = path.parts
    if parts and parts[0] == "storage":
        return execution_storage_root().joinpath(*parts[1:])
    return execution_storage_root() / path


__all__ = ["execution_storage_root", "projects_root", "resolve_output_root", "trading_storage_root"]
