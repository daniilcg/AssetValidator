"""Configuration settings for asset validation pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _default_base_dir() -> Path:
    # Prefer project-local defaults (works on Windows/Linux/macOS without permissions issues).
    # ASSETVALIDATOR_BASE_DIR wins; else PROJECT_PATH (no hardcoded D:/Work/...); else cwd.
    raw = os.getenv("ASSETVALIDATOR_BASE_DIR") or os.getenv("PROJECT_PATH")
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.cwd().resolve()


@dataclass(frozen=True)
class Settings:
    """Application settings loaded from environment (instance-based)."""

    # Asset storage
    asset_root: Path
    cache_root: Path

    # Database (optional, used by custom DB implementations)
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # Validation settings
    default_threads: int
    hash_blocksize: int
    validation_timeout: int

    # Cache
    enable_cache: bool
    cache_ttl_days: int

    # Logging
    log_level: str
    log_file: str

    @classmethod
    def from_env(cls) -> Settings:
        base = _default_base_dir()

        asset_root = Path(os.getenv("ASSET_ROOT", str(base / "assets"))).expanduser()
        cache_root = Path(
            os.getenv("CACHE_ROOT", str(base / ".asset_validator_cache"))
        ).expanduser()

        return cls(
            asset_root=asset_root,
            cache_root=cache_root,
            db_host=os.getenv("DB_HOST", "localhost"),
            db_port=_env_int("DB_PORT", 5432),
            db_name=os.getenv("DB_NAME", "asset_db"),
            db_user=os.getenv("DB_USER", "render"),
            db_password=os.getenv("DB_PASSWORD", ""),
            default_threads=_env_int("VALIDATION_THREADS", 16),
            hash_blocksize=_env_int("HASH_BLOCKSIZE", 65536),
            validation_timeout=_env_int("VALIDATION_TIMEOUT", 300),
            enable_cache=_env_bool("ENABLE_CACHE", True),
            cache_ttl_days=_env_int("CACHE_TTL_DAYS", 7),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            # Empty default means: don't log to file unless explicitly set.
            log_file=os.getenv("LOG_FILE", ""),
        )

    def validate(self) -> None:
        """Validate settings values (does not touch filesystem)."""
        if self.default_threads <= 0:
            raise ValueError("VALIDATION_THREADS must be > 0")
        if self.hash_blocksize <= 0:
            raise ValueError("HASH_BLOCKSIZE must be > 0")
        if self.validation_timeout <= 0:
            raise ValueError("VALIDATION_TIMEOUT must be > 0")
        if self.cache_ttl_days < 0:
            raise ValueError("CACHE_TTL_DAYS must be >= 0")
