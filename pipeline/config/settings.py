"""Configuration settings for asset validation pipeline."""

import os
from pathlib import Path
from typing import Optional


class Settings:
    """Application settings loaded from environment."""
    
    # Asset storage
    ASSET_ROOT = Path(os.getenv("ASSET_ROOT", "/mnt/library/assets"))
    CACHE_ROOT = Path(os.getenv("CACHE_ROOT", "/mnt/cache/asset_validator"))
    
    # Database
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "asset_db")
    DB_USER = os.getenv("DB_USER", "render")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    
    # Validation settings
    DEFAULT_THREADS = int(os.getenv("VALIDATION_THREADS", "16"))
    HASH_BLOCKSIZE = int(os.getenv("HASH_BLOCKSIZE", "65536"))
    VALIDATION_TIMEOUT = int(os.getenv("VALIDATION_TIMEOUT", "300"))
    
    # Cache
    ENABLE_CACHE = os.getenv("ENABLE_CACHE", "true").lower() == "true"
    CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", "7"))
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "/var/log/asset_validator.log")
    
    @classmethod
    def validate(cls) -> bool:
        """Validate critical settings."""
        if not cls.ASSET_ROOT.exists():
            raise ValueError(f"Asset root does not exist: {cls.ASSET_ROOT}")
        return True