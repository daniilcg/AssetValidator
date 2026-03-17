"""Utility functions for asset validation."""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)


def hash_file(file_path: Union[str, Path], blocksize: int = 65536) -> Optional[str]:
    """
    Calculate MD5 hash of a file without loading entire file into memory.

    Args:
        file_path: Path to the file
        blocksize: Size of chunks to read (default 64KB)

    Returns:
        MD5 hash string or None if file cannot be read
    """
    path = Path(file_path)
    if not path.exists():
        logger.error(f"File not found: {path}")
        return None

    hasher = hashlib.md5()
    try:
        with open(path, "rb") as f:
            buf = f.read(blocksize)
            while buf:
                hasher.update(buf)
                buf = f.read(blocksize)
        return hasher.hexdigest()
    except PermissionError:
        logger.error(f"Permission denied: {path}")
        return None
    except Exception as e:
        logger.error(f"Failed to hash {path}: {e}")
        return None


def safe_path_join(*parts) -> Path:
    """Safely join path parts and resolve."""
    path = Path(*parts).resolve()
    return path


def ensure_directory(path: Union[str, Path]) -> Path:
    """Ensure directory exists, create if necessary."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def format_timestamp(dt: Optional[datetime] = None) -> str:
    """Format timestamp for logs and filenames."""
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y%m%d_%H%M%S")


def load_json_safe(file_path: Union[str, Path]) -> Optional[dict]:
    """Load JSON file safely, return None on error."""
    path = Path(file_path)
    if not path.exists():
        return None

    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON {path}: {e}")
        return None
