"""Custom exceptions for asset validation pipeline."""

class AssetValidationError(Exception):
    """Base exception for asset validation errors."""
    pass

class AssetNotFoundError(AssetValidationError):
    """Raised when asset file is missing."""
    pass

class AssetCorruptedError(AssetValidationError):
    """Raised when asset file is corrupted or invalid."""
    pass

class VersionMismatchError(AssetValidationError):
    """Raised when asset version doesn't match expected."""
    pass

class DatabaseError(AssetValidationError):
    """Raised when database operations fail."""
    pass