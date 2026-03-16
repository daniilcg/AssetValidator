"""
Backwards-compatible module.

The canonical implementation lives in `pipeline.core.asset_validator`.
This file re-exports public API for users that import from the repo root.
"""

from pipeline.core.asset_validator import (  # noqa: F401
    AssetValidator,
    ValidationResult,
    ValidationSummary,
    main_cli,
)


if __name__ == "__main__":
    main_cli()