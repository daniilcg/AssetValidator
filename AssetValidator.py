"""
Backwards-compatible module.

The canonical implementation lives in `pipeline.core.asset_validator`.
This file re-exports public API for users that import from the repo root.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    from pipeline.core.asset_validator import (  # noqa: F401
        AssetValidator,
        ValidationResult,
        ValidationSummary,
        main_cli,
    )
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pipeline.core.asset_validator import (  # noqa: F401
        AssetValidator,
        ValidationResult,
        ValidationSummary,
        main_cli,
    )

if __name__ == "__main__":
    main_cli()
