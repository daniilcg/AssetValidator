#!/usr/bin/env python3
"""Test to use AssetValidator."""

import logging
import os

from pipeline.core.asset_validator import AssetValidator


def main():
    logging.basicConfig(level=logging.INFO)

    # Create test file
    os.makedirs("test_assets", exist_ok=True)
    with open("test_assets/test.usd", "w") as f:
        f.write("#usda 1.0\n")

    # Validation
    v = AssetValidator(asset_root="test_assets")
    result = v.validate_asset("test", "v001", "test.usd")
    logging.info("Valid: %s", result.is_valid)


if __name__ == "__main__":
    main()
