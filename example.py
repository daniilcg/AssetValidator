#!/usr/bin/env python3
"""Test to use AssetValidator."""

from pipeline.core.asset_validator import AssetValidator


def main():
    # Create test file
    import os

    os.makedirs("test_assets", exist_ok=True)
    with open("test_assets/test.usd", "w") as f:
        f.write("#usda 1.0\n")

    # Validation
    v = AssetValidator(asset_root="test_assets")
    result = v.validate_asset("test", "v001", "test.usd")
    print(f"Valid: {result.is_valid}")


if __name__ == "__main__":
    main()
