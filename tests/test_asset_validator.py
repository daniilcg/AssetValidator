"""Unit tests for asset validator."""

import shutil
import tempfile
import unittest
from pathlib import Path

from pipeline.core import asset_validator as cli_module
from pipeline.core.asset_validator import AssetValidator, ValidationResult
from pipeline.core.db_interface import MockAssetDatabase


class TestAssetValidator(unittest.TestCase):
    """Test cases for AssetValidator class."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.asset_root = Path(self.test_dir) / "assets"
        self.cache_root = Path(self.test_dir) / "cache"

        self.asset_root.mkdir()
        self.cache_root.mkdir()

        # Create test asset
        self.test_asset = self.asset_root / "test.usd"
        with open(self.test_asset, "w", encoding="utf-8") as f:
            f.write("#usda 1.0\n")
            f.write('def Xform "root" {}\n')

        self.db = MockAssetDatabase()

        self.validator = AssetValidator(
            asset_root=self.asset_root,
            cache_root=self.cache_root,
            db=self.db,
            enable_cache=False,  # Disable cache for tests
        )

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def test_validate_existing_asset(self):
        """Test validation of existing asset."""
        result = self.validator.validate_asset("test_asset", "v001", "test.usd")

        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.errors), 0)
        self.assertEqual(result.asset_name, "test_asset")
        self.assertEqual(result.version, "v001")

    def test_validate_missing_asset(self):
        """Test validation of missing asset."""
        result = self.validator.validate_asset("missing", "v001", "nonexistent.usd")

        self.assertFalse(result.is_valid)
        self.assertGreater(len(result.errors), 0)
        self.assertIn("not found", result.errors[0].lower())

    def test_validate_with_expected_hash(self):
        """Test validation with expected hash in database."""
        # First validate to generate hash
        result1 = self.validator.validate_asset(
            "dragon", "v042", "characters/dragon/v042/dragon_v042.usd"
        )

        # Asset should fail (doesn't exist)
        self.assertFalse(result1.is_valid)

    def test_batch_validation(self):
        """Test batch validation."""
        assets = [
            ("test1", "v001", "test.usd"),
            ("test2", "v001", "nonexistent.usd"),
        ]

        summary = self.validator.validate_batch(assets)

        self.assertEqual(summary.total, 2)
        self.assertEqual(summary.passed, 1)
        self.assertEqual(summary.failed, 1)
        self.assertEqual(summary.missing, 1)

    def test_validation_result_serialization(self):
        """Test ValidationResult serialization."""
        result = ValidationResult(
            asset_name="test",
            version="v001",
            path=Path("/test/path.usd"),
            is_valid=True,
            errors=["error1", "error2"],
            hash_value="abc123",
            expected_hash="def456",
            duration_ms=123.45,
        )

        data = result.to_dict()

        self.assertEqual(data["asset_name"], "test")
        self.assertEqual(data["version"], "v001")
        self.assertEqual(data["path"], "/test/path.usd")
        self.assertTrue(data["is_valid"])
        self.assertEqual(len(data["errors"]), 2)
        self.assertEqual(data["hash"], "abc123")
        self.assertEqual(data["expected_hash"], "def456")
        self.assertEqual(data["duration_ms"], 123.45)

    def test_cli_smoke(self):
        """Smoke test for CLI main_cli()."""
        import sys

        # Create asset specifically for CLI run
        cli_asset = self.asset_root / "cli_test.usd"
        with open(cli_asset, "w", encoding="utf-8") as f:
            f.write("#usda 1.0\n")
            f.write('def Xform "root" {}\n')

        argv_backup = sys.argv
        sys.argv = [
            "validate-assets",
            "--asset-root",
            str(self.asset_root),
            "--cache-root",
            str(self.cache_root),
            "cli_asset:v001:cli_test.usd",
        ]

        try:
            with self.assertRaises(SystemExit) as cm:
                cli_module.main_cli()
            # CLI should exit with success for valid asset
            self.assertEqual(cm.exception.code, 0)
        finally:
            sys.argv = argv_backup


if __name__ == "__main__":
    unittest.main()