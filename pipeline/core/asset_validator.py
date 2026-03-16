"""
Asset validation core implementation.

This module is the canonical location for AssetValidator used by imports and tests:
`from pipeline.core.asset_validator import AssetValidator`.
"""

from __future__ import annotations

import gc
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Union

from pipeline.config.settings import Settings
from pipeline.core.db_interface import AssetDatabase, MockAssetDatabase
from pipeline.core.exceptions import (
    AssetCorruptedError,
    AssetNotFoundError,
    AssetValidationError,
    VersionMismatchError,
)
from pipeline.core.utils import ensure_directory, format_timestamp, hash_file


@dataclass
class ValidationResult:
    """Result of asset validation."""

    asset_name: str
    version: str
    path: Path
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    hash_value: Optional[str] = None
    expected_hash: Optional[str] = None
    duration_ms: float = 0.0
    validated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "asset_name": self.asset_name,
            "version": self.version,
            "path": self.path.as_posix(),
            "is_valid": self.is_valid,
            "errors": self.errors,
            "hash": self.hash_value,
            "expected_hash": self.expected_hash,
            "duration_ms": self.duration_ms,
            "validated_at": self.validated_at.isoformat(),
        }

    def __str__(self) -> str:
        status = "V" if self.is_valid else "X"
        return f"{status} {self.asset_name} {self.version}: {self.path.name}"


@dataclass
class ValidationSummary:
    """Summary statistics for batch validation."""

    total: int = 0
    passed: int = 0
    failed: int = 0
    missing: int = 0
    corrupted: int = 0
    mismatched: int = 0
    total_duration_ms: float = 0.0
    results: List[ValidationResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.passed / self.total) * 100

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "missing": self.missing,
            "corrupted": self.corrupted,
            "mismatched": self.mismatched,
            "success_rate": self.success_rate,
            "total_duration_ms": self.total_duration_ms,
            "timestamp": format_timestamp(),
        }

    def print_report(self) -> None:
        logger = logging.getLogger(__name__)

        logger.info("=" * 60)
        logger.info("ASSET VALIDATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total:     {self.total:4d} assets")
        logger.info(f"Passed:    {self.passed:4d} ({self.success_rate:.1f}%) V")
        logger.info(f"Failed:    {self.failed:4d} ({100 - self.success_rate:.1f}%) X")

        if self.missing:
            logger.info(f"  Missing:    {self.missing:4d}")
        if self.corrupted:
            logger.info(f"  Corrupted:  {self.corrupted:4d}")
        if self.mismatched:
            logger.info(f"  Mismatched: {self.mismatched:4d}")

        if self.failed > 0:
            logger.info("Failed assets:")
            failed_assets = [r for r in self.results if not r.is_valid]
            for result in failed_assets[:10]:
                for error in result.errors[:2]:
                    logger.info(f"  - {result.asset_name} {result.version}: {error}")
            if len(failed_assets) > 10:
                logger.info(f"  ... and {len(failed_assets) - 10} more")

        logger.info("=" * 60)


class AssetValidator:
    """Main asset validation class."""

    def __init__(
        self,
        asset_root: Union[str, Path, None] = None,
        cache_root: Union[str, Path, None] = None,
        db: Optional[AssetDatabase] = None,
        threads: Optional[int] = None,
        enable_cache: Optional[bool] = None,
        timeout: Optional[int] = None,
        blocksize: Optional[int] = None,
        log_level: Optional[str] = None,
    ):
        self.settings = Settings()

        self.asset_root = Path(asset_root or self.settings.ASSET_ROOT)
        self.cache_root = Path(cache_root or self.settings.CACHE_ROOT)
        self.db = db or MockAssetDatabase()
        self.threads = threads or self.settings.DEFAULT_THREADS
        self.enable_cache = (
            enable_cache if enable_cache is not None else self.settings.ENABLE_CACHE
        )
        self.timeout = timeout or self.settings.VALIDATION_TIMEOUT
        self.blocksize = blocksize or self.settings.HASH_BLOCKSIZE

        self.logger = logging.getLogger(__name__)
        self._setup_directories()
        self._setup_logging(log_level=log_level or self.settings.LOG_LEVEL)

        self.hash_cache = self.cache_root / "hashes"
        self.hash_cache.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"AssetValidator initialized: {self.asset_root}")
        self.logger.info(f"Cache: {self.cache_root} (enabled={self.enable_cache})")
        self.logger.info(f"Threads: {self.threads}, Timeout: {self.timeout}s")

    def _setup_directories(self) -> None:
        ensure_directory(self.asset_root)
        ensure_directory(self.cache_root)

    def _setup_logging(self, log_level: str) -> None:
        if any(isinstance(h, logging.StreamHandler) for h in self.logger.handlers):
            return

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(getattr(logging, str(log_level).upper(), logging.INFO))

    def _get_cached_hash(self, asset_name: str, version: str) -> Optional[str]:
        if not self.enable_cache:
            return None

        cache_file = self.hash_cache / f"{asset_name}_{version}.hash"
        if not cache_file.exists():
            return None

        mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
        age_days = (datetime.now() - mtime).days
        if age_days > self.settings.CACHE_TTL_DAYS:
            self.logger.debug(f"Cache expired for {asset_name} {version}")
            try:
                cache_file.unlink()
            except OSError:
                pass
            return None

        try:
            return cache_file.read_text(encoding="utf-8").strip()
        except Exception as e:
            self.logger.warning(f"Failed to read cache: {e}")
            return None

    def _save_hash_to_cache(self, asset_name: str, version: str, hash_value: str) -> None:
        if not self.enable_cache:
            return

        cache_file = self.hash_cache / f"{asset_name}_{version}.hash"
        try:
            cache_file.write_text(hash_value, encoding="utf-8")
        except Exception as e:
            self.logger.warning(f"Failed to write cache: {e}")

    def _validate_usd(self, file_path: Path) -> Tuple[bool, List[str]]:
        errors: List[str] = []

        try:
            try:
                from pxr import Sdf, Usd  # type: ignore
            except ImportError:
                self.logger.debug("USD modules not available, skipping deep validation")
                return file_path.exists(), []

            layer = Sdf.Layer.FindOrOpen(str(file_path))
            if not layer:
                errors.append(f"Cannot open layer: {file_path}")
                return False, errors

            if layer.HasErrors():
                layer_errors = layer.GetErrors()
                errors.extend([f"Layer error: {e}" for e in layer_errors[:3]])
                if layer_errors:
                    return False, errors

            stage = Usd.Stage.Open(str(file_path), load=Usd.Stage.LoadNone)
            if not stage:
                errors.append("Failed to open stage")
                return False, errors

            if not stage.GetPseudoRoot():
                errors.append("No root prim")
                return False, errors

            prims = list(stage.Traverse())
            if not prims:
                self.logger.warning(f"USD file has no prims: {file_path}")

            return True, errors

        except Exception as e:
            errors.append(f"USD validation exception: {e}")
            return False, errors

        finally:
            gc.collect()

    def validate_asset(self, asset_name: str, version: str, relative_path: str) -> ValidationResult:
        import time

        start_time = time.time()
        result = ValidationResult(
            asset_name=asset_name,
            version=version,
            path=self.asset_root / relative_path,
            is_valid=False,
            errors=[],
        )

        try:
            if not result.path.exists():
                raise AssetNotFoundError(f"File not found: {result.path}")

            is_valid_usd, usd_errors = self._validate_usd(result.path)
            if not is_valid_usd:
                result.errors.extend(usd_errors)
                raise AssetCorruptedError("USD validation failed")

            result.expected_hash = self.db.get_asset_hash(asset_name, version) if self.db else None

            cached_hash = self._get_cached_hash(asset_name, version)
            if cached_hash:
                result.hash_value = cached_hash
            else:
                result.hash_value = hash_file(result.path, self.blocksize)
                if not result.hash_value:
                    raise AssetCorruptedError("Failed to calculate file hash")
                self._save_hash_to_cache(asset_name, version, result.hash_value)

            if result.expected_hash and result.hash_value != result.expected_hash:
                error = (
                    f"Hash mismatch: got {result.hash_value[:8]}..., "
                    f"expected {result.expected_hash[:8]}..."
                )
                result.errors.append(error)
                raise VersionMismatchError(error)

            if not result.expected_hash and self.db:
                self.db.update_asset_hash(asset_name, version, result.hash_value)

            result.is_valid = True

        except AssetNotFoundError as e:
            result.errors.append(str(e))
            self.logger.error(f"Asset missing: {asset_name} {version}")
        except AssetCorruptedError as e:
            result.errors.append(str(e))
            self.logger.error(f"Asset corrupted: {asset_name} {version} - {e}")
        except VersionMismatchError as e:
            result.errors.append(str(e))
            self.logger.warning(f"Version mismatch: {asset_name} {version}")
        except Exception as e:
            result.errors.append(f"Unexpected error: {e}")
            self.logger.error(f"Validation crashed: {asset_name} {version} - {e}")

        result.duration_ms = (time.time() - start_time) * 1000
        self.logger.debug(f"Validated {asset_name} {version} in {result.duration_ms:.0f}ms")
        return result

    def validate_batch(
        self, assets: List[Tuple[str, str, str]], raise_on_failure: bool = False
    ) -> ValidationSummary:
        import time

        start_time = time.time()
        summary = ValidationSummary(total=len(assets))

        if not assets:
            self.logger.warning("No assets to validate")
            return summary

        self.logger.info(f"Validating {len(assets)} assets with {self.threads} threads...")

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            future_to_asset = {
                executor.submit(self.validate_asset, name, version, path): (name, version, path)
                for name, version, path in assets
            }

            for future in as_completed(future_to_asset):
                asset_name, version, path = future_to_asset[future]

                try:
                    result = future.result(timeout=self.timeout)
                    summary.results.append(result)

                    if result.is_valid:
                        summary.passed += 1
                    else:
                        summary.failed += 1
                        for error in result.errors:
                            low = error.lower()
                            if "not found" in low or "missing" in low:
                                summary.missing += 1
                            elif "corrupt" in low or "invalid" in low:
                                summary.corrupted += 1
                            elif "mismatch" in low:
                                summary.mismatched += 1

                        if raise_on_failure:
                            raise AssetValidationError(
                                f"Validation failed: {asset_name} {version}"
                            )

                except TimeoutError:
                    summary.failed += 1
                    summary.corrupted += 1
                    error_msg = f"Validation timeout after {self.timeout}s"
                    summary.results.append(
                        ValidationResult(
                            asset_name=asset_name,
                            version=version,
                            path=self.asset_root / path,
                            is_valid=False,
                            errors=[error_msg],
                        )
                    )
                    self.logger.error(f"Timeout: {asset_name} {version}")

                except Exception as e:
                    summary.failed += 1
                    summary.corrupted += 1
                    error_msg = f"Validation error: {e}"
                    summary.results.append(
                        ValidationResult(
                            asset_name=asset_name,
                            version=version,
                            path=self.asset_root / path,
                            is_valid=False,
                            errors=[error_msg],
                        )
                    )
                    self.logger.error(f"Error: {asset_name} {version} - {e}")

        summary.total_duration_ms = (time.time() - start_time) * 1000
        self.logger.info(f"Validation completed in {summary.total_duration_ms:.0f}ms")
        return summary

    def export_report(self, summary: ValidationSummary, output_path: Union[str, Path]) -> None:
        output = Path(output_path)
        report = {"summary": summary.to_dict(), "results": [r.to_dict() for r in summary.results]}
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        self.logger.info(f"Report exported to {output}")


def main_cli() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Validate assets in VFX pipeline")
    parser.add_argument("--asset-root", type=str, help="Asset root directory")
    parser.add_argument("--cache-root", type=str, help="Cache directory")
    parser.add_argument("--threads", type=int, default=None, help="Number of threads")
    parser.add_argument("--timeout", type=int, default=None, help="Timeout in seconds")
    parser.add_argument("--output", type=str, help="Output report file")
    parser.add_argument("assets", nargs="+", help="Assets in format name:version:path")

    args = parser.parse_args()

    assets: List[Tuple[str, str, str]] = []
    for asset_str in args.assets:
        try:
            name, version, path = asset_str.split(":", 2)
            assets.append((name, version, path))
        except ValueError:
            print(f"Invalid asset format: {asset_str}", file=sys.stderr)
            print("Expected: name:version:path", file=sys.stderr)
            raise SystemExit(2)

    validator = AssetValidator(
        asset_root=args.asset_root,
        cache_root=args.cache_root,
        threads=args.threads,
        timeout=args.timeout,
    )

    summary = validator.validate_batch(assets)
    summary.print_report()

    if args.output:
        validator.export_report(summary, args.output)

    raise SystemExit(0 if summary.failed == 0 else 1)


if __name__ == "__main__":
    main_cli()
