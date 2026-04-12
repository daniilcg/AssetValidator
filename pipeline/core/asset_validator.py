"""
Asset validation core implementation.

This module is the canonical location for AssetValidator used by imports and tests:
`from pipeline.core.asset_validator import AssetValidator`.
"""

from __future__ import annotations

import gc
import json
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

from pipeline.config.settings import Settings
from pipeline.core.db_interface import (
    AssetDatabase,
    MockAssetDatabase,
    MongoAssetDatabase,
    PostgresAssetDatabase,
)
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
        self.settings = Settings.from_env()
        self.settings.validate()

        self.asset_root = Path(asset_root or self.settings.asset_root)
        self.cache_root = Path(cache_root or self.settings.cache_root)
        self.db = db or MockAssetDatabase()
        self.threads = threads or self.settings.default_threads
        self.enable_cache = enable_cache if enable_cache is not None else self.settings.enable_cache
        self.timeout = timeout or self.settings.validation_timeout
        self.blocksize = blocksize or self.settings.hash_blocksize

        self.logger = logging.getLogger(__name__)
        self._setup_directories()
        self._setup_logging(log_level=log_level or self.settings.log_level)

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
        if age_days > self.settings.cache_ttl_days:
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
                from pxr import Sdf, Usd
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
        self,
        assets: List[Tuple[str, str, str]],
        raise_on_failure: bool = False,
        progress_callback: Optional[Callable[[int, int, ValidationResult], None]] = None,
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
                    if progress_callback:
                        progress_callback(len(summary.results), summary.total, result)

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
                            raise AssetValidationError(f"Validation failed: {asset_name} {version}")

                except TimeoutError:
                    summary.failed += 1
                    summary.corrupted += 1
                    error_msg = f"Validation timeout after {self.timeout}s"
                    timeout_result = ValidationResult(
                        asset_name=asset_name,
                        version=version,
                        path=self.asset_root / path,
                        is_valid=False,
                        errors=[error_msg],
                    )
                    summary.results.append(timeout_result)
                    if progress_callback:
                        progress_callback(len(summary.results), summary.total, timeout_result)
                    self.logger.error(f"Timeout: {asset_name} {version}")

                except Exception as e:
                    summary.failed += 1
                    summary.corrupted += 1
                    error_msg = f"Validation error: {e}"
                    err_result = ValidationResult(
                        asset_name=asset_name,
                        version=version,
                        path=self.asset_root / path,
                        is_valid=False,
                        errors=[error_msg],
                    )
                    summary.results.append(err_result)
                    if progress_callback:
                        progress_callback(len(summary.results), summary.total, err_result)
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
    """Entrypoint for `validate-assets`.

    - If `click` is installed, uses a richer click-based CLI.
    - Otherwise falls back to argparse to keep the command working with minimal deps.
    """

    try:
        import click
    except ImportError:
        _main_cli_argparse()
        return

    @click.command(context_settings={"help_option_names": ["-h", "--help"]})
    @click.option("--asset-root", type=click.Path(path_type=Path), help="Asset root directory")
    @click.option("--cache-root", type=click.Path(path_type=Path), help="Cache directory")
    @click.option("--threads", type=int, default=None, help="Number of threads")
    @click.option("--timeout", type=int, default=None, help="Timeout in seconds")
    @click.option("--output", type=click.Path(path_type=Path), help="Output report JSON file")
    @click.option(
        "--input",
        "input_path",
        type=click.Path(path_type=Path, exists=True, dir_okay=False),
        help="Read assets from a .txt (name:version:path per line) or .json file",
    )
    @click.option(
        "--db",
        "db_backend",
        type=click.Choice(["mock", "postgres", "mongo"], case_sensitive=False),
        default="mock",
        show_default=True,
        help="Database backend",
    )
    @click.option("--dsn", type=str, default=None, help="PostgreSQL DSN (for --db postgres)")
    @click.option("--mongo-uri", type=str, default=None, help="MongoDB URI (for --db mongo)")
    @click.option("--mongo-db", type=str, default="asset_db", show_default=True)
    @click.option("--mongo-collection", type=str, default="assets", show_default=True)
    @click.argument("assets", nargs=-1)
    def _cli(
        asset_root: Path | None,
        cache_root: Path | None,
        threads: int | None,
        timeout: int | None,
        output: Path | None,
        input_path: Path | None,
        db_backend: str,
        dsn: str | None,
        mongo_uri: str | None,
        mongo_db: str,
        mongo_collection: str,
        assets: tuple[str, ...],
    ) -> None:
        try:
            parsed_assets = _collect_assets(list(assets), input_path=input_path)
        except ValueError as exc:
            raise click.UsageError(str(exc)) from exc

        db = _create_db(
            db_backend=db_backend.lower(),
            dsn=dsn,
            mongo_uri=mongo_uri,
            mongo_db=mongo_db,
            mongo_collection=mongo_collection,
        )

        validator = AssetValidator(
            asset_root=asset_root,
            cache_root=cache_root,
            db=db,
            threads=threads,
            timeout=timeout,
        )

        summary = validator.validate_batch(parsed_assets)
        summary.print_report()

        if output:
            validator.export_report(summary, output)

        raise SystemExit(0 if summary.failed == 0 else 1)

    _cli()


def _create_db(
    db_backend: str,
    dsn: str | None,
    mongo_uri: str | None,
    mongo_db: str,
    mongo_collection: str,
) -> AssetDatabase:
    if db_backend == "mock":
        return MockAssetDatabase()
    if db_backend == "postgres":
        if not dsn:
            raise ValueError("--dsn is required for --db postgres")
        return PostgresAssetDatabase(dsn=dsn)
    if db_backend == "mongo":
        if not mongo_uri:
            raise ValueError("--mongo-uri is required for --db mongo")
        return MongoAssetDatabase(uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection)
    raise ValueError(f"Unknown db backend: {db_backend}")


def _parse_asset_triplet(asset_str: str) -> Tuple[str, str, str]:
    try:
        name, version, path = asset_str.split(":", 2)
    except ValueError as exc:
        raise ValueError(f"Invalid asset format: {asset_str} (expected name:version:path)") from exc
    if not name or not version or not path:
        raise ValueError(f"Invalid asset format: {asset_str} (empty field)")
    return name, version, path


def _collect_assets(asset_args: List[str], input_path: Path | None) -> List[Tuple[str, str, str]]:
    if input_path:
        loaded = _load_assets_file(input_path)
        if asset_args:
            loaded.extend(asset_args)
        asset_args = loaded

    if not asset_args:
        raise ValueError("No assets provided. Pass assets or use --input.")

    return [_parse_asset_triplet(a) for a in asset_args]


def _load_assets_file(path: Path) -> List[str]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("JSON input must be a list of 'name:version:path' strings")
        out: List[str] = []
        for item in data:
            if not isinstance(item, str):
                raise ValueError("JSON input list must contain only strings")
            out.append(item)
        return out

    # default: text
    lines = path.read_text(encoding="utf-8").splitlines()
    out = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def _main_cli_argparse() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Validate assets in VFX pipeline")
    parser.add_argument("--asset-root", type=str, help="Asset root directory")
    parser.add_argument("--cache-root", type=str, help="Cache directory")
    parser.add_argument("--threads", type=int, default=None, help="Number of threads")
    parser.add_argument("--timeout", type=int, default=None, help="Timeout in seconds")
    parser.add_argument("--output", type=str, help="Output report file")
    parser.add_argument("--input", dest="input_path", type=str, help="Assets input .txt/.json file")
    parser.add_argument(
        "--db",
        dest="db_backend",
        choices=["mock", "postgres", "mongo"],
        default="mock",
        help="Database backend",
    )
    parser.add_argument("--dsn", type=str, default=None, help="PostgreSQL DSN (for --db postgres)")
    parser.add_argument("--mongo-uri", type=str, default=None, help="MongoDB URI (for --db mongo)")
    parser.add_argument("--mongo-db", type=str, default="asset_db")
    parser.add_argument("--mongo-collection", type=str, default="assets")
    parser.add_argument("assets", nargs="*", help="Assets in format name:version:path")

    args = parser.parse_args()

    input_path = Path(args.input_path) if args.input_path else None
    try:
        assets = _collect_assets(list(args.assets), input_path=input_path)
        db = _create_db(
            db_backend=args.db_backend,
            dsn=args.dsn,
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            mongo_collection=args.mongo_collection,
        )
    except ValueError as exc:
        logging.getLogger(__name__).error("%s", exc)
        raise SystemExit(2) from None

    validator = AssetValidator(
        asset_root=args.asset_root,
        cache_root=args.cache_root,
        db=db,
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
