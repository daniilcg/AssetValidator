"""Database interface and implementations for asset metadata."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from pipeline.core.exceptions import DatabaseError


class AssetDatabase(ABC):
    """Abstract interface for asset database operations."""

    @abstractmethod
    def get_asset_hash(self, asset_name: str, version: str) -> Optional[str]:
        """Get expected hash for asset version."""

    @abstractmethod
    def get_asset_path(self, asset_name: str, version: str) -> Optional[str]:
        """Get relative path for asset version."""

    @abstractmethod
    def update_asset_hash(self, asset_name: str, version: str, hash_value: str) -> bool:
        """Update asset hash after validation."""

    @abstractmethod
    def get_all_versions(self, asset_name: str) -> List[str]:
        """Get all versions of an asset."""


class MockAssetDatabase(AssetDatabase):
    """In-memory implementation for tests and examples."""

    def __init__(self):
        self._storage: Dict[tuple, dict] = {
            ("dragon", "v042"): {
                "hash": "abc123def456",
                "path": "characters/dragon/v042/dragon_v042.usd",
                "updated": datetime.now(),
            },
            ("tree", "v017"): {
                "hash": "def789abc123",
                "path": "environments/tree/v017/tree_v017.usd",
                "updated": datetime.now(),
            },
        }

    def get_asset_hash(self, asset_name: str, version: str) -> Optional[str]:
        result = self._storage.get((asset_name, version))
        return result["hash"] if result else None

    def get_asset_path(self, asset_name: str, version: str) -> Optional[str]:
        result = self._storage.get((asset_name, version))
        return result["path"] if result else None

    def update_asset_hash(self, asset_name: str, version: str, hash_value: str) -> bool:
        key = (asset_name, version)
        if key in self._storage:
            self._storage[key]["hash"] = hash_value
            self._storage[key]["updated"] = datetime.now()
            return True
        return False

    def get_all_versions(self, asset_name: str) -> List[str]:
        return [v for (name, v) in self._storage.keys() if name == asset_name]


class PostgresAssetDatabase(AssetDatabase):
    """PostgreSQL implementation using psycopg2.

    Expects a table with at least:
        assets(asset_name text, version text, hash text, path text, updated_at timestamptz)

    Connection string example:
        postgresql://user:password@host:5432/asset_db
    """

    def __init__(self, dsn: str):
        try:
            import psycopg2  # type: ignore
        except ImportError as exc:
            raise DatabaseError(
                "psycopg2-binary is not installed. Install with `pip install '.[postgres]'`."
            ) from exc

        try:
            self._conn = psycopg2.connect(dsn)
        except Exception as exc:  # pragma: no cover - network/DSN specific
            raise DatabaseError(f"Failed to connect to PostgreSQL: {exc}") from exc

    def _fetchone(self, query: str, params: tuple) -> Optional[dict]:
        with self._conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            if not row:
                return None
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))

    def get_asset_hash(self, asset_name: str, version: str) -> Optional[str]:
        row = self._fetchone(
            "SELECT hash FROM assets WHERE asset_name=%s AND version=%s",
            (asset_name, version),
        )
        return row["hash"] if row else None

    def get_asset_path(self, asset_name: str, version: str) -> Optional[str]:
        row = self._fetchone(
            "SELECT path FROM assets WHERE asset_name=%s AND version=%s",
            (asset_name, version),
        )
        return row["path"] if row else None

    def update_asset_hash(self, asset_name: str, version: str, hash_value: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO assets (asset_name, version, hash, path, updated_at)
                VALUES (%s, %s, %s, COALESCE(
                    (SELECT path FROM assets WHERE asset_name=%s AND version=%s),
                    ''
                ), NOW())
                ON CONFLICT (asset_name, version)
                DO UPDATE SET hash=EXCLUDED.hash, updated_at=EXCLUDED.updated_at
                """,
                (asset_name, version, hash_value, asset_name, version),
            )
        self._conn.commit()
        return True

    def get_all_versions(self, asset_name: str) -> List[str]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT version FROM assets WHERE asset_name=%s", (asset_name,))
            rows = cur.fetchall()
        return [r[0] for r in rows]


class MongoAssetDatabase(AssetDatabase):
    """MongoDB implementation using pymongo.

    Expects a collection with documents:
        {asset_name, version, hash, path, updated_at}
    """

    def __init__(
        self,
        uri: str = "mongodb://localhost:27017",
        db_name: str = "asset_db",
        collection_name: str = "assets",
    ):
        try:
            from pymongo import MongoClient  # type: ignore
        except ImportError as exc:
            raise DatabaseError(
                "pymongo is not installed. Install with `pip install '.[mongodb]'`."
            ) from exc

        try:
            client = MongoClient(uri)
            self._collection = client[db_name][collection_name]
        except Exception as exc:  # pragma: no cover - network/URI specific
            raise DatabaseError(f"Failed to connect to MongoDB: {exc}") from exc

    def get_asset_hash(self, asset_name: str, version: str) -> Optional[str]:
        doc = self._collection.find_one(
            {"asset_name": asset_name, "version": version}, {"hash": 1}
        )
        return doc.get("hash") if doc else None

    def get_asset_path(self, asset_name: str, version: str) -> Optional[str]:
        doc = self._collection.find_one(
            {"asset_name": asset_name, "version": version}, {"path": 1}
        )
        return doc.get("path") if doc else None

    def update_asset_hash(self, asset_name: str, version: str, hash_value: str) -> bool:
        result = self._collection.update_one(
            {"asset_name": asset_name, "version": version},
            {
                "$set": {
                    "hash": hash_value,
                    "updated_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )
        return bool(result.matched_count or result.upserted_id)

    def get_all_versions(self, asset_name: str) -> List[str]:
        cursor = self._collection.find(
            {"asset_name": asset_name}, {"version": 1, "_id": 0}
        )
        return [doc["version"] for doc in cursor]