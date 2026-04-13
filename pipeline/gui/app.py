from __future__ import annotations

import html
import json
import sys
import urllib.error
import urllib.request
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

from PySide6.QtCore import QObject, QRunnable, Qt, QThreadPool, Signal, Slot
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from pipeline.core.asset_validator import AssetValidator, ValidationResult, ValidationSummary
from pipeline.core.db_interface import (
    AssetDatabase,
    MockAssetDatabase,
    MongoAssetDatabase,
    PostgresAssetDatabase,
)

APP_VERSION = "0.1.5"
LATEST_RELEASE_API = "https://api.github.com/repos/daniilcg/AssetValidator/releases/latest"
LATEST_DOWNLOAD_WIN = "https://github.com/daniilcg/AssetValidator/releases/latest/download/AssetValidator.exe"
LATEST_DOWNLOAD_LINUX = "https://github.com/daniilcg/AssetValidator/releases/latest/download/AssetValidator"

STYLE_SHEET = """
QMainWindow {
    background-color: #090B10;
}
QWidget {
    background: transparent;
    color: #ECF1FF;
    font-size: 12px;
}
QFrame#HeaderFrame {
    border: 1px solid #2B3243;
    border-radius: 12px;
    background: qlineargradient(
        x1:0, y1:0, x2:1, y2:1,
        stop:0 #131722, stop:1 #10131B
    );
}
QGroupBox {
    border: 1px solid #2B3243;
    border-radius: 12px;
    margin-top: 12px;
    padding: 10px;
    background-color: #131722;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 6px;
    color: #7CFF4F;
    font-weight: 600;
}
QLineEdit, QComboBox, QTextEdit, QTableWidget {
    background-color: #0D1220;
    border: 1px solid #2B3243;
    border-radius: 10px;
    padding: 6px;
    color: #ECF1FF;
    selection-background-color: #34D4FF;
    selection-color: #081207;
}
QComboBox::drop-down {
    border: none;
    width: 20px;
}
QComboBox QAbstractItemView {
    background-color: #0D1220;
    border: 1px solid #2B3243;
    color: #ECF1FF;
    selection-background-color: #34D4FF;
    selection-color: #081207;
}
QTableWidget {
    gridline-color: #2B3243;
}
QHeaderView::section {
    background-color: #101526;
    color: #D7E5FF;
    border: 0;
    border-right: 1px solid #2B3243;
    padding: 6px;
    font-weight: 600;
}
QPushButton {
    border: 1px solid #2B3243;
    border-radius: 10px;
    background-color: #121728;
    color: #D8E1FF;
    padding: 7px 12px;
}
QPushButton:hover {
    background-color: #1A2232;
    border-color: #3A4B73;
}
QPushButton:disabled {
    color: #7C88A6;
    border-color: #2B3243;
    background-color: #10131B;
}
QPushButton#PrimaryButton {
    border-color: #9FFF70;
    background: qlineargradient(
        x1:0, y1:0, x2:0, y2:1,
        stop:0 #9BFF67, stop:1 #7CFF4F
    );
    color: #081207;
    font-weight: 700;
}
QPushButton#PrimaryButton:hover {
    background: qlineargradient(
        x1:0, y1:0, x2:0, y2:1,
        stop:0 #B6FF86, stop:1 #8DFF63
    );
}
QStatusBar {
    background-color: #0A0E17;
    color: #99A3BD;
    border-top: 1px solid #20283B;
}
"""


def _resource_root() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parents[2]


def _project_icon_path() -> Path:
    return (_resource_root() / "assets" / "icon.svg").resolve()


def _version_tuple(raw: str) -> Tuple[int, ...]:
    core = raw.strip().lstrip("v").split("-", 1)[0]
    parts: List[int] = []
    for token in core.split("."):
        try:
            parts.append(int(token))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def _latest_release_tag() -> str:
    req = urllib.request.Request(
        LATEST_RELEASE_API,
        headers={"Accept": "application/vnd.github+json", "User-Agent": "AssetValidator"},
    )
    with urllib.request.urlopen(req, timeout=4) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    tag = str(data.get("tag_name", "")).strip()
    if not tag:
        raise ValueError("Release tag is missing")
    return tag


def _parse_asset_triplet(asset_str: str) -> Tuple[str, str, str]:
    name, version, rel = asset_str.split(":", 2)
    if not name or not version or not rel:
        raise ValueError("Asset format must be name:version:path")
    return name, version, rel


def load_assets_file(path: Path) -> List[Tuple[str, str, str]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("JSON input must be a list of 'name:version:path' strings")
        return [_parse_asset_triplet(str(x)) for x in data]

    lines = path.read_text(encoding="utf-8").splitlines()
    out: List[Tuple[str, str, str]] = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(_parse_asset_triplet(s))
    return out


def create_db(
    backend: str,
    dsn: Optional[str],
    mongo_uri: Optional[str],
    mongo_db: str,
    mongo_collection: str,
) -> AssetDatabase:
    b = backend.lower()
    if b == "mock":
        return MockAssetDatabase()
    if b == "postgres":
        if not dsn:
            raise ValueError("Postgres DSN is required")
        return PostgresAssetDatabase(dsn=dsn)
    if b == "mongo":
        if not mongo_uri:
            raise ValueError("Mongo URI is required")
        return MongoAssetDatabase(uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection)
    raise ValueError(f"Unknown DB backend: {backend}")


@dataclass
class UiConfig:
    asset_root: Path
    cache_root: Path
    threads: int
    timeout: int

    db_backend: str = "mock"
    postgres_dsn: str = ""
    mongo_uri: str = ""
    mongo_db: str = "asset_db"
    mongo_collection: str = "assets"


class ValidationSignals(QObject):
    started = Signal(int)  # total
    progress = Signal(int, int, object)  # done, total, ValidationResult
    finished = Signal(object)  # ValidationSummary
    failed = Signal(str)


class ValidateWorker(QRunnable):
    def __init__(self, config: UiConfig, assets: List[Tuple[str, str, str]]):
        super().__init__()
        self.config = config
        self.assets = assets
        self.signals = ValidationSignals()

    @Slot()
    def run(self) -> None:
        try:
            db = create_db(
                backend=self.config.db_backend,
                dsn=self.config.postgres_dsn or None,
                mongo_uri=self.config.mongo_uri or None,
                mongo_db=self.config.mongo_db,
                mongo_collection=self.config.mongo_collection,
            )

            validator = AssetValidator(
                asset_root=self.config.asset_root,
                cache_root=self.config.cache_root,
                db=db,
                threads=self.config.threads,
                timeout=self.config.timeout,
            )

            total = len(self.assets)
            self.signals.started.emit(total)

            def _cb(done: int, tot: int, res: ValidationResult) -> None:
                self.signals.progress.emit(done, tot, res)

            summary = validator.validate_batch(self.assets, progress_callback=_cb)
            self.signals.finished.emit(summary)
        except Exception as exc:
            self.signals.failed.emit(str(exc))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AssetValidator")
        self.setMinimumSize(980, 640)

        icon_path = _project_icon_path()
        if icon_path.exists():
            self.setWindowIcon(QIcon(str(icon_path)))

        self.pool = QThreadPool.globalInstance()
        self.assets: List[Tuple[str, str, str]] = []

        self.asset_root = QLineEdit(str(Path.cwd() / "assets"))
        self.cache_root = QLineEdit(str(Path.cwd() / ".asset_validator_cache"))
        self.threads = QLineEdit("16")
        self.timeout = QLineEdit("300")

        self.db_backend = QComboBox()
        self.db_backend.addItems(["mock", "postgres", "mongo"])
        self.postgres_dsn = QLineEdit("")
        self.mongo_uri = QLineEdit("")
        self.mongo_db = QLineEdit("asset_db")
        self.mongo_collection = QLineEdit("assets")

        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Asset", "Version", "Relative path"])
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setPlaceholderText("Logs / status…")
        self.detail_log = QTextEdit()
        self.detail_log.setReadOnly(True)
        self.detail_log.setPlaceholderText("Detailed validation log (PASS/FAIL reasons)…")

        self.btn_add = QPushButton("Add asset")
        self.btn_import = QPushButton("Import .txt/.json")
        self.btn_clear = QPushButton("Clear list")
        self.btn_validate = QPushButton("Validate")
        self.btn_validate.setObjectName("PrimaryButton")
        self.btn_update = QPushButton("Check updates")

        self.btn_add.clicked.connect(self._add_asset_row)
        self.btn_import.clicked.connect(self._import_assets)
        self.btn_clear.clicked.connect(self._clear_assets)
        self.btn_validate.clicked.connect(self._start_validation)
        self.btn_update.clicked.connect(self._check_updates)
        self.db_backend.currentTextChanged.connect(self._on_db_backend_changed)

        self._build_layout()
        self._on_db_backend_changed(self.db_backend.currentText())
        self._check_updates(silent_up_to_date=True)

    def _build_layout(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)

        left = QVBoxLayout()
        right = QVBoxLayout()
        root_layout = QVBoxLayout(root)

        header = QFrame()
        header.setObjectName("HeaderFrame")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(14, 12, 14, 12)

        icon = QLabel()
        icon.setFixedSize(44, 44)
        icon_path = _project_icon_path()
        if icon_path.exists():
            pixmap = QPixmap(str(icon_path)).scaled(
                44, 44, Qt.KeepAspectRatio, Qt.SmoothTransformation
            )
            icon.setPixmap(pixmap)
        header_layout.addWidget(icon)

        title_box = QVBoxLayout()
        title = QLabel("AssetValidator")
        title.setStyleSheet("font-size: 18px; font-weight: 700; color: #ECF1FF;")
        subtitle = QLabel("Clean validation for VFX/CG assets")
        subtitle.setStyleSheet("font-size: 12px; color: #99A3BD;")
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header_layout.addLayout(title_box)
        header_layout.addStretch(1)
        header_layout.addWidget(self.btn_update)

        cfg = QGroupBox("Configuration")
        cfg_layout = QGridLayout(cfg)
        cfg_layout.addWidget(QLabel("Asset root"), 0, 0)
        cfg_layout.addWidget(self.asset_root, 0, 1)
        cfg_layout.addWidget(QLabel("Cache root"), 1, 0)
        cfg_layout.addWidget(self.cache_root, 1, 1)
        cfg_layout.addWidget(QLabel("Threads"), 2, 0)
        cfg_layout.addWidget(self.threads, 2, 1)
        cfg_layout.addWidget(QLabel("Timeout (s)"), 3, 0)
        cfg_layout.addWidget(self.timeout, 3, 1)

        db = QGroupBox("Database (optional)")
        db_layout = QGridLayout(db)
        db_layout.addWidget(QLabel("Backend (mock/postgres/mongo)"), 0, 0)
        db_layout.addWidget(self.db_backend, 0, 1)
        db_layout.addWidget(QLabel("Postgres DSN"), 1, 0)
        db_layout.addWidget(self.postgres_dsn, 1, 1)
        db_layout.addWidget(QLabel("Mongo URI"), 2, 0)
        db_layout.addWidget(self.mongo_uri, 2, 1)
        db_layout.addWidget(QLabel("Mongo DB"), 3, 0)
        db_layout.addWidget(self.mongo_db, 3, 1)
        db_layout.addWidget(QLabel("Mongo collection"), 4, 0)
        db_layout.addWidget(self.mongo_collection, 4, 1)

        actions = QHBoxLayout()
        actions.addWidget(self.btn_add)
        actions.addWidget(self.btn_import)
        actions.addWidget(self.btn_clear)
        actions.addStretch(1)
        self.btn_validate.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        actions.addWidget(self.btn_validate)

        left.addWidget(cfg)
        left.addWidget(db)
        left.addLayout(actions)
        left.addWidget(self.table, 1)

        right.addWidget(QLabel("Output"))
        right.addWidget(self.log, 1)
        right.addWidget(QLabel("Validation details"))
        right.addWidget(self.detail_log, 2)

        main = QHBoxLayout()
        main.addLayout(left, 3)
        main.addLayout(right, 2)
        root_layout.addWidget(header)
        root_layout.addLayout(main, 1)

    @Slot()
    def _check_updates(self, silent_up_to_date: bool = False) -> None:
        try:
            latest_tag = _latest_release_tag()
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
            if not silent_up_to_date:
                self._append_log("Could not check updates right now.")
            return

        latest = _version_tuple(latest_tag)
        current = _version_tuple(APP_VERSION)
        if latest <= current:
            if not silent_up_to_date:
                self._append_log(f"Already up to date (v{APP_VERSION}).")
            return

        self._append_log(f"Update available: {latest_tag} (current v{APP_VERSION})")
        target_url = LATEST_DOWNLOAD_WIN if sys.platform.startswith("win") else LATEST_DOWNLOAD_LINUX
        answer = QMessageBox.question(
            self,
            "Update available",
            f"New version {latest_tag} is available.\nCurrent version: v{APP_VERSION}\n\nOpen download page now?",
        )
        if answer == QMessageBox.Yes:
            webbrowser.open(target_url)

    @Slot()
    def _add_asset_row(self) -> None:
        r = self.table.rowCount()
        self.table.insertRow(r)
        self.table.setItem(r, 0, QTableWidgetItem("asset_name"))
        self.table.setItem(r, 1, QTableWidgetItem("v001"))
        self.table.setItem(r, 2, QTableWidgetItem("path/to/asset.usd"))

    @Slot()
    def _import_assets(self) -> None:
        path_str, _ = QFileDialog.getOpenFileName(
            self,
            "Import assets",
            str(Path.cwd()),
            "Assets (*.txt *.json);;All files (*.*)",
        )
        if not path_str:
            return
        try:
            assets = load_assets_file(Path(path_str))
        except Exception as exc:
            QMessageBox.critical(self, "Import failed", str(exc))
            return

        for name, ver, rel in assets:
            r = self.table.rowCount()
            self.table.insertRow(r)
            self.table.setItem(r, 0, QTableWidgetItem(name))
            self.table.setItem(r, 1, QTableWidgetItem(ver))
            self.table.setItem(r, 2, QTableWidgetItem(rel))

        self._append_log(f"Imported {len(assets)} assets from {path_str}")

    @Slot()
    def _clear_assets(self) -> None:
        self.table.setRowCount(0)
        self._append_log("Cleared asset list")

    def _collect_assets_from_table(self) -> List[Tuple[str, str, str]]:
        assets: List[Tuple[str, str, str]] = []
        for r in range(self.table.rowCount()):
            name = self.table.item(r, 0).text() if self.table.item(r, 0) else ""
            ver = self.table.item(r, 1).text() if self.table.item(r, 1) else ""
            rel = self.table.item(r, 2).text() if self.table.item(r, 2) else ""
            if not name or not ver or not rel:
                continue
            assets.append((name, ver, rel))
        return assets

    def _ui_config(self) -> UiConfig:
        return UiConfig(
            asset_root=Path(self.asset_root.text()).expanduser(),
            cache_root=Path(self.cache_root.text()).expanduser(),
            threads=int(self.threads.text() or "16"),
            timeout=int(self.timeout.text() or "300"),
            db_backend=self.db_backend.currentText().strip() or "mock",
            postgres_dsn=self.postgres_dsn.text().strip(),
            mongo_uri=self.mongo_uri.text().strip(),
            mongo_db=self.mongo_db.text().strip() or "asset_db",
            mongo_collection=self.mongo_collection.text().strip() or "assets",
        )

    @Slot()
    def _start_validation(self) -> None:
        assets = self._collect_assets_from_table()
        if not assets:
            QMessageBox.information(self, "Nothing to validate", "Add or import assets first.")
            return

        try:
            config = self._ui_config()
        except Exception as exc:
            QMessageBox.critical(self, "Invalid configuration", str(exc))
            return

        self.btn_validate.setEnabled(False)
        self._append_log(f"Starting validation: {len(assets)} assets…")
        self.detail_log.clear()
        self._append_detail_log("Starting validation run…", "#FFD36A")

        worker = ValidateWorker(config, assets)
        worker.signals.started.connect(self._on_started)
        worker.signals.progress.connect(self._on_progress)
        worker.signals.finished.connect(self._on_finished)
        worker.signals.failed.connect(self._on_failed)
        self.pool.start(worker)

    @Slot(int)
    def _on_started(self, total: int) -> None:
        self._append_log(f"Running… total={total}")

    @Slot(int, int, object)
    def _on_progress(self, done: int, total: int, res: object) -> None:
        r = res  # ValidationResult
        status = "OK" if getattr(r, "is_valid", False) else "FAIL"
        self.statusBar().showMessage(f"{done}/{total} — {status} {getattr(r, 'asset_name', '')}")
        if isinstance(r, ValidationResult):
            asset_ref = f"{r.asset_name} {r.version}"
            if r.is_valid:
                self._append_detail_log(f"PASS {asset_ref}", "#7CFF4F")
            else:
                self._append_detail_log(f"FAIL {asset_ref}", "#FF2F5B")
                if r.errors:
                    for err in r.errors:
                        self._append_detail_log(f"  - {err}", "#FF6B86")
                else:
                    self._append_detail_log("  - Unknown validation error", "#FF6B86")

    @Slot(object)
    def _on_finished(self, summary: object) -> None:
        self.btn_validate.setEnabled(True)
        self.statusBar().clearMessage()
        if isinstance(summary, ValidationSummary):
            self._append_log(
                f"Done. total={summary.total}, passed={summary.passed}, failed={summary.failed}"
            )
            self._append_detail_log(
                f"Done. total={summary.total}, passed={summary.passed}, failed={summary.failed}",
                "#FFD36A",
            )
        else:
            self._append_log("Done.")
            self._append_detail_log("Done.", "#FFD36A")

    @Slot(str)
    def _on_failed(self, message: str) -> None:
        self.btn_validate.setEnabled(True)
        self.statusBar().clearMessage()
        QMessageBox.critical(self, "Validation failed", message)
        self._append_log(f"ERROR: {message}")
        self._append_detail_log(f"ERROR: {message}", "#FF2F5B")

    def _append_log(self, text: str) -> None:
        self.log.append(text)

    def _append_detail_log(self, text: str, color: str) -> None:
        safe_text = html.escape(text)
        self.detail_log.append(f'<span style="color:{color}">{safe_text}</span>')

    @Slot(str)
    def _on_db_backend_changed(self, backend: str) -> None:
        b = backend.lower()
        use_pg = b == "postgres"
        use_mongo = b == "mongo"

        self.postgres_dsn.setEnabled(use_pg)
        self.mongo_uri.setEnabled(use_mongo)
        self.mongo_db.setEnabled(use_mongo)
        self.mongo_collection.setEnabled(use_mongo)


def main() -> None:
    app = QApplication(sys.argv)
    app.setStyleSheet(STYLE_SHEET)
    w = MainWindow()
    w.show()
    raise SystemExit(app.exec())


if __name__ == "__main__":
    main()
