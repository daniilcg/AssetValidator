from __future__ import annotations

import json
import html
import sys
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

STYLE_SHEET = """
QMainWindow {
    background-color: #272822;
}
QWidget {
    background: transparent;
    color: #F8F8F2;
    font-size: 12px;
}
QFrame#HeaderFrame {
    border: 1px solid #3E3D32;
    border-radius: 10px;
    background: qlineargradient(
        x1:0, y1:0, x2:1, y2:1,
        stop:0 #31322C, stop:1 #262822
    );
}
QGroupBox {
    border: 1px solid #3E3D32;
    border-radius: 8px;
    margin-top: 12px;
    padding: 8px;
    background-color: #2D2E27;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 6px;
    color: #A6E22E;
    font-weight: 600;
}
QLineEdit, QComboBox, QTextEdit, QTableWidget {
    background-color: #1F201B;
    border: 1px solid #49483E;
    border-radius: 6px;
    padding: 6px;
    color: #F8F8F2;
    selection-background-color: #66D9EF;
    selection-color: #1B1D1A;
}
QComboBox::drop-down {
    border: none;
    width: 20px;
}
QComboBox QAbstractItemView {
    background-color: #1F201B;
    border: 1px solid #49483E;
    color: #F8F8F2;
    selection-background-color: #66D9EF;
    selection-color: #1B1D1A;
}
QTableWidget {
    gridline-color: #3E3D32;
}
QHeaderView::section {
    background-color: #34352E;
    color: #FD971F;
    border: 0;
    border-right: 1px solid #3E3D32;
    padding: 6px;
    font-weight: 600;
}
QPushButton {
    border: 1px solid #5A594A;
    border-radius: 8px;
    background-color: #3D3E36;
    color: #F8F8F2;
    padding: 7px 12px;
}
QPushButton:hover {
    background-color: #4A4B42;
}
QPushButton:disabled {
    color: #8A8A80;
    border-color: #4A4B42;
    background-color: #32332C;
}
QPushButton#PrimaryButton {
    border-color: #6AAE27;
    background: qlineargradient(
        x1:0, y1:0, x2:0, y2:1,
        stop:0 #A6E22E, stop:1 #7DB221
    );
    color: #1B1D1A;
    font-weight: 700;
}
QPushButton#PrimaryButton:hover {
    background: qlineargradient(
        x1:0, y1:0, x2:0, y2:1,
        stop:0 #B4F037, stop:1 #88C526
    );
}
QStatusBar {
    background-color: #1F201B;
    color: #E6DB74;
    border-top: 1px solid #3E3D32;
}
"""


def _resource_root() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parents[2]


def _project_icon_path() -> Path:
    return (_resource_root() / "assets" / "icon.svg").resolve()


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

        self.btn_add.clicked.connect(self._add_asset_row)
        self.btn_import.clicked.connect(self._import_assets)
        self.btn_clear.clicked.connect(self._clear_assets)
        self.btn_validate.clicked.connect(self._start_validation)
        self.db_backend.currentTextChanged.connect(self._on_db_backend_changed)

        self._build_layout()
        self._on_db_backend_changed(self.db_backend.currentText())

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
        title.setStyleSheet("font-size: 18px; font-weight: 700; color: #F8F8F2;")
        subtitle = QLabel("Clean validation for VFX/CG assets")
        subtitle.setStyleSheet("font-size: 12px; color: #A6E22E;")
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header_layout.addLayout(title_box)
        header_layout.addStretch(1)

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
        self._append_detail_log("Starting validation run…", "#E6DB74")

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
                self._append_detail_log(f"PASS {asset_ref}", "#A6E22E")
            else:
                self._append_detail_log(f"FAIL {asset_ref}", "#F92672")
                if r.errors:
                    for err in r.errors:
                        self._append_detail_log(f"  - {err}", "#FF6F91")
                else:
                    self._append_detail_log("  - Unknown validation error", "#FF6F91")

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
                "#E6DB74",
            )
        else:
            self._append_log("Done.")
            self._append_detail_log("Done.", "#E6DB74")

    @Slot(str)
    def _on_failed(self, message: str) -> None:
        self.btn_validate.setEnabled(True)
        self.statusBar().clearMessage()
        QMessageBox.critical(self, "Validation failed", message)
        self._append_log(f"ERROR: {message}")
        self._append_detail_log(f"ERROR: {message}", "#F92672")

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
