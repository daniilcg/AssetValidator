## Changelog

### v0.1.4-r3

- Add color-coded validation details in GUI (`PASS` in green, `FAIL` in red with reasons).
- Replace README GUI preview with real app screenshot (`assets/gui-preview-v0.1.4-r3.png`).
- Add direct clickable download links for Windows and Linux release binaries.

### v0.1.4-r2

- Publish both Windows (`AssetValidator.exe`) and Linux (`AssetValidator`) binaries in GitHub Releases.
- Split release publishing into a dedicated workflow job that downloads build artifacts from both runners.

### v0.1.4

- Stabilize CI pipeline and artifact uploads.
- Fix GUI files formatting issues that failed `ruff format --check`.
- Keep release path clean for Windows/Linux build artifacts on tags.

### v0.1.3

- Fix GitHub Actions release permissions for tag builds.

### v0.1.2

- Add optional GUI app (PySide6) and `asset-validator-gui` entrypoint.
- Build release artifacts on tags (Windows exe + Linux binary via PyInstaller).

### v0.1.1

- Improve developer experience: ruff/mypy/pre-commit and CI checks.
- Cross-platform settings defaults (Windows-friendly).
- CLI improvements (click when available + argparse fallback, input/output/db/progress).
- Project polish (README links, icon, English contributing guide).

### v0.1.0

- Initial public release.

