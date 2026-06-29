# AssetValidator — User Manual

**Version 2.6.0** · [Product site](https://daniilcg.github.io/AssetValidator) · [Commercial License](LICENSE)

AssetValidator is **commercial software** with a **14-day free trial**. After trial, a paid license is required to run validation.

AssetValidator is a **USD-first publish gate** for VFX/CG pipelines: file validation, dependency closure, hash verification, DCC in-scene checks, and studio integrations (ShotGrid, ftrack, Slack, Telegram, Jira, Deadline).

---

## Table of contents

1. [Quick start](#quick-start)
2. [GUI](#gui)
3. [CLI](#cli)
4. [Rulesets](#rulesets)
5. [Validation checks reference](#validation-checks-reference)
6. [DCC integrations](#dcc-integrations)
7. [DCC JSON bridge](#dcc-json-bridge)
8. [Studio integrations](#studio-integrations)
9. [CI/CD](#cicd)
10. [AI assistant & support](#ai-assistant--support)
11. [Environment variables](#environment-variables)

---

## Quick start

### Windows (installer)

1. Download [AssetValidator-Setup.exe](https://github.com/daniilcg/AssetValidator/releases/latest/download/AssetValidator-Setup.exe) from the product website
2. Install and launch **AssetValidator**
3. Set **Asset root** and **Cache root**
4. Add assets or use **Scan folder**
5. Click **Validate**

### Python (developers)

```bash
pip install -e ".[cli,gui,test,images]"
validate-assets --list-rulesets
validate-assets --asset-root ./assets hero:v001:hero/v001/hero_v001.usd
asset-validator-gui
```

---

## GUI

| Area | Purpose |
|------|---------|
| **Paths** | Asset root, cache, threads, timeout, ruleset |
| **Options** | Watch folder, fail on warnings, Slack/Jira notify, DCC report import |
| **Database** | mock / Postgres / Mongo for hash history |
| **AI Assistant** | Offline help + email support (`TIKET_AV: …`) |
| **Manual** | Opens the in-app user manual (no browser) |
| **Import DCC report** | Merge Maya/Houdini/Blender scene JSON with file validation |

**Workflow:** import assets → choose ruleset (`default`, `film`, `strict`) → **Validate** → read green PASS / red FAIL in the details panel.

**Pro:** Export JSON, Version diff, Pro dashboard (history).

---

## CLI

```bash
validate-assets --asset-root /assets --ruleset film \
  hero:v001:hero/v001/hero_v001.usd

validate-assets --list-checks
validate-assets --ruleset strict --fail-on-warning --report junit.xml ...
```

**Asset triplet format:** `name:version:relative_path` (relative to asset root).

**Notifications:** `--notify slack` or `--notify telegram` or `--notify jira`

---

## Rulesets

| Ruleset | Use case |
|---------|----------|
| `default` | Daily USD checks, UDIM, metadata fps=24 |
| `film` | Broader extensions, texture whitelist, prim naming |
| `strict` | Publish gate: .exr/.tx only, power-of-2, timecode, colorSpace |

Custom YAML: copy `pipeline/rulesets/default.yml`, add checks, point GUI ruleset `…` button at file.

### Key ruleset parameters

```yaml
params:
  expected_fps: 24
  require_timecode: false        # strict: true
  require_color_space: false     # strict: true
  allowed_color_spaces: ["ACES - ACEScg", raw]
  prim_name_regex: "^[a-z][a-z0-9_]*$"
  allowed_texture_extensions: [.exr, .tx]
  require_power_of_two: true     # needs pip install asset-validator[images]
  forbid_absolute_paths: true
  check_udim_completeness: true
```

---

## Validation checks reference

### Paths & naming
| Check ID | Codes |
|----------|-------|
| `paths.naming_template` | `PATH_TEMPLATE_MISMATCH`, `ASSET_NAME_INVALID`, `VERSION_INVALID` |
| `paths.allowed_extensions` | `EXT_NOT_ALLOWED` |
| `paths.no_parent_traversal` | `PATH_TRAVERSAL` |

### USD
| Check ID | Codes |
|----------|-------|
| `usd.dependency_closure` | `MISSING_DEPENDENCY` |
| `usd.deep_validation` | `USD_NO_DEFAULT_PRIM`, `USD_EMPTY_STAGE`, `USD_INVALID_MPU` |
| `usd.absolute_paths` | `PATH_ABSOLUTE_LOCAL` |
| `usd.metadata` | `USD_FPS_MISMATCH`, `USD_TIMECODE_MISSING`, `USD_COLORSPACE_*`, `USD_PRIM_NAME_INVALID` |

### Textures
| Check ID | Codes |
|----------|-------|
| `media.texture_preflight` | `TEXTURE_MISSING`, `UDIM_TILE_MISSING`, `TEXTURE_FORMAT_NOT_ALLOWED`, `TEXTURE_NOT_POWER_OF_TWO` |

### Core (always on file validate)
| Code | Meaning |
|------|---------|
| `VERSION_MISMATCH` | File hash ≠ database |
| `core.file_exists` | Asset missing on disk |

### DCC scene (in Maya/Houdini/Blender)
| DCC | Example codes |
|-----|----------------|
| Maya | `MAYA_TRANSFORM_NOT_FROZEN`, `MAYA_NGON_FOUND`, `MAYA_UV_OUT_OF_BOUNDS` |
| Houdini | `HOUDINI_NGON_FOUND`, `HOUDINI_TRANSFORM_NOT_FROZEN` |
| Blender | `BLENDER_NGON_FOUND`, `BLENDER_UV_OUT_OF_BOUNDS` |

---

## DCC integrations

### Maya

**One-time install (pick one):**

1. **Menu (recommended)** — Script Editor, once:
   ```python
   import sys; sys.path.insert(0, r"D:/path/to/AssetValidator")
   import runpy; runpy.run_path(r"D:/path/to/AssetValidator/integrations/maya/install_menu.py")
   ```
2. **Plugin** — copy `integrations/maya/AssetValidator.py` to `MAYA_PLUG_IN_PATH`, then:
   `cmds.loadPlugin("AssetValidator.py")`
3. **Auto on startup** — append `integrations/maya/userSetup.example.py` to your `userSetup.py`
   and set `ASSETVALIDATOR_ROOT` to the repo path.

**Menu items:** Preflight Export · Scene Check Only · Export DCC Report JSON

Shelf: run `install_shelf.mel` (edit path inside) or use menu.

```python
from pathlib import Path
from pipeline.integrations.maya import preflight_selected_export
preflight_selected_export(Path(r"D:/assets"), ruleset="film", fail_on_warning=True)
```

### Houdini

**One-time install:**

```python
import sys; sys.path.insert(0, r"D:/path/to/AssetValidator")
import runpy; runpy.run_path(r"D:/path/to/AssetValidator/integrations/houdini/install.py")
```

This copies **AssetValidator** shelf to your Houdini `Shelves/` folder. Restart Houdini or add the shelf tab.

**Shelf tools:**
| Tool | Action |
|------|--------|
| AV Preflight | Scene + USD export validation |
| AV Scene Only | Geometry/transform checks |
| Create Preflight ROP | Python ROP under `/out` — blocks render on FAIL |
| Install / Refresh | Re-copy shelf file |

Wire the Preflight ROP **before** your USD ROP in the chain.

```python
from pipeline.integrations.houdini import preflight_selected_rop
preflight_selected_rop(Path("$HIP/../assets"), fail_on_warning=True)
```

### Blender

```python
from pipeline.integrations.blender import preflight_active_export
result = preflight_active_export(Path("/assets"), fail_on_warning=True)
```

CLI: `validate-blender --blender-active --asset-root ...`

### Unreal

Run inside Editor Python:

```python
exec(open(r"path/to/integrations/unreal/preflight.py").read())
```

Set `ASSET_ROOT` and `ASSETVALIDATOR_RULESET` environment variables.

---

## DCC JSON bridge

DCC tools can write a scene report JSON; CLI/GUI merges it with file validation.

```bash
validate-dcc-report --dcc-report scene.json --asset-root /assets \
  hero:v001:hero/v001/hero_v001.usd
```

**JSON format:**

```json
{
  "dcc": "maya",
  "asset_name": "hero",
  "export_path": "hero/v001/hero_v001.usd",
  "issues": [
    {
      "code": "MAYA_NGON_FOUND",
      "message": "5 n-gons on |hero",
      "hint": "Triangulate",
      "severity": "error",
      "category": "maya"
    }
  ]
}
```

Environment: `ASSETVALIDATOR_DCC_REPORT=/path/scene.json`

---

## Studio integrations

| System | Command | Notes |
|--------|---------|-------|
| **ShotGrid** | `validate-shotgrid --published-file-id 123 --asset-root ...` | Posts Note on result |
| **ShotGrid event** | `pipeline/integrations/shotgrid_event.py` | Daemon plugin blocks publish |
| **ftrack** | `validate-ftrack --asset-version-id ...` | |
| **Deadline** | `scripts/deadline_preflight.py` | Pre-job script |
| **Slack** | `SLACK_WEBHOOK_URL` + `--notify slack` | |
| **Telegram** | `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | |
| **Jira** | `JIRA_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN`, `JIRA_PROJECT_KEY` | Creates issue on FAIL |

---

## CI/CD

- **GitHub Actions:** `.github/workflows/ci.yml` — tests, ruff, PyInstaller release
- **Reusable action:** `.github/actions/validate-assets/`
- **Jenkins:** `Jenkinsfile` in repo root

```yaml
- uses: ./.github/actions/validate-assets
  with:
    asset-root: ./assets
    input: examples/ci-assets.txt
    ruleset: film
```

---

## AI assistant & support

- GUI button **AI Assistant** — local knowledge base (+ optional `OPENAI_API_KEY`)
- Email: **assetvalidator@gmail.com**
- Subject: `TIKET_AV: <plan> user_<id>`

---

## Environment variables

| Variable | Purpose |
|----------|---------|
| `ASSET_ROOT` | Default asset root |
| `ASSETVALIDATOR_RULESET` | default / film / strict |
| `ASSETVALIDATOR_LICENSE_KEY` | Paid license key (`AV2-TEAM-…`) — required after 14-day trial |
| `OPENAI_API_KEY` | Richer AI answers |
| `SHOTGRID_URL`, `SHOTGRID_SCRIPT_NAME`, `SHOTGRID_API_KEY` | ShotGrid |
| `SLACK_WEBHOOK_URL` | Slack |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | Telegram |
| `JIRA_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN`, `JIRA_PROJECT_KEY` | Jira |
| `ASSETVALIDATOR_DCC_REPORT` | Path to DCC JSON report |

---

## License and trial

1. **First launch** starts a 14-day trial (stored under your user profile).
2. **During trial** — full validation in GUI and CLI.
3. **After trial** — validation is blocked until you activate a paid key.
4. **GUI:** header shows days left; click **License** to activate.
5. **CLI:**
   ```bash
   validate-assets --license-status
   validate-assets --activate-license AV2-PERSONAL-PERPETUAL-XXXXXXXXXXXX
   ```

Purchase: [PayPal](https://www.paypal.me/daniilsegal90) · email [assetvalidator@gmail.com](mailto:assetvalidator@gmail.com) for your key.

---

## Support

- Email: assetvalidator@gmail.com
- Product site: https://daniilcg.github.io/AssetValidator
- User manual: https://daniilcg.github.io/AssetValidator/MANUAL.md
