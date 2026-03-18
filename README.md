# Asset Validator

![AssetValidator icon](assets/icon.svg)

Production-grade asset validation for VFX pipelines with USD support, parallel validation, and database integration.

## Latest Release (Recommended)

**Use the newest release first: `v0.1.4-r3`**  
This release includes the modern GUI, dark Sublime-style theme, and color-coded validation details (green `PASS`, red `FAIL` with error reasons).

- Release page: [v0.1.4-r3](https://github.com/daniilcg/AssetValidator/releases/tag/v0.1.4-r3)
- Windows app (`.exe`): [Download AssetValidator.exe](https://github.com/daniilcg/AssetValidator/releases/download/v0.1.4-r3/AssetValidator.exe)
- Linux app (standalone binary): [Download AssetValidator](https://github.com/daniilcg/AssetValidator/releases/download/v0.1.4-r3/AssetValidator)
- All previous versions remain available on the [Releases](https://github.com/daniilcg/AssetValidator/releases) page.

### GUI Preview

![AssetValidator GUI preview](assets/gui-preview-v0.1.4-r3.png)

Simple workflow:

1. Open app
2. Set asset/cache folders
3. Import assets (`.txt` / `.json`) or add rows manually
4. Click **Validate**
5. Read results in output panel

## Open Source

- **GitHub**: `https://github.com/daniilcg/AssetValidator`
- **License**: MIT
- **Email (business/support)**: [assetvalidator@gmail.com](mailto:assetvalidator@gmail.com)
- **Donations (PayPal)**: [paypal.me/daniilsegal90](https://www.paypal.me/daniilsegal90)

## Monetization and Support

AssetValidator remains open source. The monetization strategy focuses on convenience, integrations, and support for teams.

### Suggested Plans

| Plan | Target | Includes |
| --- | --- | --- |
| Free (OSS) | Individuals, evaluation | Core validator, CLI/GUI, source code, community issues |
| Indie | Freelancers, small teams | Ready-to-run binaries, priority bug triage, roadmap voting |
| Studio | Production teams | Integration help, custom validation profiles, SLA/support channel |

### Contact and Payment

- Contact email: [assetvalidator@gmail.com](mailto:assetvalidator@gmail.com)
- PayPal (custom amount): [Pay with PayPal](https://www.paypal.me/daniilsegal90)
- Quick support options:
  - [Pay $10](https://www.paypal.me/daniilsegal90/10)
  - [Pay $25](https://www.paypal.me/daniilsegal90/25)
  - [Pay $99](https://www.paypal.me/daniilsegal90/99)

### What Can Be Paid

- **Convenience**: prebuilt binaries, update notifications, setup guides.
- **Team features**: validation profiles, report history, dashboard/export templates.
- **Integrations**: ShotGrid/FTrack/Slack/Telegram/Jenkins/GitHub Actions presets.
- **Support**: onboarding sessions, pipeline audits, custom rule development.

### Quick Start for Monetization

1. Publish pricing and plan comparison.
2. Keep core engine open and stable.
3. Offer paid support/integration as first revenue stream.
4. Expand to pro add-ons based on user demand.

See `MONETIZATION.md` for a practical rollout roadmap.

## Features

- Parallel validation of multiple assets
- USD file integrity checking
- Hash-based version verification
- Caching for performance
- Database integration (PostgreSQL/MongoDB ready)
- CLI interface
- Simple GUI (optional)
- Full test suite

## Installation

```bash
# Clone repo
git clone <your-repo-url> AssetValidator
cd AssetValidator

# Install runtime (editable)
pip install -e .

# With CLI extras
pip install -e ".[cli]"

# With GUI
pip install -e ".[gui]"

# Launch GUI
asset-validator-gui

# With DB extras (choose what you need)
pip install -e ".[postgres]"
pip install -e ".[mongodb]"

# For development / tests
pip install -e ".[test]"
```

## Usage

### As a library

```python
from pipeline.core.asset_validator import AssetValidator
from pipeline.core.db_interface import MockAssetDatabase

# Create validator
validator = AssetValidator(
    asset_root="/mnt/library/assets",
    cache_root="/mnt/cache/asset_validator",
    db=MockAssetDatabase(),
    threads=16
)

# Validate assets
assets = [
    ("dragon", "v042", "characters/dragon/v042/dragon_v042.usd"),
    ("tree", "v017", "environments/tree/v017/tree_v017.usd"),
]

summary = validator.validate_batch(assets)
summary.print_report()

if summary.failed > 0:
    print(f"Validation failed for {summary.failed} assets")
else:
    print("All assets OK, starting render")
```

### From command line

```bash
validate-assets --threads 16 --output report.json ^
    dragon:v042:characters/dragon/v042/dragon_v042.usd ^
    tree:v017:environments/tree/v017/tree_v017.usd
```

### Integration with render farm

```python
def submit_render(job_name, asset_list):
    validator = AssetValidator()
    summary = validator.validate_batch(asset_list)
    
    if summary.failed > 0:
        send_alert(f"Render blocked: {summary.failed} assets failed validation")
        return False
    
    return submit_to_farm(job_name, asset_list)
```

## Configuration

Environment variables:

ASSET_ROOT: Root directory for assets (default: /mnt/library/assets)

CACHE_ROOT: Cache directory (default: /mnt/cache/asset_validator)

VALIDATION_THREADS: Number of threads (default: 16)

ENABLE_CACHE: Enable/disable caching (default: true)

LOG_LEVEL: Logging level (default: INFO)

## Testing

```bash
pytest tests/ -v
```

## Database backends

### PostgreSQL

Install extra:

```bash
pip install -e ".[postgres]"
```

Create table (minimal example):

```sql
CREATE TABLE assets (
  asset_name text NOT NULL,
  version    text NOT NULL,
  hash       text NOT NULL,
  path       text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (asset_name, version)
);
```

Use in code:

```python
from pipeline.core.db_interface import PostgresAssetDatabase
from pipeline.core.asset_validator import AssetValidator

db = PostgresAssetDatabase(
    dsn="postgresql://render:password@localhost:5432/asset_db"
)
validator = AssetValidator(db=db)
```

### MongoDB

Install extra:

```bash
pip install -e ".[mongodb]"
```

Use in code:

```python
from pipeline.core.db_interface import MongoAssetDatabase
from pipeline.core.asset_validator import AssetValidator

db = MongoAssetDatabase(
    uri="mongodb://localhost:27017",
    db_name="asset_db",
    collection_name="assets",
)
validator = AssetValidator(db=db)
```

## License
This project is licensed under the MIT License - see the [LICENSE] file for details.