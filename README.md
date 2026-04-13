# Asset Validator (Production Validation Framework)

![AssetValidator icon](assets/icon.svg)

Production-grade asset validation for VFX pipelines with USD support, parallel validation, and database integration.

## Latest Release (Recommended)

Use the newest release first.  
It includes the modern GUI theme and color-coded validation details (green `PASS`, red `FAIL` with error reasons).

- Release page: [Latest](https://github.com/daniilcg/AssetValidator/releases/latest)
- Windows app (`.exe`): [Download AssetValidator.exe](https://github.com/daniilcg/AssetValidator/releases/latest/download/AssetValidator.exe)
- Linux app (standalone binary): [Download AssetValidator](https://github.com/daniilcg/AssetValidator/releases/latest/download/AssetValidator)
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

| Plan | Target | Includes | Monthly | Yearly (3% off) | You save |
| --- | --- | --- | ---: | ---: | ---: |
| Free (OSS) | Individuals, evaluation | Core validator, CLI/GUI, source code, community issues | $0.00 | $0.00 | $0.00 |
| Personal | Solo creators | Ready-to-run binaries, priority bug triage | $10.00 | $116.40 | $3.60 |
| Team | Small teams | Team workflows, roadmap voting, faster support | $25.00 | $291.00 | $9.00 |
| Business | Production teams | Integration help, custom validation profiles, SLA/support channel | $99.00 | $1,152.36 | $35.64 |
| Premium Business | Studios with high load | Priority SLA, onboarding, advanced integration support | $199.00 | $2,316.36 | $71.64 |

**Totals across paid plans (Personal + Team + Business + Premium Business):**
- Monthly total: **$333.00**
- Yearly total without discount: **$3,996.00**
- Yearly total with 3% discount: **$3,876.12**
- Total yearly savings: **$119.88**

### Contact and Payment

- Contact email: [assetvalidator@gmail.com](mailto:assetvalidator@gmail.com)
- PayPal (custom amount): [Pay with PayPal](https://www.paypal.me/daniilsegal90)

### Choose Plan and Pay

- **Free (OSS)**: [Start free on GitHub](https://github.com/daniilcg/AssetValidator)
- **Personal - $10/mo**: [Pay $10](https://www.paypal.me/daniilsegal90/10) | **$116.40/year**: [Pay $116.40](https://www.paypal.me/daniilsegal90/116.40)
- **Team - $25/mo**: [Pay $25](https://www.paypal.me/daniilsegal90/25) | **$291/year**: [Pay $291](https://www.paypal.me/daniilsegal90/291)
- **Business - $99/mo**: [Pay $99](https://www.paypal.me/daniilsegal90/99) | **$1,152.36/year**: [Pay $1152.36](https://www.paypal.me/daniilsegal90/1152.36)
- **Premium Business - $199/mo**: [Pay $199](https://www.paypal.me/daniilsegal90/199) | **$2,316.36/year**: [Pay $2316.36](https://www.paypal.me/daniilsegal90/2316.36)

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
import logging

from pipeline.core.asset_validator import AssetValidator
from pipeline.core.db_interface import MockAssetDatabase

logging.basicConfig(level=logging.INFO)

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
    logging.info("Validation failed for %s assets", summary.failed)
else:
    logging.info("All assets OK, starting render")
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

PROJECT_PATH: Base directory when `ASSETVALIDATOR_BASE_DIR` is unset (use instead of a fixed path like `D:/Work/...`).

ASSETVALIDATOR_BASE_DIR: Base directory for default `ASSET_ROOT` / `CACHE_ROOT` (overrides `PROJECT_PATH` when set).

ASSET_ROOT: Root directory for assets (default: `{base}/assets`)

CACHE_ROOT: Cache directory (default: `{base}/.asset_validator_cache`)

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