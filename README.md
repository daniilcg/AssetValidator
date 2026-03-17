# Asset Validator

![AssetValidator icon](assets/icon.svg)

Production-grade asset validation for VFX pipelines with USD support, parallel validation, and database integration.

## Open Source

- **GitHub**: `https://github.com/daniilcg/AssetValidator`
- **License**: MIT
- **Donations (PayPal)**: `https://www.paypal.me/daniilsegal90`

## Features

- Parallel validation of multiple assets
- USD file integrity checking
- Hash-based version verification
- Caching for performance
- Database integration (PostgreSQL/MongoDB ready)
- CLI interface
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