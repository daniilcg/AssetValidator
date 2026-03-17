## Contributing

### Development environment

- Install Python 3.8+ (3.10+ recommended).
- Create and activate a virtual environment.
- Install dependencies:

```bash
pip install -e ".[test,cli,dev]"
pre-commit install
```

### Tests

```bash
pytest -q
```

### Code style

- Keep formatting consistent (ruff format).
- Prefer explicit failures (`raise AssetValidationError` / `DatabaseError`) over silent fallbacks.
- Avoid comments that restate the obvious.

### Lint / type-check

```bash
ruff check .
ruff format --check .
mypy .
```

