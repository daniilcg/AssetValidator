## Contributing

### Development environment

- Install Python 3.8+ (3.10+ рекомендовано).
- Создайте и активируйте виртуальное окружение.
- Установите зависимости:

```bash
pip install -e ".[test,cli]"
```

### Тесты

```bash
pytest -q
```

### Стиль кода

- Поддерживайте существующий стиль (black-like, 4 spaces).
- Не добавляйте лишние комментарии, описывающие очевидный код.
- Предпочитайте явные ошибки (raise `AssetValidationError` / `DatabaseError`) вместо немого падения.

