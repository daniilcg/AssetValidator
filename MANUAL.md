# AssetValidator — User Manual

**Version 2.6.2** · [Product site](https://daniilcg.github.io/AssetValidator/)

AssetValidator is **commercial software** with a **14-day free trial**. After trial, a paid license is required.

---

## Quick start (Windows)

1. Download **AssetValidator-Setup.exe** from the [product website](https://daniilcg.github.io/AssetValidator/#download)
2. Install and launch **AssetValidator**
3. Set **Asset root** and **Cache root**
4. Add assets or use **Scan folder**
5. Click **Validate**

---

## GUI overview

| Area | Purpose |
|------|---------|
| **Paths** | Asset root, cache, threads, timeout, ruleset |
| **Options** | Watch folder, fail on warnings, notifications, DCC report import |
| **Database** | mock / Postgres / Mongo for hash history |
| **Manual** | In-app user manual (header button) |
| **AI Assistant** | Help and support |
| **License** | Trial status and key activation |

**Workflow:** import assets → choose ruleset (`default`, `film`, `strict`) → **Validate** → read PASS/FAIL in the details panel.

---

## CLI basics

```bash
validate-assets --asset-root /assets --ruleset film hero:v001:hero/v001/hero_v001.usd
validate-assets --list-rulesets
validate-assets --license-status
validate-assets --activate-license AV2-PERSONAL-PERPETUAL-XXXXXXXXXXXX
```

**Asset format:** `name:version:relative_path` (relative to asset root).

---

## Rulesets

| Ruleset | Use case |
|---------|----------|
| `default` | Daily USD checks, UDIM, metadata |
| `film` | Broader extensions, texture whitelist, prim naming |
| `strict` | Publish gate: stricter textures, timecode, colorSpace |

Custom rulesets are available in Studio plans — contact support.

---

## License and trial

1. **First launch** starts a 14-day trial.
2. **During trial** — full validation in GUI and CLI.
3. **After trial** — validation is blocked until you activate a paid key.
4. **GUI:** header shows days left; click **License** to activate.

Purchase: [PayPal](https://www.paypal.me/daniilsegal90) · email [assetvalidator@gmail.com](mailto:assetvalidator@gmail.com) for your key.

---

## Support

- Email: **assetvalidator@gmail.com**
- Subject for tickets: `TIKET_AV: <plan> user_<id>` (shown in the app)
- Full manual: open **Manual** in the application (includes DCC and studio integration details)
