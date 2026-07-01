# AssetValidator — User Manual

**Version 2.8.1** · [Product site](https://daniilcg.github.io/AssetValidator/)

AssetValidator is **commercial software** with a **14-day free trial**. After trial, a paid license is required.

**What it does:** validates VFX/CG publishes with **300+ built-in checks** — paths, naming, USD closure, textures, media headers, delivery hygiene, and more. Use ruleset **`general`** for mixed formats (USD, ABC, FBX, EXR, …). Use **`studio`** for the full built-in check pack. USD-specific checks run only on USD files.

---

## Quick start (Windows)

1. Download **AssetValidator-Setup.exe** from the [product website](https://daniilcg.github.io/AssetValidator/#download)
2. Install and launch **AssetValidator**
3. Set **Asset root** and **Cache root**
4. Add assets or use **Scan folder**
5. Choose ruleset **`general`** (all formats), **`default`** (USD-focused), or **`studio`** (full 300+ check pack)
6. Click **Validate**

---

## GUI overview

| Area | Purpose |
|------|---------|
| **Paths** | Asset root, cache, threads, timeout, ruleset |
| **Options** | Watch folder, fail on warnings, notifications, DCC report import |
| **Custom plugins** | Studio Python checks (Business / Studio license) |
| **Manual** | In-app user manual (header button) |
| **AI Assistant** | Help and support |
| **License** | Trial status and key activation |

---

## Plans (after trial)

| Plan | Users | Includes |
|------|-------|----------|
| Personal | 1 | Built-in checks, GUI/CLI |
| Team | up to 5 | + CI, shared reports |
| Business | up to 25 | + **custom check plugins** |
| Studio | unlimited | + SLA, onboarding |

---

## CLI basics

```bash
validate-assets --asset-root /assets --ruleset general hero:v001:hero/v001/hero_v001.usd
validate-assets --list-rulesets
validate-assets --license-status
```

**Asset format:** `name:version:relative_path` (relative to asset root).

---

## Rulesets

| Ruleset | Use case |
|---------|----------|
| `general` | All common formats — paths on every file; USD checks on USD only |
| `default` | Daily USD checks, UDIM, metadata |
| `studio` | Full built-in library — 300+ checks (paths, naming, USD, media, delivery) |
| `film` | Broader extensions, texture whitelist |
| `strict` | Strict publish gate |

---

## Custom plugins (Business+)

Drop `.py` files in the plugins folder (GUI → **Custom plugins**). Each file exports `CHECKS = [MyCheck(), …]`. Full authoring guide is in the in-app **Manual**.

---

## License and trial

1. **First launch** starts a 14-day trial (1 user, full product).
2. **After trial** — validation blocked until you activate a paid key.
3. **GUI:** click **License** to activate.

Purchase: [PayPal @segalcommic](https://www.paypal.me/segalcommic) · email [assetvalidator@gmail.com](mailto:assetvalidator@gmail.com) for your key.

---

## Support

- Email: **assetvalidator@gmail.com**
- Subject: `TIKET_AV: <plan> user_<id>` (shown in the app)
- Full manual: **Manual** button in the application
