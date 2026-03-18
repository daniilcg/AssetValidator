(() => {
  const buttons = document.querySelectorAll(".toggle-btn");
  const cards = document.querySelectorAll(".price-card[data-monthly]");

  function setBilling(mode) {
    document.body.classList.toggle("is-yearly", mode === "yearly");
    buttons.forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.billing === mode);
    });

    cards.forEach((card) => {
      const priceEl = card.querySelector(".price");
      if (!priceEl) {
        return;
      }

      const monthly = card.dataset.monthly ?? "";
      const yearly = card.dataset.yearly ?? "";
      priceEl.textContent = mode === "yearly" ? yearly : monthly;

      const monthlyLinks = card.querySelector(".monthly-links");
      const yearlyLinks = card.querySelector(".yearly-links");
      if (monthlyLinks && yearlyLinks) {
        monthlyLinks.classList.toggle("hidden", mode === "yearly");
        yearlyLinks.classList.toggle("hidden", mode !== "yearly");
      }
    });
  }

  buttons.forEach((btn) => {
    btn.addEventListener("click", () => setBilling(btn.dataset.billing || "monthly"));
  });

  setBilling("monthly");
})();

(() => {
  const input = document.getElementById("demoInput");
  const out = document.getElementById("demoOutput");
  const summary = document.getElementById("demoSummary");
  const btnValidate = document.getElementById("demoValidate");
  const btnClear = document.getElementById("demoClear");
  const btnSample = document.getElementById("demoSample");

  if (!input || !out || !summary || !btnValidate || !btnClear || !btnSample) {
    return;
  }

  const sample = `dragon:v042:characters/dragon/v042/dragon_v042.usd
tree:v017:environments/tree/v017/tree_v017.usd
camera:v001:shots/shot010/camera/cam_v001.usd
badasset::missing/version.usd
dragon:v042:characters/dragon/v042/dragon_v042.usd`;

  function line(text, cls = "demo-muted") {
    const div = document.createElement("div");
    div.className = `demo-line ${cls}`;
    div.textContent = text;
    out.appendChild(div);
  }

  function clearOutput() {
    out.textContent = "";
  }

  function validate() {
    clearOutput();
    const raw = input.value || "";
    const lines = raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);

    const seen = new Map();
    let passed = 0;
    let failed = 0;

    line(`Starting validation: ${lines.length} assets…`, "demo-warn");

    for (const l of lines) {
      const parts = l.split(":");
      const reasons = [];

      if (parts.length < 3) {
        reasons.push("Invalid format (expected name:version:path)");
      }

      const [name, version, rel] = [parts[0] ?? "", parts[1] ?? "", parts.slice(2).join(":") ?? ""];

      if (!name) reasons.push("Missing asset name");
      if (!version) reasons.push("Missing version");
      if (!rel) reasons.push("Missing relative path");

      if (rel && (rel.startsWith("/") || rel.startsWith("\\") || /^[a-zA-Z]:\\/.test(rel))) {
        reasons.push("Path looks absolute (use relative path)");
      }
      if (rel && rel.includes("..")) {
        reasons.push("Path contains '..' (avoid parent traversal)");
      }
      if (rel && rel.length > 140) {
        reasons.push("Path is very long (may break on Windows)");
      }
      if (rel && !/\.(usd|usda|usdc|abc|fbx|obj|vdb|exr|png|jpg|jpeg)$/i.test(rel)) {
        reasons.push("Unknown file extension (demo check)");
      }

      const key = `${name}:${version}:${rel}`;
      if (seen.has(key)) {
        reasons.push("Duplicate entry");
      } else {
        seen.set(key, true);
      }

      if (reasons.length === 0) {
        passed += 1;
        line(`PASS ${name} ${version}`, "demo-pass");
      } else {
        failed += 1;
        line(`FAIL ${name || "(unknown)"} ${version || "(no version)"}`, "demo-fail");
        for (const r of reasons) {
          line(`  - ${r}`, "demo-fail");
        }
      }
    }

    summary.textContent = `Done. total=${lines.length}, passed=${passed}, failed=${failed}`;
    line(summary.textContent, "demo-warn");
  }

  btnValidate.addEventListener("click", validate);
  btnClear.addEventListener("click", () => {
    input.value = "";
    summary.textContent = "Ready.";
    clearOutput();
    line("Ready.", "demo-muted");
  });
  btnSample.addEventListener("click", () => {
    input.value = sample;
    summary.textContent = "Sample loaded. Click Validate.";
    clearOutput();
    line("Sample loaded. Click Validate.", "demo-muted");
  });

  clearOutput();
  line("Ready. Paste assets and click Validate.", "demo-muted");
})();
