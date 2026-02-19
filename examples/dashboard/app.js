const palette = ["#1f59d4", "#0ea7a1", "#ef8d22", "#d0473d", "#6e4fd6", "#2f8859", "#f0b429"];

const fmtTHB = new Intl.NumberFormat("th-TH", { maximumFractionDigits: 0 });
const fmtPct = new Intl.NumberFormat("th-TH", { minimumFractionDigits: 1, maximumFractionDigits: 1 });

function toBillions(value) {
  return `${fmtTHB.format((Number(value) || 0) / 1e9)}B`;
}

function normalizeTicker(ticker) {
  if (!ticker) return "";
  const t = String(ticker).trim().toUpperCase();
  return t.includes(":") ? t.split(":")[0] : t;
}

function cleanHoldingName(name, ticker) {
  if (!name) return "";
  let s = String(name).trim();
  const rawTicker = String(ticker || "").trim();

  // Remove a ticker suffix if it is glued to the end of the holding name.
  if (rawTicker && s.toUpperCase().endsWith(rawTicker.toUpperCase())) {
    s = s.slice(0, -rawTicker.length).trim();
  }

  // Remove trailing "(SYMBOL)" for cleaner display labels.
  s = s.replace(/\s*\(([A-Z0-9.\-:]{2,20})\)\s*$/g, "").trim();

  return s.replace(/\s{2,}/g, " ");
}

function renderCards(cards) {
  const el = document.getElementById("summary-cards");
  el.innerHTML = "";
  const avg1y = cards.avg_fund_return_1y == null ? "N/A" : `+${fmtPct.format(cards.avg_fund_return_1y)}%`;
  const items = [
    ["Total Holdings Value", `฿${toBillions(cards.total_holdings_value_thb)}`],
    ["Top Sector Allocation", `${cards.top_sector_name} ${fmtPct.format(cards.top_sector_weight_pct)}%`],
    ["Top Country Exposure", `${cards.top_country_name} ${fmtPct.format(cards.top_country_weight_pct)}%`],
    ["Avg Fund Return (1Y)", avg1y],
  ];

  items.forEach(([label, value]) => {
    const div = document.createElement("div");
    div.className = "card";
    div.innerHTML = `<div class="label">${label}</div><div class="value">${value}</div>`;
    el.appendChild(div);
  });
}

function renderTopHoldings(rows, mode) {
  const wrap = document.getElementById("top-holdings");
  wrap.innerHTML = "";
  const valueKey = mode === "thai" ? "total_thai_fund_value" : "global_fund_value";
  const max = Math.max(...rows.map(r => Number(r[valueKey]) || 0), 1);

  rows.forEach((row, idx) => {
    const value = Number(row[valueKey]) || 0;
    const pct = (value / max) * 100;
    const tickerRaw = mode === "thai" ? row.symbol : row.holding_ticker;
    const ticker = normalizeTicker(tickerRaw);
    const cleanName = cleanHoldingName(row.holding_name, row.holding_ticker);
    const label = mode === "thai"
      ? (cleanName || ticker || "-")
      : (cleanName || "-");
    const div = document.createElement("div");
    div.className = "holding-row";
    div.style.animationDelay = `${idx * 40}ms`;
    const pctText = `${fmtPct.format(pct)}% of #1`;
    div.innerHTML = `
      <div class="rank-chip">${idx + 1}</div>
      <div class="holding-main">
        <div class="holding-line">
          <span class="holding-name" title="${label}">${label}</span>
          ${ticker ? `<span class="ticker-badge">${ticker}</span>` : ""}
        </div>
        <div class="mini-track"><div class="mini-fill" style="width:${pct.toFixed(2)}%"></div></div>
      </div>
      <div class="holding-value">
        <div class="value-pill">฿${toBillions(value)}</div>
        <div class="value-sub">${pctText}</div>
      </div>
    `;
    wrap.appendChild(div);
  });
}

function renderDonut(targetId, legendId, rowsShown, rowsAll, labelKey, valueKey, percentBasis, displayLimit) {
  const donut = document.getElementById(targetId);
  const legend = document.getElementById(legendId);
  const shown = Number.isFinite(displayLimit) ? rowsShown.slice(0, displayLimit) : rowsShown;
  const shownTotal = shown.reduce((s, r) => s + (Number(r[valueKey]) || 0), 0);
  const allTotal = rowsAll.reduce((s, r) => s + (Number(r[valueKey]) || 0), 0);
  const denominator = (percentBasis === "all" ? allTotal : shownTotal) || 1;

  let start = 0;
  const slices = shown.map((r, i) => {
    const share = ((Number(r[valueKey]) || 0) / denominator) * 100;
    const end = start + share;
    const css = `${palette[i % palette.length]} ${start.toFixed(2)}% ${end.toFixed(2)}%`;
    start = end;
    return css;
  });
  if (start < 100) {
    slices.push(`#dfe6df ${start.toFixed(2)}% 100%`);
  }
  donut.style.background = `conic-gradient(${slices.join(",")})`;

  legend.innerHTML = "";
  shown.forEach((r, i) => {
    const li = document.createElement("li");
    const share = ((Number(r[valueKey]) || 0) / denominator) * 100;
    li.innerHTML = `<span class="swatch" style="background:${palette[i % palette.length]}"></span><span>${r[labelKey]}</span><span>${fmtPct.format(share)}%</span>`;
    legend.appendChild(li);
  });
  if (percentBasis === "all" && shownTotal < allTotal) {
    const rest = ((allTotal - shownTotal) / denominator) * 100;
    const li = document.createElement("li");
    li.innerHTML = `<span class="swatch" style="background:#dfe6df"></span><span>Others</span><span>${fmtPct.format(rest)}%</span>`;
    legend.appendChild(li);
  }
}

function fillTableBody(tbodyId, rows, leftCols) {
  const tbody = document.getElementById(tbodyId);
  tbody.innerHTML = "";
  rows.slice(0, 8).forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${leftCols(row)}</td>
      <td class="r">${fmtPct.format(row.total_true_weight_pct || 0)}</td>
      <td class="r">${fmtTHB.format(row.total_true_value_thb || 0)}</td>
    `;
    tbody.appendChild(tr);
  });
}

function wireSearch(data) {
  const byFund = data.search_by_fund;
  const byAsset = data.search_by_asset;

  const fundInput = document.getElementById("fund-input");
  const assetInput = document.getElementById("asset-input");

  function renderFund() {
    const q = fundInput.value.trim().toLowerCase();
    const rows = byFund.filter(r => !q || String(r.fund_code).toLowerCase().includes(q));
    fillTableBody("fund-results", rows, r => {
      const ticker = normalizeTicker(r.holding_ticker);
      const clean = cleanHoldingName(r.holding_name, r.holding_ticker);
      return `${clean}${ticker ? ` (${ticker})` : ""}`;
    });
  }

  function renderAsset() {
    const q = assetInput.value.trim().toLowerCase();
    const rows = byAsset.filter(r => {
      const h = String(r.holding_name || "").toLowerCase();
      const t = String(r.holding_ticker || "").toLowerCase();
      return !q || h.includes(q) || t.includes(q);
    });
    fillTableBody("asset-results", rows, r => `${r.fund_code}`);
  }

  fundInput.addEventListener("input", renderFund);
  assetInput.addEventListener("input", renderAsset);
  renderFund();
  renderAsset();
}

async function main() {
  const res = await fetch("data/dashboard_data.json");
  const data = await res.json();

  let activeScope = "thai";
  let sectorMode = "all";
  let countryMode = "all";
  let percentBasis = "shown";

  function topRows(scope) {
    return scope === "thai" ? data.top_thai_holdings_top10 : data.top_global_traceability_top10;
  }

  function summary(scope) {
    return scope === "thai" ? data.dashboard_summary_thai : data.dashboard_summary_global;
  }

  function sectorRows(scope, mode) {
    const rows = scope === "thai" ? data.sector_allocation_thai : data.sector_allocation_global;
    return mode === "top10" ? rows.slice(0, 10) : rows;
  }

  function countryRows(scope, mode) {
    const rows = scope === "thai" ? data.country_allocation_thai : data.country_allocation_global;
    return mode === "top10" ? rows.slice(0, 10) : rows;
  }

  function syncMiniTabs() {
    document.getElementById("sector-all").classList.toggle("active", sectorMode === "all");
    document.getElementById("sector-top10").classList.toggle("active", sectorMode === "top10");
    document.getElementById("country-all").classList.toggle("active", countryMode === "all");
    document.getElementById("country-top10").classList.toggle("active", countryMode === "top10");
    document.getElementById("pct-shown").classList.toggle("active", percentBasis === "shown");
    document.getElementById("pct-all").classList.toggle("active", percentBasis === "all");
  }

  function renderAll() {
    const isThai = activeScope === "thai";
    document.getElementById("tab-thai").classList.toggle("active", isThai);
    document.getElementById("tab-global").classList.toggle("active", !isThai);
    document.getElementById("top-title").textContent = isThai ? "Top 10 หุ้นในไทย (มุมมอง บลจ.ไทย)" : "Top 10 หุ้นโลก (แกะรอยไส้ใน)";
    document.getElementById("top-subtitle").textContent = isThai
      ? "มุมมองความนิยมในพอร์ตของ บลจ. ไทย (total_thai_fund_value)"
      : "แกะรอย Feeder → Master → Real Holdings (global_fund_value)";
    renderCards(summary(activeScope));
    renderTopHoldings(topRows(activeScope), activeScope);
    const sectorLimit = sectorMode === "top10" ? 10 : undefined;
    const countryLimit = countryMode === "top10" ? 10 : undefined;
    renderDonut(
      "sector-donut",
      "sector-legend",
      sectorRows(activeScope, sectorMode),
      sectorRows(activeScope, "all"),
      "sector_name",
      "total_value_thb",
      percentBasis,
      sectorLimit
    );
    renderDonut(
      "country-donut",
      "country-legend",
      countryRows(activeScope, countryMode),
      countryRows(activeScope, "all"),
      "country_name",
      "total_value_thb",
      percentBasis,
      countryLimit
    );
    syncMiniTabs();
  }

  document.getElementById("tab-thai").addEventListener("click", () => {
    activeScope = "thai";
    renderAll();
  });
  document.getElementById("tab-global").addEventListener("click", () => {
    activeScope = "global";
    renderAll();
  });
  document.getElementById("sector-all").addEventListener("click", () => {
    sectorMode = "all";
    renderAll();
  });
  document.getElementById("sector-top10").addEventListener("click", () => {
    sectorMode = "top10";
    renderAll();
  });
  document.getElementById("country-all").addEventListener("click", () => {
    countryMode = "all";
    renderAll();
  });
  document.getElementById("country-top10").addEventListener("click", () => {
    countryMode = "top10";
    renderAll();
  });
  document.getElementById("pct-shown").addEventListener("click", () => {
    percentBasis = "shown";
    renderAll();
  });
  document.getElementById("pct-all").addEventListener("click", () => {
    percentBasis = "all";
    renderAll();
  });

  renderAll();
  wireSearch(data);
}

main().catch((err) => {
  console.error(err);
  alert("โหลดข้อมูลไม่สำเร็จ กรุณารัน export script ก่อน");
});
