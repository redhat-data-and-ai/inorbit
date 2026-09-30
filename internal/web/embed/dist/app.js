const API = "";
const REFRESH_MS = 15000;

const state = {
  meta: null,
  snaps: [],
  trends: {},
  filter: "",
  tableFilter: "",
  sourceFilter: "all",
  healthFilter: "all",
  typeFilter: "all",
  listSort: "name",
  listView: "grid",
  envFilter: "production",
  productEnv: "production",
  highImpact: false,
  noteDismissed: false,
  runFilter: "all",
  freshnessFilter: "all",
  qualityStatusFilter: "all",
  lineageKind: "all",
  relWindow: "7d",
  trendWindow: "30d",
  tableSort: "",
  tableSortDir: "asc",
  expanded: {},
  loaded: false,
};

let lastRouteKey = "";
let lastProductKey = "";
let lastRenderedRoute = "";
let dataSigCache = "";
let filterTimer = 0;

function parseRoute() {
  const raw = (location.hash || "#/").replace(/^#/, "") || "/";
  const u = new URL(raw, "http://ui.local");
  const parts = u.pathname.replace(/\/+$/, "").split("/").filter(Boolean);
  if (parts[0] === "data-product" && parts[1]) {
    let tab = u.searchParams.get("tab") || "overview";
    if (tab === "freshness" || tab === "sla") tab = "pipeline";
    return { view: "detail", id: decodeURIComponent(parts[1]), tab };
  }
  return { view: "list", id: "", tab: "" };
}

function go(hash) {
  location.hash = hash;
}

async function getJSON(path) {
  const res = await fetch(API + path);
  if (!res.ok) throw new Error(path + " " + res.status);
  return res.json();
}

function fmtScore(n) {
  if (n == null || Number.isNaN(n)) return "—";
  return Number(n).toFixed(n % 1 === 0 ? 0 : 1);
}

function parseDate(v) {
  if (!v || String(v).startsWith("0001-01-01")) return null;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function localZone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "local";
  } catch {
    return "local";
  }
}

function fmtTime(v) {
  const d = parseDate(v);
  if (!d) return "—";
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short",
  }).format(d);
}

function fmtTimeShort(v) {
  const d = parseDate(v);
  if (!d) return "—";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(d);
}

function fmtRelative(v) {
  const d = parseDate(v);
  if (!d) return "";
  const sec = Math.round((Date.now() - d.getTime()) / 1000);
  const abs = Math.abs(sec);
  const future = sec < 0;
  let label = "just now";
  if (abs >= 60 && abs < 3600) label = Math.floor(abs / 60) + "m";
  else if (abs >= 3600 && abs < 86400) label = Math.floor(abs / 3600) + "h";
  else if (abs >= 86400 && abs < 86400 * 14) label = Math.floor(abs / 86400) + "d";
  else if (abs >= 86400 * 14) return fmtTimeShort(v);
  if (abs < 60) return future ? "soon" : "just now";
  return future ? "in " + label : label + " ago";
}

function fmtWhen(v) {
  const d = parseDate(v);
  if (!d) return "—";
  const abs = fmtTime(v);
  return `<time datetime="${esc(d.toISOString())}" title="${esc(abs)}">${esc(fmtRelative(v))}</time>`;
}

function fmtBytes(n) {
  const v = Number(n) || 0;
  if (v < 1024) return v + " B";
  if (v < 1024 * 1024) return (v / 1024).toFixed(1) + " KiB";
  if (v < 1024 * 1024 * 1024) return (v / (1024 * 1024)).toFixed(1) + " MiB";
  return (v / (1024 * 1024 * 1024)).toFixed(2) + " GiB";
}

function healthClass(status) {
  const s = String(status || "").toUpperCase();
  if (s === "TRUSTED" || s === "SUCCESS" || s === "PASSED" || s === "GREEN" || s === "OK") return "ok";
  if (s === "RUNNING") return "info";
  if (s === "CAUTION" || s === "YELLOW" || s === "WARNING" || s === "DELAYED") return "warn";
  if (s === "AT_RISK" || s === "FAILED" || s === "RED" || s === "BREACH" || s === "PAUSED" || s === "STALE") return "bad";
  return "muted";
}

function pretty(status) {
  if (!status) return "—";
  const raw = String(status);
  const u = raw.toUpperCase();
  if (u === "OK") return "OK";
  if (u === "GREEN") return "Trusted";
  if (u === "YELLOW") return "Caution";
  if (u === "RED") return "At risk";
  return raw.replace(/_/g, " ").toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
}

function pipelineSummary(rows) {
  const list = rows || [];
  if (!list.length) return { label: "No DAGs", cls: "muted", detail: "0 DAGs" };
  const failed = list.filter((d) => String(d.dag_status).toUpperCase() === "FAILED" || d.dag_overall_status === "FAILED").length;
  const running = list.filter((d) => String(d.dag_status).toUpperCase() === "RUNNING").length;
  const paused = list.filter((d) => d.dag_is_paused).length;
  const n = list.length + (list.length === 1 ? " DAG" : " DAGs");
  if (failed) return { label: "Failed", cls: "bad", detail: failed + " failed · " + n };
  if (running) return { label: "Running", cls: "warn", detail: running + " running · " + n };
  if (paused === list.length) return { label: "Paused", cls: "bad", detail: n };
  const worst = list.find((d) => d.dag_overall_status === "AT_RISK") ? "At risk" : "Trusted";
  return { label: worst, cls: worst === "Trusted" ? "ok" : "bad", detail: n };
}

function qualitySummary(rows, sources) {
  const list = (rows || []).filter((c) => c.source_type === "VALIDATION" || c.source_type === "DBT_TEST");
  const failed = list.filter((c) => c.status === "FAILED").length;
  const warn = list.filter((c) => c.status === "WARNING").length;
  const vxStatus = sources && sources.validation ? sources.validation.status : "";
  if (!list.length) {
    if (vxStatus === "missing") return { label: "No checks", cls: "muted", detail: "Validation checks not exist" };
    if (vxStatus === "ok") return { label: "No checks", cls: "muted", detail: "No rows in latest run" };
    return { label: "No checks", cls: "muted", detail: "Not configured" };
  }
  if (failed) return { label: failed + " failed", cls: "bad", detail: list.length + " checks" };
  if (warn) return { label: warn + " warning", cls: "warn", detail: list.length + " checks" };
  return { label: "Passing", cls: "ok", detail: list.length + " checks" };
}

function lineageSummary(lin) {
  lin = lin || {};
  const up = lin.upstream_count != null ? Number(lin.upstream_count) : (lin.upstream_sources || []).length;
  const down = lin.direct_downstream_count != null ? Number(lin.direct_downstream_count) : (lin.downstream_consumers || []).length;
  const impact = lin.blast_radius_score || (lin.blast_radius_count ? lin.blast_radius_count + " blast radius" : "Warehouse mart");
  return { label: up + " up · " + down + " down", detail: impact, up, down };
}

function labelMod(cls) {
  return { ok: "pf-m-green", warn: "pf-m-orange", bad: "pf-m-red", info: "pf-m-blue", muted: "pf-m-grey" }[cls] || "pf-m-grey";
}

function label(status) {
  const cls = healthClass(status);
  return `<span class="pf-v5-c-label pf-m-outline ${labelMod(cls)} label ${cls}"><span class="pf-v5-c-label__content"><span class="pf-v5-c-label__text">${esc(pretty(status))}</span></span></span>`;
}

function labelAs(status, text) {
  const cls = healthClass(status);
  return `<span class="pf-v5-c-label pf-m-outline ${labelMod(cls)} label ${cls}"><span class="pf-v5-c-label__content"><span class="pf-v5-c-label__text">${esc(text)}</span></span></span>`;
}

function astroLink(d, compact) {
  const href = d.astro_url;
  if (!href) return compact ? "" : "—";
  const a = `<a class="astro-open" href="${esc(href)}" target="_blank" rel="noopener noreferrer" title="Open ${esc(d.dag_id)} in Astro">Open in Astro</a>`;
  if (compact) return a;
  return `${a}<div class="url-path">${esc(href)}</div>`;
}

function dagNameCell(d) {
  const name = esc(d.dag_id);
  if (!d.astro_url) return name;
  return `<a class="dag-link" href="${esc(d.astro_url)}" target="_blank" rel="noopener noreferrer" title="Open ${name} in Astro">${name}</a>`;
}

function fmtSpan(v, empty) {
  if (v == null || v === "" || Number.isNaN(Number(v))) return empty || "—";
  const mins = Math.max(0, Math.round(Number(v)));
  const days = Math.floor(mins / 1440);
  const hours = Math.floor((mins % 1440) / 60);
  const m = mins % 60;
  const parts = [];
  if (days) parts.push(days + (days === 1 ? " day" : " days"));
  if (hours) parts.push(hours + (hours === 1 ? " hour" : " hours"));
  if (m || !parts.length) parts.push(m + " min");
  return parts.join(" ");
}

function fmtMins(v) {
  return fmtSpan(v, "—");
}

function fmtPct(v) {
  if (v == null || v === "") return "—";
  const n = Number(v);
  if (Number.isNaN(n)) return "—";
  return (Math.abs(n % 1) < 0.001 ? n.toFixed(0) : n.toFixed(2)) + "%";
}

function fmtDuration(d) {
  let sec = Number(d.dag_duration_seconds) || 0;
  if (String(d.dag_status || "").toUpperCase() === "RUNNING" && parseDate(d.dag_started_at)) {
    sec = Math.max(0, (Date.now() - parseDate(d.dag_started_at).getTime()) / 1000);
  }
  if (!sec) return "—";
  const total = Math.round(sec);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h) return h + "h " + m + "m " + String(s).padStart(2, "0") + "s";
  return m + "m " + String(s).padStart(2, "0") + "s";
}

function fmtRunClock(v) {
  const d = parseDate(v);
  if (!d) return "—";
  const time = new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit", timeZoneName: "short" }).format(d);
  const day = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: "numeric" }).format(d);
  return `<div class="run-clock" title="${esc(fmtTime(v))}"><div class="run-time">${esc(time)}</div><div class="run-date">${esc(day)}</div></div>`;
}

function overallPretty(status) {
  const s = String(status || "").toUpperCase();
  if (s === "TRUSTED") return "Healthy";
  return pretty(status);
}

function dagKey(d) {
  return (d.astro_deployment_name || "") + "|" + (d.dag_id || "");
}

function lastRunAt(d) {
  return d.dag_completed_at || d.dag_started_at || d.last_successful_at;
}

function reliabilityCell(d) {
  const rows = [
    ["7d", d.dag_reliability_7d, d.dag_reliability_status_7d, d.dag_runs_7d],
    ["30d", d.dag_reliability_30d, d.dag_reliability_status_30d, d.dag_runs_30d],
    ["90d", d.dag_reliability_90d, d.dag_reliability_status_90d, d.dag_runs_90d],
  ];
  return `<div class="rel-stack">${rows.map(([w, p, st, n]) =>
    `<div class="rel-row ${healthClass(st)}" title="${esc((n || 0) + " runs in window")}"><span>${w}</span><strong>${esc(fmtPct(p))}</strong></div>`
  ).join("")}</div>`;
}

function pipelineRows(d) {
  const key = dagKey(d);
  const open = !!state.expanded[key];
  const note = d.dag_overall_status_description || "";
  const custom = !!d.dag_is_custom;
  const badges = [
    d.is_primary_dag ? `<span class="flag">Primary</span>` : "",
    d.dag_is_paused ? `<span class="flag muted">Paused</span>` : "",
    custom ? `<span class="flag custom">Custom</span>` : "",
  ].filter(Boolean).join("");
  const freq = d.dag_frequency_display || (custom ? "Custom" : "Unscheduled");
  const slaVal = custom && (d.dag_sla_minutes == null) ? "Not scored" : fmtSpan(d.dag_sla_minutes, "—");
  const ageVal = d.dag_data_age_mins == null ? (lastRunAt(d) ? fmtSpan(ageMinsFrom(d), "No runs") : "No runs") : fmtSpan(d.dag_data_age_mins, "No runs");
  const statusLabel = d.dag_is_paused ? labelAs("PAUSED", "Paused") : label(d.dag_status);
  const main = `<tr class="pipe-row ${open ? "is-open" : ""} ${custom ? "is-custom" : ""}">
    <td class="col-expand">
      <button type="button" class="expand" data-expand="${esc(key)}" aria-expanded="${open}" aria-label="${open ? "Collapse" : "Expand"} ${esc(d.dag_id)}">
        <span aria-hidden="true">${open ? "▾" : "▸"}</span>
      </button>
    </td>
    <td class="pipe-name">
      <div class="pipe-id">${esc(d.dag_id)}</div>
      <div class="pipe-meta">${esc(d.astro_deployment_name || "Astro")} · ${esc(d.pipeline_type || "DAG")}${badges ? " " + badges : ""}</div>
    </td>
    <td class="pipe-status">
      ${statusLabel}
      <div class="pipe-meta">${esc(d.trigger_type || (custom ? "Custom" : "—"))}</div>
    </td>
    <td>${custom ? labelAs("CUSTOM", "Not scored") : label(d.dag_pipeline_sla_status)}</td>
    <td>${fmtRunClock(lastRunAt(d))}</td>
    <td>${d.dag_next_expected_at ? fmtRunClock(d.dag_next_expected_at) : `<span class="muted-cell">${esc(custom ? "Custom" : "No next run")}</span>`}</td>
    <td class="pipe-rel">${reliabilityCell(d)}${custom ? `<div class="pipe-meta">Not scored</div>` : ""}</td>
    <td class="col-astro">${d.astro_url ? astroLink(d, true) : `<span class="muted-cell">—</span>`}</td>
  </tr>`;
  if (!open) return main;
  return main + `<tr class="pipe-detail"><td colspan="8">
    <div class="detail-grid">
      <div><span>Astro</span>${astroLink(d)}</div>
      <div><span>Run id</span>${esc(d.external_run_id || "—")}</div>
      <div><span>Last success</span>${fmtWhen(d.last_successful_at)}</div>
      <div><span>Started</span>${fmtWhen(d.dag_started_at)}</div>
      <div><span>Status</span>${label(d.dag_overall_status)}</div>
      <div><span>SLA</span>${label(d.dag_pipeline_sla_status)}</div>
      <div><span>Interval</span>${esc(freq)}</div>
      <div><span>Age</span>${esc(ageVal)}</div>
      <div><span>Duration</span>${esc(fmtDuration(d))}</div>
      <div><span>SLA window</span>${esc(slaVal)}</div>
      ${custom ? `<div><span>Custom</span>Excluded from health score and product SLA age</div>` : ""}
      ${note ? `<div class="span2"><span>Note</span>${esc(note)}</div>` : ""}
      ${d.error_message ? `<div class="span2"><span>Error</span>${esc(d.error_message)}</div>` : ""}
    </div>
  </td></tr>`;
}

function ageMinsFrom(d) {
  const t = parseDate(lastRunAt(d));
  if (!t) return null;
  return (Date.now() - t.getTime()) / 60000;
}

function sortDAGs(list) {
  return [...(list || [])].sort((a, b) => Number(b.is_primary_dag) - Number(a.is_primary_dag) || String(a.dag_id).localeCompare(String(b.dag_id)));
}

function esc(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function donut(pct, title, sub, cls, inner) {
  const r = 36;
  const circ = 2 * Math.PI * r;
  const p = Math.max(0, Math.min(1, Number(pct) || 0));
  const dash = (p * circ).toFixed(2);
  return `<div class="donut ${cls || ""}">
    <div class="donut-ring">
      <svg viewBox="0 0 100 100" aria-hidden="true">
        <circle class="track" cx="50" cy="50" r="${r}"></circle>
        <circle class="arc" cx="50" cy="50" r="${r}" stroke-dasharray="${dash} ${circ.toFixed(2)}" transform="rotate(-90 50 50)"></circle>
      </svg>
      <div class="donut-center">${inner || `<strong>${esc(title)}</strong>`}</div>
    </div>
    <div class="donut-caption">${esc(sub)}</div>
  </div>`;
}

function polar(cx, cy, r, deg) {
  const rad = ((deg - 90) * Math.PI) / 180;
  return [cx + r * Math.cos(rad), cy + r * Math.sin(rad)];
}

function arcPath(cx, cy, r, start, end) {
  if (end - start >= 359.99) {
    return `<circle class="seg" cx="${cx}" cy="${cy}" r="${r}"></circle>`;
  }
  const [x1, y1] = polar(cx, cy, r, start);
  const [x2, y2] = polar(cx, cy, r, end);
  const large = end - start > 180 ? 1 : 0;
  return `<path class="seg" d="M ${x1.toFixed(2)} ${y1.toFixed(2)} A ${r} ${r} 0 ${large} 1 ${x2.toFixed(2)} ${y2.toFixed(2)}"></path>`;
}

function segmentDonut(segments, center, caption) {
  const parts = (segments || []).filter((s) => Number(s.value) > 0);
  const total = parts.reduce((n, s) => n + Number(s.value) || 0, 0);
  let angle = 0;
  const arcs = total
    ? parts.map((s) => {
        const sweep = (Number(s.value) / total) * 360;
        const start = angle;
        const end = angle + sweep;
        angle = end;
        return `<g class="${esc(s.cls || "muted")}">${arcPath(50, 50, 36, start, end)}</g>`;
      }).join("")
    : `<g class="muted">${arcPath(50, 50, 36, 0, 359.99)}</g>`;
  const legend = (segments || []).map((s) =>
    `<li class="${esc(s.cls || "muted")}"><span class="swatch"></span>${esc(s.label)} <strong>${esc(String(s.value))}</strong></li>`
  ).join("");
  return `<div class="seg-donut">
    <div class="donut-ring">
      <svg viewBox="0 0 100 100" aria-hidden="true">
        <circle class="track" cx="50" cy="50" r="36"></circle>
        ${arcs}
      </svg>
      <div class="donut-center"><strong>${esc(center)}</strong></div>
    </div>
    <div class="donut-caption">${esc(caption)}</div>
    <ul class="seg-legend">${legend}</ul>
  </div>`;
}

function dimBars(h) {
  const dims = [
    ["Freshness", h.freshness_score],
    ["Accuracy", h.accuracy_score],
    ["Consistency", h.consistency_score],
    ["Completeness", h.completeness_score],
    ["Validity", h.validity_score],
    ["Uniqueness", h.uniqueness_score],
  ];
  return `<div class="pf-v5-c-card chart-card io-dim-card">
    <div class="pf-v5-c-card__title"><h3 class="pf-v5-c-title pf-m-md">Dimension Health Breakdown</h3></div>
    <div class="pf-v5-c-card__body">
    <div class="dim-bars">${dims.map(([name, score]) => {
      if (score == null) {
        return `<div class="dim-bar muted"><span class="dim-name">${esc(name)}</span><div class="dim-track"></div><span class="dim-val">—</span></div>`;
      }
      const cls = score >= 80 ? "ok" : score >= 60 ? "warn" : "bad";
      const pct = Math.max(0, Math.min(100, Number(score) || 0));
      return `<div class="dim-bar ${cls}">
        <span class="dim-name">${esc(name)}</span>
        <div class="dim-track" aria-hidden="true"><div class="dim-fill" style="width:${pct}%"></div></div>
        <span class="dim-val">${esc(fmtScore(score))}</span>
      </div>`;
    }).join("")}</div>
    </div>
  </div>`;
}

function trendWindowMs(w) {
  if (w === "7d") return 7 * 86400000;
  if (w === "30d") return 30 * 86400000;
  if (w === "90d") return 90 * 86400000;
  return null;
}

function filterTrend(points, w) {
  const pts = points || [];
  const ms = trendWindowMs(w);
  if (!ms) return pts;
  const cutoff = Date.now() - ms;
  const inWin = pts.filter((p) => {
    const t = Date.parse(p.t);
    return !Number.isNaN(t) && t >= cutoff;
  });
  return inWin.length ? inWin : pts.slice(-1);
}

function trendDirection(pts) {
  if (!pts || pts.length < 2) return { text: "stable", cls: "muted" };
  const first = Number(pts[0].health_score) || 0;
  const last = Number(pts[pts.length - 1].health_score) || 0;
  const d = last - first;
  if (d <= -2) return { text: "degrading", cls: "bad" };
  if (d >= 2) return { text: "improving", cls: "ok" };
  return { text: "stable", cls: "muted" };
}

function healthChart(points) {
  const win = state.trendWindow || "30d";
  const pts = filterTrend(points, win);
  const windows = [["7d", "7d"], ["30d", "30d"], ["90d", "3m"], ["all", "All"]].map(([key, label]) =>
    `<button type="button" class="io-rel-btn${win === key ? " active" : ""}" data-trend-window="${key}">${label}</button>`
  ).join("");
  const dir = trendDirection(pts);
  const dirMark = dir.cls === "bad"
    ? `<svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path fill="currentColor" d="M6 9.2L1.8 3.4h8.4L6 9.2z"/></svg>`
    : dir.cls === "ok"
      ? `<svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path fill="currentColor" d="M6 2.8l4.2 5.8H1.8L6 2.8z"/></svg>`
      : "";
  const head = `<div class="io-trend-head">
      <h3 class="io-trend-title">Score Trend By Quality Dimension <span class="io-trend-dir ${dir.cls}">${dirMark}${esc(dir.text)}</span></h3>
      <div class="io-rel-toggle" role="group" aria-label="Score trend window">${windows}</div>
    </div>`;
  if (!pts.length) {
    return `<div class="io-score-card io-trend-card">${head}<p class="empty">Trend fills as the SLA clock samples the live score.</p></div>`;
  }
  const w = 720, h = 200, padL = 36, padR = 12, padT = 16, padB = 32;
  const ys = pts.map((p) => Number(p.health_score) || 0);
  const xs = pts.map((p, i) => {
    const t = Date.parse(p.t);
    return Number.isNaN(t) ? i : t;
  });
  const minX = Math.min(...xs), maxX = Math.max(...xs);
  const spanX = maxX === minX ? 1 : maxX - minX;
  const yAt = (score) => padT + (1 - score / 100) * (h - padT - padB);
  const xy = pts.map((_, i) => {
    const x = padL + ((xs[i] - minX) / spanX) * (w - padL - padR);
    const y = yAt(ys[i]);
    return [x, y];
  });
  const linePts = xy.length === 1
    ? [[padL, xy[0][1]], [w - padR, xy[0][1]]]
    : xy;
  const line = linePts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const lo = Math.min(...ys);
  const hi = Math.max(...ys);
  const leftLabel = win === "all" ? "Start" : (win === "90d" ? "3m ago" : win + " ago");
  return `<div class="io-score-card io-trend-card">
    ${head}
    <svg class="spark" viewBox="0 0 ${w} ${h}" role="img" aria-label="Health score over time">
      <line class="guide" x1="${padL}" x2="${w - padR}" y1="${yAt(100)}" y2="${yAt(100)}"></line>
      <line class="guide" x1="${padL}" x2="${w - padR}" y1="${yAt(50)}" y2="${yAt(50)}"></line>
      <line class="guide" x1="${padL}" x2="${w - padR}" y1="${yAt(0)}" y2="${yAt(0)}"></line>
      <text class="axis-label" x="${padL - 6}" y="${yAt(100) + 3}">100</text>
      <text class="axis-label" x="${padL - 6}" y="${yAt(0) + 3}">0</text>
      <path class="line" d="${line}"></path>
      <circle class="dot" cx="${linePts[linePts.length - 1][0].toFixed(1)}" cy="${linePts[linePts.length - 1][1].toFixed(1)}" r="3.5"></circle>
      <text class="axis-label x" x="${padL}" y="${h - 8}">${esc(leftLabel)}</text>
      <text class="axis-label x end" x="${w - padR}" y="${h - 8}">Today</text>
    </svg>
    <div class="io-trend-range">
      <span class="bad">Lowest ${esc(fmtScore(lo))}%</span>
      <span class="ok">Highest ${esc(fmtScore(hi))}%</span>
    </div>
  </div>`;
}

function ruleCounts(snap) {
  const h = snap.health || {};
  const dq = dqChecks(snap.quality);
  const fromQuality = dq.length > 0;
  const rows = fromQuality ? dq : [];
  const total = fromQuality ? rows.length : Number(h.total_checks) || 0;
  const failing = fromQuality ? rows.filter((c) => c.status === "FAILED").length : Number(h.failed_checks) || 0;
  const warnings = fromQuality ? rows.filter((c) => c.status === "WARNING").length : Number(h.warning_checks) || 0;
  const passing = Math.max(0, total - failing - warnings);
  return { total, failing, passing, warnings };
}

function scoreboardHTML(snap, trend) {
  const h = snap.health || {};
  const cls = healthClass(h.status);
  const has = !!h.total_checks;
  const pct = has ? Math.max(0, Math.min(100, Number(h.health_score) || 0)) : 0;
  const title = has ? Math.round(pct) + "%" : "—";
  const inner = `<strong>${esc(title)}</strong><span>Health Score</span>`;
  const rules = ruleCounts(snap);
  const href = snap.data_product && snap.data_product.data_product_id
    ? `#/data-product/${encodeURIComponent(snap.data_product.data_product_id)}?tab=quality`
    : "";
  return `<div class="io-scoreboard">
    <div class="io-score-card io-score-donut">
      ${donut(has ? pct / 100 : 0, title, "Composite Score across all dimensions", cls, inner)}
    </div>
    ${healthChart(trend)}
    <a class="io-score-card io-rules-card"${href ? ` href="${href}"` : ""}>
      <div class="io-rules">
        <div class="io-rule"><span class="io-rule-k">Total rules</span><strong>${rules.total}</strong></div>
        <div class="io-rule bad"><span class="io-rule-k">Failing</span><strong>${rules.failing}</strong></div>
        <div class="io-rule ok"><span class="io-rule-k">Passing</span><strong>${rules.passing}</strong></div>
        <div class="io-rule warn"><span class="io-rule-k">Warnings</span><strong>${rules.warnings}</strong></div>
      </div>
    </a>
  </div>`;
}

function overviewCharts(snap, trend) {
  const h = snap.health || {};
  let pts = trend || [];
  if (!pts.length && h.total_checks) {
    pts = [{ t: h.evaluated_at || snap.updated_at, health_score: h.health_score, status: h.status }];
  }
  return `${scoreboardHTML(snap, pts)}${dimBars(h)}`;
}

function dqChecks(rows) {
  return (rows || []).filter((c) => c.source_type === "VALIDATION" || c.source_type === "DBT_TEST");
}

function rowMatch(q, parts) {
  if (!q) return true;
  return parts.some((p) => String(p || "").toLowerCase().includes(q));
}

function tableToolbar(count, total, placeholder, extra) {
  return `<div class="table-toolbar">
    <div class="toolbar-filters">${extra || ""}</div>
    <div class="toolbar-search">
      <input class="pf-v5-c-form-control io-search search" id="table-filter" type="search" aria-label="${esc(placeholder)}" placeholder="${esc(placeholder)}" value="${esc(state.tableFilter)}" />
      <span class="sub shown-count">${count} of ${total} shown</span>
    </div>
  </div>`;
}

function typeKind(t) {
  const v = String(t || "").toLowerCase();
  if (v === "aggregate") return { text: "Aggregate", kind: "aggregate" };
  if (v === "source-aligned" || v === "source") return { text: "Source", kind: "source" };
  return { text: pretty(t) || "Product", kind: "other" };
}

function envRank(name) {
  const v = String(name || "").toLowerCase();
  if (!v) return 0;
  if (/pre[-_ ]?prod|staging|\buat\b/.test(v)) return 2;
  if (/\bprod(uction)?\b/.test(v) && !/non[-_ ]?prod/.test(v)) return 3;
  if (/sandbox|\bsbx\b|\bdev\b|\bqa\b/.test(v)) return 1;
  return 0;
}

function dagEnv(d) {
  const r = envRank(d && d.astro_deployment_name);
  if (r === 3) return "production";
  if (r === 2) return "preprod";
  if (r === 1) return "sandbox";
  return "unknown";
}

function envLabel(kind) {
  if (kind === "production") return { text: "Production", kind: "production" };
  if (kind === "preprod") return { text: "Pre Prod", kind: "preprod" };
  if (kind === "sandbox") return { text: "Sandbox", kind: "sandbox" };
  return { text: "Unknown", kind: "unknown" };
}

function pipelineForEnv(list, env) {
  const rows = list || [];
  if (!env || env === "all") return rows;
  return rows.filter((d) => dagEnv(d) === env);
}

function snapEnvs(s) {
  const seen = {};
  (s.pipeline || []).forEach((d) => { seen[dagEnv(d)] = true; });
  return ["production", "preprod", "sandbox", "unknown"].filter((k) => seen[k]).map(envLabel);
}

function hasEnv(s, env) {
  if (!env || env === "all") return true;
  return (s.pipeline || []).some((d) => dagEnv(d) === env);
}

function envKind(s, env) {
  if (env && env !== "all") return envLabel(env);
  const names = (s.pipeline || []).map((d) => String(d.astro_deployment_name || "").trim()).filter(Boolean);
  if (!names.length) return envLabel("unknown");
  let best = names[0];
  let bestR = -1;
  names.forEach((n) => {
    const r = envRank(n);
    if (r > bestR) {
      bestR = r;
      best = n;
    }
  });
  if (bestR === 3) return envLabel("production");
  if (bestR === 2) return envLabel("preprod");
  if (bestR === 1) return envLabel("sandbox");
  return { text: pretty(best) || "Unknown", kind: "unknown" };
}

function preferredProductEnv(snap) {
  const kinds = snapEnvs(snap).map((e) => e.kind);
  if (state.productEnv && kinds.includes(state.productEnv)) return state.productEnv;
  if (kinds.includes("production")) return "production";
  return kinds[0] || "production";
}

function scopedSnap(s, env) {
  return Object.assign({}, s, { pipeline: pipelineForEnv(s.pipeline, env) });
}

function productEnvSelect(snap) {
  const envs = snapEnvs(snap);
  if (!envs.length) return "";
  const cur = preferredProductEnv(snap);
  return catalogSelect("product-env", "Environment", envs.map((e) => {
    const n = pipelineForEnv(snap.pipeline, e.kind).length;
    return [e.kind, e.text, n];
  }), cur);
}

function csvCell(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function catalogNote() {
  if (state.noteDismissed) return "";
  try {
    if (sessionStorage.getItem("io-note") === "1") return "";
  } catch { /* ignore */ }
  return `<div class="io-note" role="status">
    <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><path fill="currentColor" d="M8 1.3L15 14H1L8 1.3z"/><rect x="7.25" y="6.2" width="1.5" height="4.2" fill="#151515"/><rect x="7.25" y="11.2" width="1.5" height="1.5" fill="#151515"/></svg>
    <p>Please note: Pipeline, Quality, and Lineage data is live ingest and may change.</p>
    <button type="button" class="io-note-close" data-dismiss-note aria-label="Dismiss">×</button>
  </div>`;
}

function catalogBlurb(s) {
  const p = s.data_product || {};
  const h = s.health || {};
  const msg = String(h.status_message || "").trim();
  if (msg && msg.length < 180 && !/^score capped/i.test(msg)) return msg;
  const kind = typeKind(p.dp_type).text;
  const owner = p.owner_team ? " · " + p.owner_team : "";
  const pipe = pipelineSummary(s.pipeline);
  const qual = qualitySummary(s.quality, s.quality_sources);
  return kind + " data product" + owner + ". Pipeline " + pipe.label.toLowerCase() + ", quality " + qual.label.toLowerCase() + ".";
}

function catalogBlurbHTML(s) {
  const text = catalogBlurb(s);
  const more = text.length > 92 ? ` <span class="io-show-more">show more</span>` : "";
  return `<p class="io-card-desc">${esc(text)}${more}</p>`;
}

function healthChip(h) {
  h = h || {};
  const cls = healthClass(h.status);
  const score = h.total_checks ? Math.round(Number(h.health_score) || 0) : null;
  let text = pretty(h.status);
  if (score != null) text = pretty(h.status) + " " + score + "%";
  const mark = cls === "ok"
    ? `<svg class="io-health-mark" width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><circle cx="7" cy="7" r="7" fill="currentColor"/><path d="M4 7.2l2 2 4.2-4.4" fill="none" stroke="#fff" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>`
    : cls === "warn"
      ? `<svg class="io-health-mark" width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><circle cx="7" cy="7" r="7" fill="currentColor"/><path d="M7 3.6v4.2M7 9.8v.7" fill="none" stroke="#fff" stroke-width="1.6" stroke-linecap="round"/></svg>`
      : cls === "bad"
        ? `<svg class="io-health-mark" width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><circle cx="7" cy="7" r="7" fill="currentColor"/><path d="M4.6 4.6l4.8 4.8M9.4 4.6l-4.8 4.8" fill="none" stroke="#fff" stroke-width="1.6" stroke-linecap="round"/></svg>`
        : "";
  return `<span class="io-health ${cls}">${mark}<span>${esc(text)}</span></span>`;
}

function catalogSelect(id, aria, options, current) {
  return `<select class="io-catalog-select" id="${id}" aria-label="${esc(aria)}">
    ${options.map(([val, title, n]) => {
      const label = n == null ? title : title + " (" + n + ")";
      return `<option value="${esc(val)}"${current === val ? " selected" : ""}>${esc(label)}</option>`;
    }).join("")}
  </select>`;
}

function viewToggle() {
  const grid = state.listView !== "list";
  return `<div class="io-view-toggle" role="group" aria-label="Catalog layout">
    <button type="button" class="io-view-btn chip${grid ? " active" : ""}" data-view="grid" aria-pressed="${grid}" title="Grid">
      <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><rect x="1" y="1" width="6" height="6" rx="1"/><rect x="9" y="1" width="6" height="6" rx="1"/><rect x="1" y="9" width="6" height="6" rx="1"/><rect x="9" y="9" width="6" height="6" rx="1"/></svg>
    </button>
    <button type="button" class="io-view-btn chip${grid ? "" : " active"}" data-view="list" aria-pressed="${!grid}" title="List">
      <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><rect x="1" y="2" width="14" height="2" rx="0.5"/><rect x="1" y="7" width="14" height="2" rx="0.5"/><rect x="1" y="12" width="14" height="2" rx="0.5"/></svg>
    </button>
  </div>`;
}

function filteredCatalog() {
  const all = state.snaps || [];
  const q = state.filter.trim().toLowerCase();
  let snaps = all.filter((s) => {
    const bucket = productHealthBucket((s.health || {}).status);
    if (state.healthFilter !== "all" && bucket !== state.healthFilter) return false;
    const t = String((s.data_product || {}).dp_type || "").toLowerCase();
    if (state.typeFilter === "aggregate" && t !== "aggregate") return false;
    if (state.typeFilter === "source-aligned" && t !== "source-aligned" && t !== "source") return false;
    if (state.envFilter !== "all" && !hasEnv(s, state.envFilter)) return false;
    if (state.highImpact && bucket !== "at_risk") return false;
    if (!q) return true;
    const p = s.data_product || {};
    return [p.data_product_name, p.data_product_id, p.owner_team, p.dp_type, envKind(s).text].some((v) => String(v || "").toLowerCase().includes(q));
  });
  return [...snaps].sort((a, b) => {
    const na = productName(a);
    const nb = productName(b);
    if (state.listSort === "health") return (Number((b.health || {}).health_score) || 0) - (Number((a.health || {}).health_score) || 0) || cmp(na, nb);
    if (state.listSort === "age") return (Number((b.freshness || {}).current_delay_mins) || 0) - (Number((a.freshness || {}).current_delay_mins) || 0) || cmp(na, nb);
    if (state.listSort === "dags") return ((b.pipeline || []).length) - ((a.pipeline || []).length) || cmp(na, nb);
    return cmp(na, nb);
  });
}

function downloadCatalogCSV() {
  const snaps = filteredCatalog();
  const lines = [["name", "id", "type", "environment", "health", "score", "pipeline", "quality", "owner"].map(csvCell).join(",")];
  snaps.forEach((s) => {
    const p = s.data_product || {};
    const h = s.health || {};
    lines.push([
      p.data_product_name || "",
      p.data_product_id || "",
      typeKind(p.dp_type).text,
      envKind(s, state.envFilter).text,
      pretty(h.status),
      h.total_checks ? Math.round(Number(h.health_score) || 0) : "",
      pipelineSummary(pipelineForEnv(s.pipeline, state.envFilter)).label,
      qualitySummary(s.quality, s.quality_sources).label,
      p.owner_team || "",
    ].map(csvCell).join(","));
  });
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "data-products.csv";
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

function chipGroup(options, current, attr) {
  return `<div class="pf-v5-c-toggle-group" role="group">
    ${options.map(([id, title, n]) => {
      const count = n == null ? "" : ` <span class="chip-count">${n}</span>`;
      const sel = current === id ? " pf-m-selected" : "";
      return `<div class="pf-v5-c-toggle-group__item">
        <button type="button" class="pf-v5-c-toggle-group__button chip ${current === id ? "active" : ""}${sel}" data-${attr}="${esc(id)}" aria-pressed="${current === id}">
          <span class="pf-v5-c-toggle-group__text">${esc(title)}${count}</span>
        </button>
      </div>`;
    }).join("")}
  </div>`;
}

function emptyState(title, hint) {
  return `<div class="pf-v5-c-empty-state pf-m-sm empty">
    <div class="pf-v5-c-empty-state__content">
      <div class="pf-v5-c-empty-state__header">
        <h2 class="pf-v5-c-title pf-m-md empty-title">${esc(title)}</h2>
      </div>
      ${hint ? `<div class="pf-v5-c-empty-state__body empty-hint">${esc(hint)}</div>` : ""}
    </div>
  </div>`;
}

function emptyRow(cols, title, hint) {
  return `<tr><td colspan="${cols}">${emptyState(title, hint)}</td></tr>`;
}

function dagRunBucket(d) {
  const st = String(d.dag_status || "").toUpperCase();
  if (st === "FAILED" || d.dag_overall_status === "FAILED") return "failed";
  if (st === "RUNNING") return "running";
  if (d.dag_is_paused) return "paused";
  return "success";
}

function dagFreshBucket(d) {
  const s = String(d.dag_freshness_status || "").toUpperCase();
  if (s === "AT_RISK" || s === "RED") return "at_risk";
  if (s === "CAUTION" || s === "YELLOW") return "caution";
  if (s === "TRUSTED" || s === "GREEN") return "trusted";
  return "other";
}

function productHealthBucket(status) {
  const s = String(status || "").toUpperCase();
  if (s === "TRUSTED" || s === "OK" || s === "GREEN") return "trusted";
  if (s === "CAUTION" || s === "YELLOW" || s === "WARNING") return "caution";
  if (s === "AT_RISK" || s === "RED" || s === "FAILED") return "at_risk";
  return "other";
}

function resetViewState(route) {
  const productKey = route.view + ":" + (route.id || "");
  if (productKey !== lastProductKey) {
    lastProductKey = productKey;
    if (route.view === "detail") state.productEnv = "production";
  }
  const key = route.view + ":" + (route.id || "") + ":" + (route.tab || "");
  if (key === lastRouteKey) return;
  lastRouteKey = key;
  state.tableFilter = "";
  state.sourceFilter = "all";
  state.runFilter = "all";
  state.freshnessFilter = "all";
  state.qualityStatusFilter = "all";
  state.lineageKind = "all";
  state.tableSort = "";
  state.tableSortDir = "asc";
  state.expanded = {};
}

function sortBtn(key, label) {
  const active = state.tableSort === key;
  const arrow = !active ? "" : state.tableSortDir === "desc" ? " ↓" : " ↑";
  return `<button type="button" class="th-sort ${active ? "active" : ""}" data-sort="${esc(key)}">${esc(label)}${arrow}</button>`;
}

function cmp(a, b) {
  const sa = String(a ?? "").toLowerCase();
  const sb = String(b ?? "").toLowerCase();
  return sa.localeCompare(sb, undefined, { numeric: true });
}

function applyTableSort(rows, getters) {
  const key = state.tableSort;
  if (!key || !getters[key]) return rows;
  const dir = state.tableSortDir === "desc" ? -1 : 1;
  return [...rows].sort((a, b) => dir * cmp(getters[key](a), getters[key](b)));
}

function tile(k, v, s, href) {
  const inner = `<article class="pf-v5-c-card pf-m-compact pf-m-flat">
    <div class="pf-v5-c-card__header"><div class="pf-v5-c-card__header-main"><span class="io-metric-k">${esc(k)}</span></div></div>
    <div class="pf-v5-c-card__body"><div class="io-metric-v">${v}</div><div class="io-metric-s">${s}</div></div>
  </article>`;
  if (!href) return inner;
  return `<a class="io-metric-link clickable-tile" href="${href}">${inner}</a>`;
}

function tilesHTML(snap, hrefBase) {
  const pipe = pipelineSummary(snap.pipeline);
  const qual = qualitySummary(snap.quality, snap.quality_sources);
  const lin = lineageSummary(snap.lineage);
  return `
    <div class="io-kpis io-kpis-3">
      ${tile("Pipeline", esc(pipe.label), esc(pipe.detail), hrefBase ? hrefBase + "?tab=pipeline" : "")}
      ${tile("Quality", esc(qual.label), esc(qual.detail), hrefBase ? hrefBase + "?tab=quality" : "")}
      ${tile("Lineage", esc(lin.label), esc(lin.detail), hrefBase ? hrefBase + "?tab=lineage" : "")}
    </div>`;
}

function fmtWhenLong(v) {
  const d = parseDate(v);
  if (!d) return "—";
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(d);
}

function primaryDAG(list) {
  const rows = list || [];
  return rows.find((d) => d.is_primary_dag) || rows[0] || null;
}

function runHeadline(d) {
  if (!d) return { title: "No DAG", cls: "muted" };
  if (d.dag_is_paused) return { title: "Paused", cls: "bad" };
  const st = String(d.dag_status || "").toUpperCase();
  if (st === "SUCCESS") return { title: "Succeeded", cls: "ok" };
  if (st === "FAILED") return { title: "Failed", cls: "bad" };
  if (st === "RUNNING") return { title: "Running", cls: "info" };
  return { title: pretty(d.dag_status), cls: healthClass(d.dag_status) };
}

function freshnessHeadline(d, f) {
  const band = String((d && d.dag_freshness_status) || (f && f.freshness_status) || "").toUpperCase();
  if (band === "TRUSTED" || band === "GREEN" || band === "OK") return { title: "On Time", cls: "ok", badge: "Pipeline is on time" };
  if (band === "CAUTION" || band === "YELLOW") return { title: "Caution", cls: "warn", badge: "Inside the caution window" };
  if (band === "AT_RISK" || band === "RED" || band === "BREACH") return { title: "Delayed", cls: "bad", badge: "Past the SLA" };
  return { title: pretty(band) || "Unknown", cls: "muted", badge: "No SLA sample yet" };
}

function slaBadge(d) {
  if (!d) return { text: "No DAG", cls: "muted" };
  if (d.dag_is_custom) return { text: "Not scored", cls: "muted" };
  const sla = String(d.dag_pipeline_sla_status || "").toUpperCase();
  if (sla === "TRUSTED" || sla === "OK" || sla === "GREEN") return { text: "On schedule", cls: "ok" };
  if (sla === "CAUTION" || sla === "YELLOW") return { text: "Watch SLA", cls: "warn" };
  if (sla === "AT_RISK" || sla === "BREACH" || sla === "RED") return { text: "Behind schedule", cls: "bad" };
  return { text: pretty(d.dag_pipeline_sla_status) || "No SLA", cls: "muted" };
}

function relForWindow(d) {
  const w = state.relWindow || "7d";
  if (w === "30d") return { pct: d.dag_reliability_30d, st: d.dag_reliability_status_30d, n: d.dag_runs_30d, key: "30d" };
  if (w === "90d") return { pct: d.dag_reliability_90d, st: d.dag_reliability_status_90d, n: d.dag_runs_90d, key: "90d" };
  return { pct: d.dag_reliability_7d, st: d.dag_reliability_status_7d, n: d.dag_runs_7d, key: "7d" };
}

function relHeadline(d) {
  if (!d || d.dag_is_custom) return { title: "Not scored", cls: "muted", detail: "Custom DAG" };
  const w = relForWindow(d);
  const cls = healthClass(w.st);
  const pct = fmtPct(w.pct);
  let title = "Stable";
  if (cls === "warn") title = "Partial issues";
  if (cls === "bad") title = "Unstable";
  if (cls === "muted") title = pretty(w.st) || "Unknown";
  const n = w.n || 0;
  return { title, cls: cls === "ok" ? "ok" : cls, detail: n ? pct + " success rate · " + n + " run" + (n === 1 ? "" : "s") : pct + " success rate" };
}

function pfreshIcon(kind) {
  const icons = {
    chain: `<svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" d="M6.2 9.8l3.6-3.6M7 5.2l.6-1.1a2.6 2.6 0 013.8 0l.5.5a2.6 2.6 0 010 3.8L10.8 9M9 10.8l-.6 1.1a2.6 2.6 0 01-3.8 0l-.5-.5a2.6 2.6 0 010-3.8L5.2 7"/></svg>`,
    clock: `<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" stroke-width="1.4"/><path fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" d="M8 5v3.2L10.2 10"/></svg>`,
    shield: `<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round" d="M8 2.2l5 1.6v4.4c0 2.6-2 4.8-5 5.8-3-1-5-3.2-5-5.8V3.8l5-1.6z"/><path fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round" d="M5.8 8.1l1.5 1.5 2.9-3"/></svg>`,
    spark: `<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" d="M8 2.2v2.2M8 11.6v2.2M2.2 8h2.2M11.6 8h2.2M4 4l1.4 1.4M10.6 10.6L12 12M12 4l-1.4 1.4M5.4 10.6L4 12"/><circle cx="8" cy="8" r="1.6" fill="currentColor"/></svg>`,
    info: `<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" stroke-width="1.4"/><path fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" d="M8 7.2V11M8 5.2v.2"/></svg>`,
    warn: `<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round" d="M8 2.4L14.4 14H1.6L8 2.4z"/><path fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" d="M8 6.4v3.4M8 11.6v.3"/></svg>`,
    check: `<svg width="12" height="12" viewBox="0 0 16 16" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" d="M3.5 8.2l3 3.1 6-6.4"/></svg>`,
  };
  return icons[kind] || "";
}

function upstreamHeadline(lin) {
  lin = lin || {};
  const nodes = lin.upstream_sources || [];
  const total = nodes.length || Number(lin.upstream_count) || 0;
  if (!total) return { title: "No sources", cls: "muted", detail: "No upstream in the lineage mart", note: "" };
  const healthy = nodes.filter((n) => healthClass(n.status) === "ok").length;
  const known = nodes.length;
  const ratio = known ? healthy + "/" + known : String(total);
  if (!known) return { title: "Upstream", cls: "muted", detail: total + " source" + (total === 1 ? "" : "s"), note: "" };
  if (healthy === known) return { title: "Healthy", cls: "ok", detail: ratio + " sources healthy", note: "" };
  if (healthy === 0) return { title: "At risk", cls: "bad", detail: ratio + " sources healthy", note: "Upstream sources feeding this pipeline are unhealthy." };
  return { title: "Partial issues", cls: "warn", detail: ratio + " sources healthy", note: "Some data sources feeding this pipeline are delayed or unhealthy." };
}

function primaryFreshnessHTML(snap, hrefBase) {
  const d = primaryDAG(snap.pipeline);
  if (!d) return "";
  const f = snap.freshness || {};
  const run = runHeadline(d);
  const fresh = freshnessHeadline(d, f);
  const sla = slaBadge(d);
  const rel = relHeadline(d);
  const up = upstreamHeadline(snap.lineage);
  const last = lastRunAt(d) || d.last_successful_at;
  const dataAsOf = d.last_successful_at || last;
  const updated = d.computed_at || snap.updated_at;
  const age = d.dag_data_age_mins != null ? fmtSpan(d.dag_data_age_mins) : (last ? fmtRelative(last) : "—");
  const slaSpan = d.dag_sla_minutes != null ? fmtSpan(d.dag_sla_minutes) : (f.sla_minutes != null ? fmtSpan(f.sla_minutes) : "—");
  const windows = [["7d", "7d"], ["30d", "30d"], ["90d", "3m"]].map(([key, label]) =>
    `<button type="button" class="io-rel-btn${(state.relWindow || "7d") === key ? " active" : ""}" data-rel-window="${key}">${label}</button>`
  ).join("");
  const astro = d.astro_url
    ? `<a class="io-pfresh-link astro-open" href="${esc(d.astro_url)}" target="_blank" rel="noopener noreferrer" title="Open ${esc(d.dag_id)} in Astro">Open in Astro</a>`
    : "";
  const upHref = hrefBase ? hrefBase + "?tab=lineage" : "";
  const infoTitle = "Primary DAG " + d.dag_id + ". Data as of is last successful run; last updated is when InOrbit scored the DAG.";
  return `
    <div class="io-pfresh-wrap">
      <div class="io-pfresh-head">
        <h2 class="io-pfresh-title">${pfreshIcon("chain")} Pipeline &amp; Freshness ${astro}</h2>
        <p class="io-pfresh-meta">Data as of ${esc(fmtWhenLong(dataAsOf))} · Last updated ${esc(fmtWhenLong(updated))} <span class="io-pfresh-info" title="${esc(infoTitle)}">${pfreshIcon("info")}</span></p>
      </div>
      <div class="io-pfresh">
        <article class="io-pfresh-card">
          <div class="io-pfresh-k">${pfreshIcon("clock")} Primary Pipeline</div>
          <div class="io-pfresh-v ${run.cls}"><span class="dot" aria-hidden="true"></span>${esc(run.title)}</div>
          <div class="io-pfresh-s">${esc(fmtWhenLong(last))}</div>
          <span class="io-pfresh-badge ${sla.cls}">${sla.cls === "ok" ? pfreshIcon("check") : ""}${esc(sla.text)}</span>
        </article>
        <article class="io-pfresh-card">
          <div class="io-pfresh-k">${pfreshIcon("clock")} Data Freshness</div>
          <div class="io-pfresh-v ${fresh.cls}"><span class="dot" aria-hidden="true"></span>${esc(fresh.title)}</div>
          <div class="io-pfresh-s">Last updated ${esc(age)} · SLA: ${esc(slaSpan)}</div>
          <span class="io-pfresh-badge ${fresh.cls}">${fresh.cls === "ok" ? pfreshIcon("check") : ""}${esc(fresh.badge)}</span>
        </article>
        <article class="io-pfresh-card">
          <div class="io-pfresh-k">${pfreshIcon("shield")} Pipeline Reliability</div>
          <div class="io-pfresh-v ${rel.cls}"><span class="dot" aria-hidden="true"></span>${esc(rel.title)}</div>
          <div class="io-pfresh-s">${esc(rel.detail)}</div>
          <div class="io-rel-toggle" role="group" aria-label="Reliability window">${windows}</div>
        </article>
        ${upHref ? `<a class="io-pfresh-card io-pfresh-linkcard" href="${upHref}">` : `<article class="io-pfresh-card">`}
          <div class="io-pfresh-k">${pfreshIcon("spark")} Upstream Sources</div>
          <div class="io-pfresh-v ${up.cls}"><span class="dot" aria-hidden="true"></span>${esc(up.title)}</div>
          <div class="io-pfresh-s">${esc(up.detail)}</div>
          ${up.note ? `<span class="io-pfresh-note ${up.cls}">${pfreshIcon("warn")}${esc(up.note)}</span>` : ""}
        ${upHref ? `</a>` : `</article>`}
      </div>
    </div>`;
}

function fetchBanner(extra) {
  const m = state.meta || {};
  const warnings = m.warnings || [];
  const warn = warnings.length
    ? `<div class="pf-v5-c-alert pf-m-danger pf-m-inline alert-banner" role="alert">
        <h4 class="pf-v5-c-alert__title">Ingest warning</h4>
        <div class="pf-v5-c-alert__description">${warnings.map((w) => `<p>${esc(w)}</p>`).join("")}</div>
      </div>`
    : "";
  const bits = [
    extra,
    "Airflow " + (parseDate(m.last_astro_run_poll) ? fmtRelative(m.last_astro_run_poll) : "—"),
    "Quality " + (parseDate(m.last_quality_poll) ? fmtRelative(m.last_quality_poll) : "—"),
    "Lineage " + (parseDate(m.last_lineage_poll) ? fmtRelative(m.last_lineage_poll) : "—"),
  ].filter((x) => x && !String(x).endsWith("—"));
  return `${warn}<p class="fetch-banner" role="status" title="${esc("Airflow " + fmtTime(m.last_astro_run_poll) + " · Quality " + fmtTime(m.last_quality_poll) + " · " + localZone())}">${bits.map(esc).join(" · ")}</p>`;
}

function productName(s) {
  const p = s.data_product || {};
  return p.data_product_name || p.data_product_id || "";
}

function renderList() {
  const all = state.snaps || [];
  const healthCounts = { trusted: 0, caution: 0, at_risk: 0, other: 0 };
  const typeCounts = { aggregate: 0, "source-aligned": 0, other: 0 };
  const envCounts = { production: 0, preprod: 0, sandbox: 0, unknown: 0 };
  all.forEach((s) => {
    healthCounts[productHealthBucket((s.health || {}).status)] += 1;
    const t = String((s.data_product || {}).dp_type || "").toLowerCase();
    if (t === "aggregate") typeCounts.aggregate += 1;
    else if (t === "source-aligned" || t === "source") typeCounts["source-aligned"] += 1;
    else typeCounts.other += 1;
    const kinds = {};
    (s.pipeline || []).forEach((d) => { kinds[dagEnv(d)] = true; });
    Object.keys(kinds).forEach((k) => {
      if (envCounts[k] != null) envCounts[k] += 1;
    });
  });
  const catalogEnv = state.envFilter || "production";
  const snaps = filteredCatalog();
  const cards = snaps.map((s) => {
    const p = s.data_product || {};
    const id = p.data_product_id;
    const view = scopedSnap(s, catalogEnv);
    const h = s.health || {};
    const tp = typeKind(p.dp_type);
    const env = envKind(s, catalogEnv);
    const href = `#/data-product/${encodeURIComponent(id)}`;
    const tone = healthClass(h.status);
    return `<a class="io-card tone-${tone}" href="${href}">
      <div class="io-card-head">
        <h2 class="io-card-name">${esc(p.data_product_name || id)}</h2>
        ${healthChip(h)}
      </div>
      ${catalogBlurbHTML(view)}
      <div class="io-card-pills">
        <span class="io-pill io-pill-${tp.kind}">${esc(tp.text)}</span>
        <span class="io-pill io-pill-env-${env.kind}">${esc(env.text)}</span>
      </div>
    </a>`;
  }).join("");
  const rows = snaps.map((s) => {
    const p = s.data_product || {};
    const id = p.data_product_id;
    const view = scopedSnap(s, catalogEnv);
    const h = s.health || {};
    const primary = primaryDAG(view.pipeline);
    const pipe = pipelineSummary(view.pipeline);
    const qual = qualitySummary(s.quality, s.quality_sources);
    const href = `#/data-product/${encodeURIComponent(id)}`;
    const age = primary && primary.dag_data_age_mins != null ? fmtSpan(primary.dag_data_age_mins) : "—";
    return `<tr class="io-row" data-href="${href}">
      <td><a class="io-product-name" href="${href}">${esc(p.data_product_name || id)}</a></td>
      <td>${esc(typeKind(p.dp_type).text)}</td>
      <td>${esc(envKind(s, catalogEnv).text)}</td>
      <td>${healthChip(h)}</td>
      <td>${esc(pipe.label)}</td>
      <td>${esc(qual.label)}<div class="muted-cell">${esc(qual.detail)}</div></td>
      <td>${esc(age)}</td>
    </tr>`;
  }).join("");
  const emptyHint = all.length
    ? "Clear the search or filters to see the full list."
    : "Start the process with a config that lists data products, then wait for the first Airflow poll.";
  const emptyTitle = all.length ? "No products match these filters." : "No data products in the snapshot.";
  const catalog = state.listView === "list"
    ? `<div class="io-table-wrap"><table class="pf-v5-c-table pf-m-compact io-catalog" role="grid">
        <thead><tr><th>Product</th><th>Type</th><th>Environment</th><th>Health</th><th>Pipeline</th><th>Quality</th><th>Data age</th></tr></thead>
        <tbody>${rows || emptyRow(7, emptyTitle, emptyHint)}</tbody>
      </table></div>`
    : `<div class="io-grid">${cards || `<div class="io-empty">${emptyState(emptyTitle, emptyHint)}</div>`}</div>`;
  const warnings = ((state.meta || {}).warnings || []);
  const warn = warnings.length
    ? `<div class="io-note io-note-warn" role="alert">${warnings.map((w) => `<p>${esc(w)}</p>`).join("")}</div>`
    : "";
  return `
    <section class="pf-v5-c-page__main-section pf-m-light io-catalog-head">
      ${catalogNote()}
      ${warn}
      <div class="io-header-row">
        <div>
          <h1 class="pf-v5-c-title pf-m-2xl io-page-title">Data Products <span class="io-count">${all.length}</span></h1>
          <p class="io-lede">List of data products currently active.</p>
        </div>
      </div>
      <div class="io-toolbar">
        <label class="io-search-wrap">
          <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="5" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="M11 11.5L14 14.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
          <input class="io-search-input search" id="filter" type="search" aria-label="Search by name" placeholder="Search by name" value="${esc(state.filter)}" />
          <button type="button" class="io-search-clear" data-clear-search aria-label="Clear search"${state.filter ? "" : " hidden"}>×</button>
        </label>
        ${catalogSelect("env-filter", "Environment", [
          ["production", "Production", envCounts.production],
          ["preprod", "Pre Prod", envCounts.preprod],
          ["sandbox", "Sandbox", envCounts.sandbox],
          ["all", "All", all.length],
        ], state.envFilter)}
        ${catalogSelect("type-filter", "Type", [
          ["all", "Type", all.length],
          ["aggregate", "Aggregate", typeCounts.aggregate],
          ["source-aligned", "Source", typeCounts["source-aligned"]],
        ], state.typeFilter)}
        ${catalogSelect("health-filter", "Health", [
          ["all", "Health", all.length],
          ["trusted", "Trusted", healthCounts.trusted],
          ["caution", "Caution", healthCounts.caution],
          ["at_risk", "At risk", healthCounts.at_risk],
        ], state.healthFilter)}
        <button type="button" class="io-filter-pill${state.highImpact ? " active" : ""}" data-high-impact="1" aria-pressed="${state.highImpact}">High impact</button>
        <div class="io-toolbar-end">
          ${viewToggle()}
          <button type="button" class="io-csv-btn" data-csv="1">
            <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" d="M8 2v8M5 7.5L8 11l3-3.5M3 13.2h10"/></svg>
            CSV
          </button>
        </div>
      </div>
      ${catalog}
    </section>`;
}

function dagGetters() {
  return {
    dag: (d) => d.dag_id,
    deployment: (d) => d.astro_deployment_name,
    run: (d) => d.dag_status,
    freshness: (d) => d.dag_freshness_status,
    overall: (d) => d.dag_overall_status,
    last: (d) => lastRunAt(d) || "",
    next: (d) => d.dag_next_expected_at || "",
    sla: (d) => d.dag_pipeline_sla_status,
    slamins: (d) => d.dag_sla_minutes,
    rel7: (d) => d.dag_reliability_7d,
    age: (d) => d.dag_data_age_mins,
    interval: (d) => d.dag_expected_interval_mins,
  };
}

function nodeKind(n) {
  const t = String((n && n.type) || "").toLowerCase();
  if (t === "data_product" || t === "") return "data_product";
  if (t.includes("fivetran") || t.includes("snowpipe") || t.includes("external")) return "source";
  if (t === "service_account") return "service_account";
  if (t === "consumer_group") return "consumer_group";
  return t || "other";
}

function lineageNodeHTML(n, known) {
  const kind = nodeKind(n);
  const score = n.health_score != null ? fmtScore(n.health_score) : "—";
  const name = n.name || "—";
  const canLink = kind === "data_product" && name;
  const inner = `<span class="lineage-name">${esc(name)}</span>${label(n.status || "UNKNOWN")} <span class="muted-cell">${esc(pretty(n.type || "data product"))} · ${esc(score)}</span>`;
  if (canLink) {
    return `<a class="pf-v5-c-card pf-m-compact pf-m-clickable pf-m-flat lineage-pill" href="#/data-product/${encodeURIComponent(name)}?tab=lineage">${inner}</a>`;
  }
  return `<div class="pf-v5-c-card pf-m-compact pf-m-flat lineage-pill">${inner}</div>`;
}

function lineageTable(title, nodes, kind) {
  const q = state.tableFilter.trim().toLowerCase();
  let rows = (nodes || []).filter((n) => {
    if (kind !== "all" && nodeKind(n) !== kind) return false;
    return rowMatch(q, [n.name, n.type, n.status]);
  });
  rows = applyTableSort(rows, {
    name: (n) => n.name,
    type: (n) => n.type,
    status: (n) => n.status,
    score: (n) => n.health_score,
  });
  const empty = emptyRow(4, "No " + title.toLowerCase() + " in the mart.", "Lineage is loaded from the observability MARTS.DP_LINEAGE table, not from Airflow.");
  return `
    <div>
      <h3 class="pf-v5-c-title pf-m-md lineage-h">${esc(title)} <span class="chip-count">${rows.length}</span></h3>
      <div class="io-table-wrap lineage-table">
        <table class="pf-v5-c-table pf-m-compact" role="grid">
          <thead><tr>
            <th>${sortBtn("name", "Name")}</th>
            <th>${sortBtn("type", "Type")}</th>
            <th>${sortBtn("status", "Status")}</th>
            <th>${sortBtn("score", "Health")}</th>
          </tr></thead>
          <tbody>
            ${rows.map((n) => {
              const kind = nodeKind(n);
              const nameCell = kind === "data_product" && n.name
                ? `<a href="#/data-product/${encodeURIComponent(n.name)}?tab=lineage">${esc(n.name)}</a>`
                : esc(n.name || "—");
              return `<tr>
                <td>${nameCell}</td>
                <td>${esc(pretty(n.type || "data product"))}</td>
                <td>${label(n.status || "UNKNOWN")}</td>
                <td>${n.health_score != null ? esc(fmtScore(n.health_score)) : "—"}</td>
              </tr>`;
            }).join("") || empty}
          </tbody>
        </table>
      </div>
    </div>`;
}

function renderLineage(snap, base) {
  const lin = snap.lineage || {};
  const up = lin.upstream_sources || [];
  const down = lin.downstream_consumers || [];
  const kind = state.lineageKind;
  const allNodes = up.concat(down);
  const counts = {
    all: allNodes.length,
    data_product: allNodes.filter((n) => nodeKind(n) === "data_product").length,
    source: allNodes.filter((n) => nodeKind(n) === "source").length,
    service_account: allNodes.filter((n) => nodeKind(n) === "service_account").length,
    consumer_group: allNodes.filter((n) => nodeKind(n) === "consumer_group").length,
  };
  const known = {};
  (state.snaps || []).forEach((s) => {
    const p = s.data_product || {};
    if (p.data_product_id) known[p.data_product_id] = true;
    if (p.data_product_name) known[p.data_product_name] = true;
  });
  const impact = lin.blast_radius_score ? label(lin.blast_radius_score) : "";
  const computed = parseDate(lin.computed_at) ? fmtRelative(lin.computed_at) : "—";
  return `
    <div class="io-stat-row">
      <div class="io-stat"><strong>${esc(String(lin.upstream_count || up.length))}</strong><span>Upstream</span></div>
      <div class="io-stat"><strong>${esc(String(lin.direct_downstream_count || down.length))}</strong><span>Downstream</span></div>
      <div class="io-stat"><strong>${esc(String(lin.blast_radius_count || 0))}</strong><span>Blast radius</span></div>
    </div>
    <div class="lineage-flow">
      <div class="lineage-col up">${up.slice(0, 8).map((n) => lineageNodeHTML(n, known)).join("") || `<p class="muted-cell">No upstream</p>`}</div>
      <div class="pf-v5-c-card pf-m-compact lineage-center">
        <div class="pf-v5-c-card__body">
        <div class="k io-metric-k">This product</div>
        <div class="v io-metric-v">${esc((snap.data_product || {}).data_product_name || "")}</div>
        <div class="s">${label((snap.health || {}).status)}${impact ? " " + impact : ""}</div>
        </div>
      </div>
      <div class="lineage-col down">${down.slice(0, 8).map((n) => lineageNodeHTML(n, known)).join("") || `<p class="muted-cell">No downstream</p>`}</div>
    </div>
    ${tableToolbar(allNodes.length, allNodes.length, "Filter lineage by name or type",
      chipGroup([
        ["all", "All", counts.all],
        ["data_product", "Data products", counts.data_product],
        ["source", "Sources", counts.source],
        ["service_account", "Service accounts", counts.service_account],
        ["consumer_group", "Consumer groups", counts.consumer_group],
      ], kind, "lkind")
    )}
    <div class="lineage-grid">
      ${lineageTable("Upstream sources", up, kind)}
      ${lineageTable("Downstream consumers", down, kind)}
    </div>
    <p class="sub">From the observability warehouse mart (DP_LINEAGE), not a live poll. Computed ${esc(computed)}. ${lin.direct_dp_consumer_count || 0} downstream data products · ${lin.service_account_count || 0} service accounts · ${lin.consumer_group_count || 0} consumer groups.</p>`;
}

function renderDetail(route) {
  const snap = state.snaps.find((s) => (s.data_product || {}).data_product_id === route.id);
  if (!snap) {
    return `<section class="pf-v5-c-page__main-section"><p class="error">Data product not found: ${esc(route.id)}</p><p><a class="pf-v5-c-button pf-m-link" href="#/">Back to data products</a></p></section>`;
  }
  const env = preferredProductEnv(snap);
  state.productEnv = env;
  const view = scopedSnap(snap, env);
  const p = snap.data_product;
  const tab = route.tab;
  const base = `#/data-product/${encodeURIComponent(route.id)}`;
  const updatedRel = parseDate(snap.updated_at) ? fmtRelative(snap.updated_at) : "—";
  const tabDefs = [
    ["overview", "Overview"],
    ["pipeline", "Pipeline"],
    ["quality", "Quality"],
    ["lineage", "Lineage"],
  ];
  const tabs = tabDefs.map(([t, title]) => {
    const href = `${base}?tab=${t}`;
    const on = tab === t;
    return `<li class="pf-v5-c-tabs__item ${on ? "pf-m-current" : ""}">
      <a class="pf-v5-c-tabs__link tab ${on ? "active" : ""}" href="${href}" role="tab" aria-selected="${on}">
        <span class="pf-v5-c-tabs__item-text">${title}</span>
      </a>
    </li>`;
  }).join("");
  let body = "";
  if (tab === "pipeline") {
    const q = state.tableFilter.trim().toLowerCase();
    const all = sortDAGs(view.pipeline);
    const failed = all.filter((d) => dagRunBucket(d) === "failed").length;
    const running = all.filter((d) => dagRunBucket(d) === "running").length;
    const paused = all.filter((d) => dagRunBucket(d) === "paused").length;
    const success = all.filter((d) => dagRunBucket(d) === "success").length;
    const atRisk = all.filter((d) => dagFreshBucket(d) === "at_risk").length;
    const caution = all.filter((d) => dagFreshBucket(d) === "caution").length;
    const trusted = all.filter((d) => dagFreshBucket(d) === "trusted").length;
    const ok = success;
    const dagEmptyHint = "Live Airflow only lists deployments in config. The warehouse pipeline mart fills DAGs for catalog products that are not on those deployments.";
    let rows = all.filter((d) => {
      if (state.runFilter !== "all" && dagRunBucket(d) !== state.runFilter) return false;
      if (state.freshnessFilter !== "all" && dagFreshBucket(d) !== state.freshnessFilter) return false;
      return rowMatch(q, [d.dag_id, d.astro_deployment_name, d.dag_status, d.dag_overall_status, d.dag_freshness_status, d.astro_url, d.external_run_id, d.trigger_type, d.dag_frequency_display, d.dag_overall_status_description, d.is_primary_dag ? "primary" : "", d.dag_is_paused ? "paused" : "", d.dag_is_custom ? "custom" : ""]);
    });
    rows = applyTableSort(rows, dagGetters());
    const dagFilterBar = tableToolbar(rows.length, all.length, "Filter DAGs by id or deployment",
      chipGroup([
        ["all", "All", all.length],
        ["failed", "Failed", failed],
        ["running", "Running", running],
        ["paused", "Paused", paused],
        ["success", "Success", success],
      ], state.runFilter, "run") +
      chipGroup([
        ["all", "Any SLA", all.length],
        ["at_risk", "At risk", atRisk],
        ["caution", "Caution", caution],
        ["trusted", "Trusted", trusted],
      ], state.freshnessFilter, "fresh")
    );
    const cols = 8;
    const noDags = all.length === 0
      ? emptyRow(cols, "No active DAGs matched this product.", dagEmptyHint)
      : emptyRow(cols, "No DAGs match these filters.", "Clear the run or SLA chips, or the search box.");
    const customCount = all.filter((d) => d.dag_is_custom).length;
    body = `
      ${primaryFreshnessHTML(view, base)}
      <div class="io-stat-row">
        <div class="io-stat"><strong>${all.length}</strong><span>DAGs</span></div>
        <div class="io-stat"><strong>${failed}</strong><span>Failed</span></div>
        <div class="io-stat"><strong>${running}</strong><span>Running</span></div>
        <div class="io-stat"><strong>${atRisk}</strong><span>SLA risk</span></div>
      </div>
      ${dagFilterBar}
      <div class="io-table-wrap">
        <table class="pf-v5-c-table pf-m-compact pipe-table" role="grid">
          <thead><tr>
            <th class="col-expand"><span class="vh">Expand</span></th>
            <th>${sortBtn("dag", "Pipeline")}</th>
            <th>${sortBtn("run", "Status")}</th>
            <th>${sortBtn("sla", "SLA")}</th>
            <th>${sortBtn("last", "Last Run")}</th>
            <th>${sortBtn("next", "Next Run")}</th>
            <th>${sortBtn("rel7", "Reliability")}</th>
            <th>Astro</th>
          </tr></thead>
          <tbody>
            ${rows.map((d) => pipelineRows(d)).join("") || noDags}
          </tbody>
        </table>
      </div>
      <p class="sub">${all.length} active DAG${all.length === 1 ? "" : "s"}${customCount ? " · " + customCount + " custom (shown, excluded from scores)" : ""} · Status, SLA, last/next run on the table. Expand a row for interval, age, and Astro URL. <strong>Open in Astro</strong> is on each row.</p>`;
  } else if (tab === "quality") {
    const q = state.tableFilter.trim().toLowerCase();
    const src = state.sourceFilter;
    const all = [...dqChecks(snap.quality)].sort((a, b) => String(a.status).localeCompare(String(b.status)) || String(a.check_name).localeCompare(String(b.check_name)));
    const scoped = src === "all" ? all : all.filter((c) => c.source_type === src);
    let rows = scoped.filter((c) => {
      if (state.qualityStatusFilter !== "all" && String(c.status || "").toUpperCase() !== state.qualityStatusFilter) return false;
      return rowMatch(q, [c.check_name, c.check_description, c.status, c.dimension, c.severity, c.source_display_name, c.source_type, c.source_table, c.element]);
    });
    rows = applyTableSort(rows, {
      check: (c) => c.check_name,
      status: (c) => c.status,
      dimension: (c) => c.dimension,
      severity: (c) => c.severity,
      source: (c) => c.source_display_name || c.source_type,
      table: (c) => c.source_table,
      executed: (c) => c.executed_at || "",
    });
    const failed = scoped.filter((c) => c.status === "FAILED").length;
    const warn = scoped.filter((c) => c.status === "WARNING").length;
    const passed = scoped.filter((c) => c.status === "PASSED").length;
    const vx = all.filter((c) => c.source_type === "VALIDATION").length;
    const dbt = all.filter((c) => c.source_type === "DBT_TEST").length;
    const latestDbt = all.filter((c) => c.source_type === "DBT_TEST").map((c) => c.executed_at).filter(Boolean).sort().slice(-1)[0];
    const latestVx = all.filter((c) => c.source_type === "VALIDATION").map((c) => c.executed_at).filter(Boolean).sort().slice(-1)[0];
    const sources = snap.quality_sources || {};
    const vxMissing = sources.validation && sources.validation.status === "missing";
    const dbtMissing = sources.dbt && sources.dbt.status === "missing";
    const emptyTitle = src === "DBT_TEST"
      ? (dbtMissing ? "Elementary checks not exist" : "No Elementary rows in the latest dbt invocation for this product.")
      : src === "VALIDATION"
        ? (vxMissing ? "Validation checks not exist" : "No validation rows in the latest warehouse run for this product.")
        : (vxMissing && !dbt ? "Validation checks not exist" : "No validation or Elementary checks in the latest warehouse run.");
    const emptyHint = all.length
      ? "Clear the source or status chips, or the search box."
      : vxMissing
        ? "The validation warehouse table does not exist for this product, so ingest skips it."
        : "Validation ingest is skipped when that table is missing. Elementary still loads from DBTLOGS when that table exists.";
    body = `
      <div class="io-stat-row">
        <div class="io-stat"><strong>${scoped.length}</strong><span>Checks</span></div>
        <div class="io-stat"><strong>${failed}</strong><span>Failed</span></div>
        <div class="io-stat"><strong>${vx}</strong><span>Validation</span></div>
        <div class="io-stat"><strong>${dbt}</strong><span>Elementary</span></div>
      </div>
      ${tableToolbar(rows.length, scoped.length, "Filter checks",
        chipGroup([
          ["all", "All", all.length],
          ["VALIDATION", "Validation", vx],
          ["DBT_TEST", "Elementary", dbt],
        ], src, "source") +
        chipGroup([
          ["all", "Any status", scoped.length],
          ["FAILED", "Failed", failed],
          ["WARNING", "Warning", warn],
          ["PASSED", "Passed", passed],
        ], state.qualityStatusFilter, "qstatus")
      )}
      <div class="io-table-wrap">
        <table class="pf-v5-c-table pf-m-compact" role="grid">
          <thead><tr>
            <th>${sortBtn("check", "Check")}</th><th>Description</th>
            <th>${sortBtn("status", "Status")}</th><th>${sortBtn("dimension", "Dimension")}</th>
            <th>${sortBtn("severity", "Severity")}</th><th>${sortBtn("source", "Source")}</th>
            <th>${sortBtn("table", "Table")}</th><th>Element</th>
            <th>${sortBtn("executed", "Executed")}</th>
          </tr></thead>
          <tbody>
            ${rows.map((c) => `
              <tr>
                <td class="check-name">${esc(c.check_name)}${c.is_cde ? " · CDE" : ""}</td>
                <td class="muted-cell">${esc(c.check_description || "—")}</td>
                <td>${label(c.status)}</td>
                <td>${esc(pretty(c.dimension))}</td>
                <td>${esc(pretty(c.severity))}</td>
                <td>${esc(c.source_display_name || c.source_type)}</td>
                <td class="muted-cell">${esc(c.source_table || "—")}</td>
                <td class="muted-cell">${esc(c.element || "—")}</td>
                <td>${fmtWhen(c.executed_at)}</td>
              </tr>`).join("") || emptyRow(9, emptyTitle, emptyHint)}
          </tbody>
        </table>
      </div>
      <p class="sub">${vxMissing ? "Validation checks not exist" : vx + " validation" + (latestVx ? " " + fmtRelative(latestVx) : "")} · ${dbtMissing ? "Elementary checks not exist" : dbt + " Elementary" + (latestDbt ? " " + fmtRelative(latestDbt) : "")}. Latest warehouse invocation only.</p>`;
  } else if (tab === "lineage") {
    body = renderLineage(snap, base);
  } else {
    const h = snap.health || {};
    const f = snap.freshness || {};
    const cls = healthClass(h.status);
    const trend = state.trends[route.id] || [];
    body = `
      ${primaryFreshnessHTML(view, base)}
      ${overviewCharts(snap, trend)}
      ${tilesHTML(view, base)}
      <div class="pf-v5-c-alert pf-m-inline ${cls === "bad" ? "pf-m-danger" : cls === "warn" ? "pf-m-warning" : "pf-m-info"} msg ${cls}">
        <h4 class="pf-v5-c-alert__title">${esc(h.status_message || f.status_reason || "No status message")}</h4>
      </div>
      <p class="sub">Health Score v2 from live validation/Elementary and Astro pipeline checks. Open Pipeline for Status, SLA, and Astro links. Lineage is the warehouse mart graph.</p>`;
  }
  return `
    <section class="pf-v5-c-page__main-section pf-m-light io-product-head">
      <nav class="pf-v5-c-breadcrumb crumb" aria-label="Breadcrumb">
        <ol class="pf-v5-c-breadcrumb__list">
          <li class="pf-v5-c-breadcrumb__item"><a class="pf-v5-c-breadcrumb__link" href="#/">Data products</a></li>
          <li class="pf-v5-c-breadcrumb__item">${esc(p.data_product_name || p.data_product_id)}</li>
        </ol>
      </nav>
      <div class="io-title-row title-row">
        <h1 class="pf-v5-c-title pf-m-2xl">${esc(p.data_product_name || p.data_product_id)}</h1>
        ${healthChip(snap.health || {})}
        ${productEnvSelect(snap)}
      </div>
      <p class="io-lede">${esc(pretty(p.dp_type) || "")}${p.owner_team ? " · " + esc(p.owner_team) : ""}</p>
      ${fetchBanner("Updated " + updatedRel)}
      <div class="pf-v5-c-tabs pf-m-page-insets io-tabs tabs">
        <ul class="pf-v5-c-tabs__list" role="tablist" aria-label="Product sections">${tabs}</ul>
      </div>
    </section>
    <section class="pf-v5-c-page__main-section io-product-body">${body}</section>`;
}

function renderMast() {
  const meta = document.getElementById("mast-meta");
  if (!meta) return;
  const m = state.meta || {};
  const sc = m.scale || {};
  const rss = fmtBytes(sc.estimated_rss_bytes_at_target);
  const json = fmtBytes(sc.estimated_snapshot_json_bytes_at_target);
  const air = parseDate(m.last_astro_run_poll) ? fmtRelative(m.last_astro_run_poll) : "—";
  const qual = parseDate(m.last_quality_poll) ? fmtRelative(m.last_quality_poll) : "—";
  meta.title = "Airflow " + fmtTime(m.last_astro_run_poll) + " · Quality " + fmtTime(m.last_quality_poll) + " · " + localZone() + " · " + (sc.target_data_products || 200) + "-DP estimate " + json + " snapshots / " + rss + " RSS";
  meta.innerHTML = `<span>${esc(m.mode || "unknown")}${m.data_product_count != null ? " · " + m.data_product_count + " products" : ""}</span>
    <span>Airflow ${esc(air)} · Quality ${esc(qual)}</span>`;
}

function applyChip(el) {
  if (el.dataset.health) state.healthFilter = el.dataset.health;
  if (el.dataset.type) state.typeFilter = el.dataset.type;
  if (el.dataset.listSort) state.listSort = el.dataset.listSort;
  if (el.dataset.source) {
    state.sourceFilter = el.dataset.source;
    state.qualityStatusFilter = "all";
  }
  if (el.dataset.run) state.runFilter = el.dataset.run;
  if (el.dataset.fresh) state.freshnessFilter = el.dataset.fresh;
  if (el.dataset.qstatus) state.qualityStatusFilter = el.dataset.qstatus;
  if (el.dataset.lkind) state.lineageKind = el.dataset.lkind;
  if (el.dataset.view) state.listView = el.dataset.view;
}

function bindApp() {
  const app = document.getElementById("app");
  if (!app || app.dataset.bound === "1") return;
  app.dataset.bound = "1";
  app.addEventListener("click", (e) => {
    const csv = e.target.closest("[data-csv]");
    if (csv) {
      e.preventDefault();
      downloadCatalogCSV();
      return;
    }
    const clear = e.target.closest("[data-clear-search]");
    if (clear) {
      e.preventDefault();
      state.filter = "";
      render();
      return;
    }
    const dismiss = e.target.closest("[data-dismiss-note]");
    if (dismiss) {
      e.preventDefault();
      state.noteDismissed = true;
      try { sessionStorage.setItem("io-note", "1"); } catch { /* ignore */ }
      render();
      return;
    }
    const impact = e.target.closest("[data-high-impact]");
    if (impact) {
      e.preventDefault();
      state.highImpact = !state.highImpact;
      render();
      return;
    }
    const relWin = e.target.closest("[data-rel-window]");
    if (relWin) {
      e.preventDefault();
      state.relWindow = relWin.dataset.relWindow;
      render();
      return;
    }
    const trendWin = e.target.closest("[data-trend-window]");
    if (trendWin) {
      e.preventDefault();
      state.trendWindow = trendWin.dataset.trendWindow;
      render();
      return;
    }
    const expand = e.target.closest("[data-expand]");
    if (expand) {
      e.preventDefault();
      e.stopPropagation();
      const key = expand.dataset.expand;
      state.expanded[key] = !state.expanded[key];
      render();
      return;
    }
    const sort = e.target.closest("[data-sort]");
    if (sort) {
      const key = sort.dataset.sort;
      if (state.tableSort === key) {
        state.tableSortDir = state.tableSortDir === "asc" ? "desc" : "asc";
      } else {
        state.tableSort = key;
        state.tableSortDir = (key === "last" || key === "age" || key === "executed" || key === "rel7") ? "desc" : "asc";
      }
      render();
      return;
    }
    const chip = e.target.closest(".chip");
    if (chip) {
      applyChip(chip);
      render();
      return;
    }
    const row = e.target.closest("tr[data-href]");
    if (row && !e.target.closest("a,button")) go(row.dataset.href);
  });
  app.addEventListener("change", (e) => {
    const id = e.target.id;
    if (id === "health-filter") state.healthFilter = e.target.value;
    else if (id === "type-filter") state.typeFilter = e.target.value;
    else if (id === "env-filter") state.envFilter = e.target.value;
    else if (id === "product-env") state.productEnv = e.target.value;
    else if (id === "list-sort") state.listSort = e.target.value;
    else return;
    render();
  });
  app.addEventListener("input", (e) => {
    const id = e.target.id;
    if (id !== "filter" && id !== "table-filter") return;
    if (id === "filter") state.filter = e.target.value;
    else state.tableFilter = e.target.value;
    clearTimeout(filterTimer);
    filterTimer = setTimeout(() => {
      render();
      const again = document.getElementById(id);
      if (again) {
        again.focus();
        const len = again.value.length;
        again.setSelectionRange(len, len);
      }
    }, 80);
  });
}

function routeKey(route) {
  return route.view + ":" + (route.id || "") + ":" + (route.tab || "");
}

function dataSig(snaps) {
  return (snaps || []).map((s) => {
    const p = s.data_product || {};
    const h = s.health || {};
    const pipe = s.pipeline || [];
    return [
      p.data_product_id,
      h.health_score,
      h.status,
      pipe.length,
      pipe.map((d) => (d.dag_id || "") + ":" + (d.dag_status || "") + ":" + (d.dag_completed_at || "")).join(","),
      (s.quality || []).length,
      s.updated_at,
    ].join("|");
  }).join("\n");
}

function render() {
  const route = parseRoute();
  resetViewState(route);
  const app = document.getElementById("app");
  const y = window.scrollY;
  const key = routeKey(route);
  const same = lastRenderedRoute === key;
  if (!state.loaded) {
    app.innerHTML = `<section class="pf-v5-c-page__main-section"><p class="sub">Loading data products…</p></section>`;
    renderMast();
    return;
  }
  app.innerHTML = route.view === "detail" ? renderDetail(route) : renderList();
  renderMast();
  lastRenderedRoute = key;
  if (same) window.scrollTo(0, y);
  else window.scrollTo(0, 0);
}

function loadTrend(id) {
  return getJSON("/v1/data-products/" + encodeURIComponent(id) + "/health-trend")
    .then((t) => { state.trends[id] = t; })
    .catch(() => { state.trends[id] = []; });
}

function onHashChange() {
  const route = parseRoute();
  render();
  if (route.view === "detail" && route.id && state.trends[route.id] == null) {
    loadTrend(route.id).then(() => {
      if (parseRoute().id === route.id) render();
    });
  }
}

async function refresh() {
  state.meta = await getJSON("/v1/meta");
  const snaps = (await getJSON("/v1/snapshots")) || [];
  const sig = dataSig(snaps);
  const route = parseRoute();
  if (route.view === "detail" && route.id) {
    try {
      state.trends[route.id] = await getJSON("/v1/data-products/" + encodeURIComponent(route.id) + "/health-trend");
    } catch (_) {
      if (state.trends[route.id] == null) state.trends[route.id] = [];
    }
  }
  const skip = state.loaded && sig === dataSigCache && routeKey(route) === lastRenderedRoute && route.tab !== "overview";
  state.snaps = snaps;
  dataSigCache = sig;
  state.loaded = true;
  if (skip) {
    renderMast();
    return;
  }
  render();
}

async function boot() {
  bindApp();
  try {
    await refresh();
  } catch (err) {
    document.getElementById("app").innerHTML = `<section class="pf-v5-c-page__main-section"><p class="error">${esc(err.message)}</p></section>`;
  }
}

window.addEventListener("hashchange", onHashChange);
boot();
setInterval(() => { refresh().catch(() => {}); }, REFRESH_MS);
