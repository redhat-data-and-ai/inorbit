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
  lineageDownType: "all",
  lineageModal: "",
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
  const aliases = {
    snowpipe_db: "Snowpipe",
    fivetran_db: "Fivetran",
    external_table: "External table",
  };
  const aliased = aliases[raw.toLowerCase()];
  if (aliased) return aliased;
  const u = raw.toUpperCase();
  if (u === "OK") return "OK";
  if (u === "GREEN") return "Trusted";
  if (u === "YELLOW") return "Caution";
  if (u === "RED") return "At risk";
  return raw.replace(/_/g, " ").toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
}

function pipelineCountLabel(list) {
  const n = (list || []).length;
  return n === 1 ? "1 DAG" : n + " DAGs";
}

function connectorCountLabel(list) {
  const n = (list || []).length;
  if (!n) return "";
  return n === 1 ? "1 connector" : n + " connectors";
}

function pipelineSummary(rows, connectors) {
  const list = rows || [];
  const conns = connectors || [];
  const extra = connectorCountLabel(conns);
  const failedConns = conns.filter((c) => String(c.status || "").toUpperCase() === "FAILED").length;
  const runningConns = conns.filter((c) => String(c.status || "").toUpperCase() === "RUNNING").length;
  const pausedConns = conns.filter((c) => c.paused).length;
  const failedDags = list.filter((d) => String(d.dag_status).toUpperCase() === "FAILED" || d.dag_overall_status === "FAILED").length;
  const runningDags = list.filter((d) => String(d.dag_status).toUpperCase() === "RUNNING").length;
  const pausedDags = list.filter((d) => d.dag_is_paused).length;
  const failed = failedDags + failedConns;
  const running = runningDags + runningConns;
  if (!list.length) {
    if (!extra) return { label: "No pipelines", cls: "muted", detail: "0 DAGs" };
    if (failedConns) return { label: "Failed", cls: "bad", detail: failedConns + " failed · " + extra };
    if (runningConns) return { label: "Running", cls: "warn", detail: runningConns + " running · " + extra };
    if (pausedConns === conns.length) return { label: "Paused", cls: "bad", detail: extra };
    return { label: "Connectors", cls: "ok", detail: extra };
  }
  const n = pipelineCountLabel(list);
  const detail = extra ? n + " · " + extra : n;
  if (failed) return { label: "Failed", cls: "bad", detail: failed + " failed · " + detail };
  if (running) return { label: "Running", cls: "warn", detail: running + " running · " + detail };
  if (pausedDags === list.length && (!conns.length || pausedConns === conns.length)) return { label: "Paused", cls: "bad", detail };
  const worst = list.find((d) => d.dag_overall_status === "AT_RISK") ? "At risk" : "Trusted";
  return { label: worst, cls: worst === "Trusted" ? "ok" : "bad", detail };
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

function isFivetran(d) {
  return String((d && d.pipeline_type) || "").toUpperCase() === "FIVETRAN";
}

function openLabel(d) {
  return isFivetran(d) ? "Open in Fivetran" : "Open in Astro";
}

function fivetranLink(href, label, compact) {
  if (!href) return compact ? "" : "—";
  const text = label || "Open in Fivetran";
  return `<a class="astro-open" href="${esc(href)}" target="_blank" rel="noopener noreferrer" title="${esc(text)}">${esc(text)}</a>`;
}

function astroLink(d, compact) {
  const href = d.astro_url;
  if (!href) return compact ? "" : "—";
  const label = openLabel(d);
  const a = `<a class="astro-open" href="${esc(href)}" target="_blank" rel="noopener noreferrer" title="Open ${esc(d.dag_id)} in ${isFivetran(d) ? "Fivetran" : "Astro"}">${esc(label)}</a>`;
  if (compact) return a;
  return `${a}<div class="url-path">${esc(href)}</div>`;
}

function dagNameCell(d) {
  const name = esc(d.dag_id);
  if (!d.astro_url) return name;
  return `<a class="dag-link" href="${esc(d.astro_url)}" target="_blank" rel="noopener noreferrer" title="${esc(openLabel(d))}">${name}</a>`;
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
      <div class="pipe-id">${dagNameCell(d)}</div>
      <div class="pipe-meta">${esc(d.astro_deployment_name || (isFivetran(d) ? "Fivetran" : "Astro"))} · ${esc(d.pipeline_type || "DAG")}${badges ? " " + badges : ""}</div>
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
      <div><span>${isFivetran(d) ? "Fivetran" : "Astro"}</span>${astroLink(d)}</div>
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

function collapseTrend(pts) {
  const out = [];
  (pts || []).forEach((p) => {
    const s = Number(p.health_score);
    if (Number.isNaN(s)) return;
    const last = out[out.length - 1];
    if (last && Math.abs((Number(last.health_score) || 0) - s) < 0.05) {
      out[out.length - 1] = p;
      return;
    }
    out.push(p);
  });
  return out;
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

function trendLinePath(pts) {
  if (!pts.length) return "";
  return pts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
}

function healthChart(points) {
  const win = state.trendWindow || "30d";
  const raw = collapseTrend(filterTrend(points, win)).filter((p) => {
    const s = Number(p.health_score);
    return !Number.isNaN(s);
  });
  const max = raw.reduce((m, p) => Math.max(m, Number(p.health_score) || 0), 0);
  const pts = max > 10 ? raw.filter((p) => (Number(p.health_score) || 0) > 0) : raw;
  const windows = [["7d", "7d"], ["30d", "30d"], ["90d", "3m"], ["all", "All"]].map(([key, label]) =>
    `<button type="button" class="io-trend-seg-btn${win === key ? " active" : ""}" data-trend-window="${key}" aria-pressed="${win === key ? "true" : "false"}">${label}</button>`
  ).join("");
  const dir = trendDirection(pts);
  const head = `<div class="io-trend-head">
      <h3 class="io-trend-title">Score trend <span class="io-trend-dir ${dir.cls}">— ${esc(dir.text)}</span></h3>
      <div class="io-trend-seg" role="group" aria-label="Score trend window">${windows}</div>
    </div>`;
  if (!pts.length) {
    return `<div class="io-score-card io-trend-card">${head}<p class="empty">Trend fills from the warehouse daily health snapshot.</p></div>`;
  }
  const w = 720, h = 210, padL = 44, padR = 8, padT = 8, padB = 28;
  const plotL = padL, plotR = w - padR, plotT = padT, plotB = h - padB;
  const now = Date.now();
  const ms = trendWindowMs(win);
  const xs = pts.map((p, i) => {
    const t = Date.parse(p.t);
    return Number.isNaN(t) ? i : t;
  });
  const ys = pts.map((p) => Number(p.health_score) || 0);
  let minX = ms ? now - ms : Math.min(...xs);
  let maxX = ms ? now : Math.max(now, ...xs);
  if (maxX === minX) maxX = minX + 1;
  const yAt = (score) => plotT + (1 - score / 100) * (plotB - plotT);
  const xAt = (t) => plotL + ((t - minX) / (maxX - minX)) * (plotR - plotL);
  const linePts = [];
  const firstX = xAt(xs[0]);
  if (firstX - plotL > 1) {
    linePts.push([plotL, yAt(ys[0])]);
  }
  pts.forEach((_, i) => linePts.push([xAt(xs[i]), yAt(ys[i])]));
  const lastX = linePts[linePts.length - 1][0];
  if (plotR - lastX > 1) {
    linePts.push([plotR, yAt(ys[ys.length - 1])]);
  }
  const line = trendLinePath(linePts);
  const lo = Math.min(...ys);
  const hi = Math.max(...ys);
  const leftLabel = win === "all" ? "Start" : (win === "90d" ? "3m ago" : win + " ago");
  const ticks = [100, 50, 0].map((n) => {
    const y = yAt(n);
    return `<line class="tick" x1="${plotL - 4}" x2="${plotL}" y1="${y}" y2="${y}"></line>
      <text class="axis-label" x="${plotL - 8}" y="${y + 4}">${n}</text>`;
  }).join("");
  return `<div class="io-score-card io-trend-card">
    ${head}
    <svg class="io-trend-svg" viewBox="0 0 ${w} ${h}" role="img" aria-label="Health score over time">
      <rect class="plot" x="${plotL}" y="${plotT}" width="${plotR - plotL}" height="${plotB - plotT}"></rect>
      ${ticks}
      <path class="line" d="${line}"></path>
      <text class="axis-label x" x="${plotL}" y="${h - 6}">${esc(leftLabel)}</text>
      <text class="axis-label x end" x="${plotR}" y="${h - 6}">Today</text>
    </svg>
    <div class="io-trend-range">
      <span>Lowest <strong class="lo">${esc(fmtScore(lo))}%</strong></span>
      <span>Highest <strong class="hi">${esc(fmtScore(hi))}%</strong></span>
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
  if (/pre[-_ ]?prod|staging|(^|[^a-z0-9])stage([^a-z0-9]|$)|(^|[^a-z0-9])uat([^a-z0-9]|$)/.test(v)) return 2;
  if (v.includes("prod") && !/non[-_ ]?prod/.test(v)) return 3;
  if (/sandbox|(^|[^a-z0-9])sbx([^a-z0-9]|$)|(^|[^a-z0-9])dev([^a-z0-9]|$)|(^|[^a-z0-9])qa([^a-z0-9]|$)/.test(v)) return 1;
  return 0;
}

function dagEnv(d) {
  const r = envRank(d && d.astro_deployment_name);
  if (r === 3) return "production";
  if (r === 2) return "preprod";
  if (r === 1) return "sandbox";
  return "unknown";
}

function connEnv(c) {
  return dagEnv({ astro_deployment_name: c && c.group_name });
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

function connectorsForEnv(list, env) {
  const rows = list || [];
  if (!env || env === "all") return rows;
  return rows.filter((c) => connEnv(c) === env);
}

function snapEnvs(s) {
  const seen = {};
  (s.pipeline || []).forEach((d) => { seen[dagEnv(d)] = true; });
  (s.connectors || []).forEach((c) => { seen[connEnv(c)] = true; });
  return ["production", "preprod", "sandbox", "unknown"].filter((k) => seen[k]).map(envLabel);
}

function hasEnv(s, env) {
  if (!env || env === "all") return true;
  return (s.pipeline || []).some((d) => dagEnv(d) === env) || (s.connectors || []).some((c) => connEnv(c) === env);
}

function envKind(s, env) {
  if (env && env !== "all") return envLabel(env);
  const names = (s.pipeline || []).map((d) => String(d.astro_deployment_name || "").trim())
    .concat((s.connectors || []).map((c) => String(c.group_name || "").trim()))
    .filter(Boolean);
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

function lineageNodeInEnv(n, env) {
  if (!env || env === "all") return true;
  if (!(n && (n.connector_service || n.connector_type))) return true;
  if (!n.group_name) return true;
  return connEnv({ group_name: n.group_name }) === env;
}

function lineageForEnv(lin, env) {
  lin = lin || {};
  const up = (lin.upstream_sources || []).filter((n) => lineageNodeInEnv(n, env));
  return Object.assign({}, lin, {
    upstream_sources: up,
    upstream_count: up.length,
  });
}

function scopedSnap(s, env) {
  return Object.assign({}, s, {
    pipeline: pipelineForEnv(s.pipeline, env),
    connectors: connectorsForEnv(s.connectors, env),
    lineage: lineageForEnv(s.lineage, env),
  });
}

function productEnvSelect(snap) {
  const envs = snapEnvs(snap);
  if (!envs.length) return "";
  const cur = preferredProductEnv(snap);
  return catalogSelect("product-env", "Environment", envs.map((e) => {
    const n = pipelineForEnv(snap.pipeline, e.kind).length + connectorsForEnv(snap.connectors, e.kind).length;
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
  const pipe = pipelineSummary(s.pipeline, s.connectors);
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
      pipelineSummary(pipelineForEnv(s.pipeline, state.envFilter), connectorsForEnv(s.connectors, state.envFilter)).label,
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
    state.lineageModal = "";
    state.lineageDownType = "all";
  }
  const key = route.view + ":" + (route.id || "") + ":" + (route.tab || "");
  if (key === lastRouteKey) return;
  lastRouteKey = key;
  state.tableFilter = "";
  state.sourceFilter = "all";
  state.runFilter = "all";
  state.freshnessFilter = "all";
  state.qualityStatusFilter = "all";
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
  const pipe = pipelineSummary(snap.pipeline, snap.connectors);
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
  if (!d) return { title: "No pipeline", cls: "muted" };
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
  if (!d) return { text: "No pipeline", cls: "muted" };
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
    ? `<a class="io-pfresh-link astro-open" href="${esc(d.astro_url)}" target="_blank" rel="noopener noreferrer" title="${esc(openLabel(d))}">${esc(openLabel(d))}</a>`
    : "";
  const upHref = hrefBase ? hrefBase + "?tab=lineage" : "";
  const infoTitle = (isFivetran(d) ? "Primary connector " : "Primary DAG ") + d.dag_id + ". Data as of is last successful run; last updated is when InOrbit scored the pipeline.";
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

function connectorStripHTML(snap, hrefBase) {
  const conns = snap.connectors || [];
  if (!conns.length) return "";
  const paused = conns.filter((c) => c.paused).length;
  const failed = conns.filter((c) => String(c.status || "").toUpperCase() === "FAILED").length;
  const running = conns.filter((c) => String(c.status || "").toUpperCase() === "RUNNING").length;
  const types = [];
  const seen = {};
  conns.forEach((c) => {
    const name = c.service_name || pretty(c.service);
    if (name && !seen[name]) {
      seen[name] = true;
      types.push(name);
    }
  });
  const typeBit = types.length ? " (" + types.slice(0, 4).join(", ") + (types.length > 4 ? ", …" : "") + ")" : "";
  const bits = ["This product has " + connectorCountLabel(conns) + typeBit];
  if (failed) bits.push(failed === 1 ? "1 failed" : failed + " failed");
  if (running) bits.push(running === 1 ? "1 running" : running + " running");
  if (paused) bits.push(paused === 1 ? "1 paused" : paused + " paused");
  const text = bits.join(" · ") + ". Status is in the connector table. Failed or unhealthy connectors deduct health like a failed Airflow DAG.";
  const href = hrefBase ? hrefBase + "?tab=lineage" : "";
  const body = href ? `<a href="${esc(href)}">${esc(text)} View lineage</a>` : esc(text);
  return `<div class="io-conn-strip">${body}</div>${connectorTableHTML(conns)}`;
}

function connectorTableHTML(conns) {
  const list = conns || [];
  if (!list.length) return "";
  return `
    <h3 class="pf-v5-c-title pf-m-md io-conn-table-title">Fivetran connectors <span class="chip-count">${list.length}</span></h3>
    <div class="io-table-wrap">
      <table class="pf-v5-c-table pf-m-compact pipe-table" role="grid">
        <thead><tr>
          <th>Connector</th>
          <th>Type</th>
          <th>Status</th>
          <th>Last sync</th>
          <th>Open</th>
        </tr></thead>
        <tbody>
          ${list.map((c) => {
            const name = c.schema || c.connection_id || "—";
            const type = c.service_name || pretty(c.service);
            const paused = c.paused ? `<span class="flag muted">Paused</span>` : "";
            const last = String(c.status || "").toUpperCase() === "FAILED" ? (c.failed_at || c.completed_at) : (c.succeeded_at || c.completed_at);
            const href = c.dashboard_url;
            const nameHTML = href
              ? `<a class="dag-link" href="${esc(href)}" target="_blank" rel="noopener noreferrer" title="Open ${esc(name)} in Fivetran">${esc(name)}</a>`
              : esc(name);
            const open = fivetranLink(href, "Open in Fivetran", true) || "—";
            const err = c.error_message ? `<div class="pipe-note">${esc(c.error_message)}</div>` : "";
            return `<tr>
              <td><div class="pipe-id">${nameHTML}</div>${paused}${err}</td>
              <td>${esc(type)}</td>
              <td>${label(c.status || "UNKNOWN")}</td>
              <td>${fmtWhen(last)}</td>
              <td class="col-astro">${open}</td>
            </tr>`;
          }).join("")}
        </tbody>
      </table>
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
    "Fivetran " + (parseDate(m.last_fivetran_poll) ? fmtRelative(m.last_fivetran_poll) : "—"),
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
    (s.connectors || []).forEach((c) => { kinds[connEnv(c)] = true; });
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
    const pipe = pipelineSummary(view.pipeline, view.connectors);
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
  if (t.includes("fivetran") || t.includes("snowpipe") || t.includes("external") || n.connector_service) return "source";
  if (t === "service_account") return "service_account";
  if (t === "consumer_group") return "consumer_group";
  return t || "other";
}

function linIcon(kind) {
  const svg = (inner) => `<svg class="io-lin-ico" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.8">${inner}</svg>`;
  if (kind === "db") return svg(`<ellipse cx="12" cy="6" rx="7" ry="3"/><path d="M5 6v12c0 1.7 3.1 3 7 3s7-1.3 7-3V6"/>`);
  if (kind === "person") return svg(`<circle cx="12" cy="8" r="3.2"/><path d="M5 20c1.4-4.2 4-6.2 7-6.2s5.6 2 7 6.2"/>`);
  if (kind === "cubes") return svg(`<rect x="4" y="9" width="9" height="9" rx="1.5"/><rect x="11" y="5" width="9" height="9" rx="1.5"/>`);
  if (kind === "spark") return svg(`<path d="M12 3.5l1.2 5.2L18.5 10 13.2 11.3 12 16.5 10.8 11.3 5.5 10l5.3-1.3z"/>`);
  if (kind === "bolt") return svg(`<path d="M13 3L6 13h5l-1 8 8-11h-5z" fill="currentColor" stroke="none"/>`);
  if (kind === "wand") return svg(`<path d="M4 20L14.5 9.5M16 4v3M20 8h-3M18.5 5.5l-1.5 1.5M12 8.5l1.5-1.5"/>`);
  if (kind === "link") return svg(`<path d="M10 13.5l-1.2 1.2a3.2 3.2 0 01-4.5-4.5L6.5 8M14 10.5l1.2-1.2a3.2 3.2 0 014.5 4.5L17.5 16M9 12h6"/>`);
  if (kind === "folder") return svg(`<path d="M3 8.5V7.2c0-.4.3-.7.7-.7h5.2l1.6 1.5h9.8c.4 0 .7.3.7.7V18c0 .6-.5 1-1 1H4c-.6 0-1-.4-1-1V8.5z"/>`);
  if (kind === "caret") return svg(`<path d="M8 10l4 4 4-4"/>`);
  return connectorMark("");
}

function lineageTrustChip(status, score) {
  const st = String(status || "").trim();
  if (!st || st.toUpperCase() === "N/A") return "";
  const cls = healthClass(st);
  let text = pretty(st);
  if (score != null && score !== "" && cls !== "muted") {
    const n = Number(score);
    if (!Number.isNaN(n)) text += " " + (Number.isInteger(n) ? String(n) : n.toFixed(2).replace(/0+$/, "").replace(/\.$/, "")) + "%";
  }
  const mark = cls === "ok"
    ? `<svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><circle cx="6" cy="6" r="6" fill="currentColor"/><path d="M3.4 6.1l1.7 1.7 3.4-3.6" fill="none" stroke="#fff" stroke-width="1.4" stroke-linecap="round"/></svg>`
    : "";
  return `<span class="io-lin-trust ${cls}">${mark}${esc(text)}</span>`;
}

function connectorMark(service) {
  const s = String(service || "").toLowerCase();
  let inner = `<circle cx="12" cy="12" r="8"/><path d="M8 12h8"/>`;
  if (s.includes("sheet")) {
    inner = `<rect x="4" y="4" width="16" height="16" rx="2"/><path d="M4 10h16M10 4v16"/>`;
  } else if (s === "s3" || s.includes("amazon_s3") || s.includes("s3_")) {
    inner = `<ellipse cx="12" cy="8" rx="8" ry="3.5"/><path d="M4 8v8c0 2 3.6 3.5 8 3.5s8-1.5 8-3.5V8"/>`;
  } else if (s.includes("salesforce")) {
    inner = `<ellipse cx="12" cy="12" rx="9" ry="6"/>`;
  } else if (s.includes("postgres") || s.includes("aurora") || s.includes("mysql") || s.includes("sql") || s.includes("snowflake") || s.includes("mongo") || s.includes("oracle")) {
    inner = `<ellipse cx="12" cy="7" rx="7" ry="3"/><path d="M5 7v10c0 1.7 3.1 3 7 3s7-1.3 7-3V7"/>`;
  }
  return `<svg class="lineage-mark" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.8">${inner}</svg>`;
}

function lineageSourceCard(n) {
  const name = n.name || n.schema || "—";
  const kind = nodeKind(n);
  const isDP = kind === "data_product";
  const typed = n.connector_service || n.connector_type;
  const typeLabel = typed ? (n.connector_type || pretty(n.connector_service)) : "";
  const nConn = Number(n.connection_count) || 0;
  const nPaused = Number(n.paused_count) || 0;
  const chips = [];
  if (typed && nConn) chips.push(`<span class="io-src-chip">${esc(nConn === 1 ? "1 connection" : nConn + " connections")}</span>`);
  if (nPaused > 0) chips.push(`<span class="io-src-chip io-src-chip-paused">${esc(nPaused === 1 ? "1 paused" : nPaused + " paused")}</span>`);
  const trust = lineageTrustChip(n.status, n.health_score);
  const openFt = !isDP && n.dashboard_url ? fivetranLink(n.dashboard_url, "Open in Fivetran", true) : "";
  let meta;
  if (typed) {
    meta = `<div class="io-src-meta"><span class="io-src-type">${connectorMark(n.connector_service)}<span>${esc(typeLabel)}</span></span>${chips.join("")}${trust}${openFt}</div>`;
  } else if (isDP && trust) {
    meta = `<div class="io-src-meta">${trust}</div>`;
  } else {
    meta = `<div class="io-src-meta"><span class="io-src-type io-src-type-generic">${linIcon("db")}<span>${esc(pretty(n.type || "source"))}</span></span>${openFt}</div>`;
  }
  const inner = `<span class="io-lin-glyph">${linIcon("db")}</span>
    <div class="io-src-body">
      <div class="io-src-title">${esc(name)}${isDP ? `<span class="io-lin-spark">${linIcon("spark")}</span>` : ""}</div>
      ${meta}
    </div>`;
  if (isDP && name) {
    return `<a class="io-src-card io-src-card-link" href="#/data-product/${encodeURIComponent(name)}?tab=lineage">${inner}</a>`;
  }
  return `<article class="io-src-card">${inner}</article>`;
}

function lineageConsumerCard(n) {
  const name = n.name || "—";
  const kind = nodeKind(n);
  const isDP = kind === "data_product";
  const icon = isDP ? linIcon("spark") : kind === "consumer_group" ? linIcon("cubes") : linIcon("person");
  const trust = lineageTrustChip(n.status, n.health_score);
  const typeChip = `<span class="io-lin-type">${esc(pretty(n.type || "consumer"))}</span>`;
  const inner = `<span class="io-lin-glyph">${icon}</span>
    <div class="io-src-body">
      <div class="io-src-title">${esc(name)}</div>
      <div class="io-src-meta">${isDP && trust ? trust : typeChip}</div>
    </div>`;
  if (isDP && name) {
    return `<a class="io-src-card io-src-card-link" href="#/data-product/${encodeURIComponent(name)}?tab=lineage">${inner}</a>`;
  }
  return `<article class="io-src-card">${inner}</article>`;
}

function lineageArrow() {
  return `<div class="lineage-arrow" aria-hidden="true">
    <svg viewBox="0 0 40 16" width="40" height="16" fill="none" stroke="#8a8d90" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M2 8h32M28 3l8 5-8 5"/></svg>
  </div>`;
}

function lineageStack(title, nodes, moreKey, cardFn, extra, total) {
  const shown = nodes.slice(0, 3);
  const more = Math.max(0, nodes.length - shown.length);
  const moreText = moreKey === "up"
    ? `+${more} more sources — View all`
    : `+${more} more consumers — View all`;
  const moreBtn = more > 0
    ? `<button type="button" class="lineage-more" data-lineage-more="${esc(moreKey)}">${esc(moreText)}</button>`
    : "";
  const count = total != null ? total : nodes.length;
  if (!nodes.length) {
    return `<div class="io-lin-colhead"><div class="io-lin-k">${esc(title)} <span class="io-lin-count">${count}</span></div>${extra || ""}</div><p class="muted-cell">No ${esc(title.toLowerCase())}</p>`;
  }
  return `<div class="io-lin-colhead"><div class="io-lin-k">${esc(title)} <span class="io-lin-count">${count}</span></div>${extra || ""}</div>${shown.map(cardFn).join("")}${moreBtn}`;
}

function lineageDatabasesHTML(snap) {
  const marts = ((snap.lineage || {}).mart_schemas || []).map((s) => String(s || "").trim()).filter(Boolean);
  if (!marts.length) return "";
  const p = snap.data_product || {};
  const db = (p.validation && p.validation.database) || (p.dbt_logs && p.dbt_logs.database) || p.validation_database || "";
  const dbLabel = db || ((p.data_product_name || "product") + "_DB");
  const items = marts.map((s) => `<li class="io-mart-item">${linIcon("caret")}${linIcon("folder")}<span>${esc(s)}</span></li>`).join("");
  return `<section class="io-lin-dbs">
    <h2 class="io-lin-dbs-title">${linIcon("db")} Data Product Databases</h2>
    <p class="io-lin-dbs-sub">Marts available for <strong>${esc(dbLabel)}</strong> data consumers</p>
    <ul class="io-mart-grid">${items}</ul>
  </section>`;
}

function lineageModalHTML(title, nodes, cardFn) {
  return `<div class="io-lin-modal" role="presentation">
    <div class="io-lin-modal-card" role="dialog" aria-modal="true" aria-label="${esc(title)}">
      <div class="io-lin-modal-head">
        <h3>${esc(title)} <span class="chip-count">${nodes.length}</span></h3>
        <button type="button" class="io-lin-modal-x" data-lineage-close aria-label="Close">×</button>
      </div>
      <div class="io-lin-modal-list">${nodes.map(cardFn).join("") || `<p class="muted-cell">None</p>`}</div>
    </div>
  </div>`;
}

function renderLineage(snap, base) {
  const lin = snap.lineage || {};
  const up = lin.upstream_sources || [];
  const down = lin.downstream_consumers || [];
  const sources = up.filter((n) => nodeKind(n) === "source" || nodeKind(n) === "data_product");
  const typed = sources.filter((n) => n.connector_service || n.connector_type);
  const restUp = sources.filter((n) => !(n.connector_service || n.connector_type));
  const orderedSources = typed.concat(restUp);
  const downType = state.lineageDownType || "all";
  const consumers = down
    .filter((n) => downType === "all" || nodeKind(n) === downType)
    .slice()
    .sort((a, b) => {
      const ra = nodeKind(a) === "service_account" ? 0 : 1;
      const rb = nodeKind(b) === "service_account" ? 0 : 1;
      return ra - rb;
    });
  const downCounts = {
    all: down.length,
    data_product: down.filter((n) => nodeKind(n) === "data_product").length,
    service_account: down.filter((n) => nodeKind(n) === "service_account").length,
    consumer_group: down.filter((n) => nodeKind(n) === "consumer_group").length,
  };
  const typeSelect = catalogSelect("lineage-down-type", "Consumer type", [
    ["all", "Type"],
    ["data_product", "Data product", downCounts.data_product],
    ["service_account", "Service account", downCounts.service_account],
    ["consumer_group", "Consumer group", downCounts.consumer_group],
  ], downType);
  const h = snap.health || {};
  const productName = (snap.data_product || {}).data_product_name || "";
  const highImpact = /high/i.test(String(lin.blast_radius_score || ""));
  const srcCard = (n) => lineageSourceCard(n);
  const downCard = (n) => lineageConsumerCard(n);
  const modal = state.lineageModal === "up"
    ? lineageModalHTML("Upstream sources", orderedSources, srcCard)
    : state.lineageModal === "down"
      ? lineageModalHTML("Downstream consumers", consumers, downCard)
      : "";
  return `
    ${lineageDatabasesHTML(snap)}
    <h2 class="io-lin-viz-title">Lineage Visualization <span class="io-lin-viz-link" title="Lineage overview">${linIcon("link")}</span></h2>
    <section class="io-lin-overview">
      <h2 class="io-lin-overview-title">${linIcon("wand")} Data Lineage Overview</h2>
      <div class="lineage-flow">
        <div class="lineage-col up">${lineageStack("Upstream sources", orderedSources, "up", srcCard)}</div>
        ${lineageArrow()}
        <div class="io-lin-product">
          <div class="io-lin-product-icons">${linIcon("cubes")}${highImpact ? `<span class="io-lin-bolt">${linIcon("bolt")}</span>` : ""}</div>
          <div class="io-lin-product-name">${esc(productName)}</div>
          ${lineageTrustChip(h.status, h.health_score)}
        </div>
        ${lineageArrow()}
        <div class="lineage-col down">${lineageStack("Downstream consumers", consumers, "down", downCard, typeSelect, down.length)}</div>
      </div>
      ${modal}
    </section>`;
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
    const dagEmptyHint = "Live Airflow lists configured deployments. The warehouse pipeline mart covers catalog products those polls miss. Source-aligned Fivetran connectors are summarized above and shown on Lineage.";
    let rows = all.filter((d) => {
      if (state.runFilter !== "all" && dagRunBucket(d) !== state.runFilter) return false;
      if (state.freshnessFilter !== "all" && dagFreshBucket(d) !== state.freshnessFilter) return false;
      return rowMatch(q, [d.dag_id, d.astro_deployment_name, d.dag_status, d.dag_overall_status, d.dag_freshness_status, d.astro_url, d.external_run_id, d.trigger_type, d.dag_frequency_display, d.dag_overall_status_description, d.is_primary_dag ? "primary" : "", d.dag_is_paused ? "paused" : "", d.dag_is_custom ? "custom" : ""]);
    });
    rows = applyTableSort(rows, dagGetters());
    const dagFilterBar = tableToolbar(rows.length, all.length, "Filter pipelines by id or destination",
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
      ? emptyRow(cols, "No active pipelines matched this product.", dagEmptyHint)
      : emptyRow(cols, "No pipelines match these filters.", "Clear the run or SLA chips, or the search box.");
    const customCount = all.filter((d) => d.dag_is_custom).length;
    body = `
      ${primaryFreshnessHTML(view, base)}
      ${connectorStripHTML(view, base)}
      <div class="io-stat-row">
        <div class="io-stat"><strong>${all.length}</strong><span>Pipelines</span></div>
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
            <th>Open</th>
          </tr></thead>
          <tbody>
            ${rows.map((d) => pipelineRows(d)).join("") || noDags}
          </tbody>
        </table>
      </div>
      <p class="sub">${esc(pipelineCountLabel(all))} active${connectorCountLabel(view.connectors) ? " · " + esc(connectorCountLabel(view.connectors)) : ""}${customCount ? " · " + customCount + " custom (shown, excluded from scores)" : ""} · Status, SLA, last/next run on the table. Expand a row for interval, age, and URL. <strong>Open in Astro</strong> is on each DAG row. <strong>Open in Fivetran</strong> is on each connector row.</p>`;
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
    body = renderLineage(view, base);
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
      <p class="sub">Health Score v2 from live validation/Elementary and pipeline checks (Astro and Fivetran). Open Pipeline for Status, SLA, and DAG links. Source-aligned connectors appear as Lineage chips.</p>`;
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
  if (el.dataset.view) state.listView = el.dataset.view;
}

function bindApp() {
  const app = document.getElementById("app");
  if (!app || app.dataset.bound === "1") return;
  app.dataset.bound = "1";
  app.addEventListener("click", (e) => {
    if (e.target.closest("a[target=\"_blank\"]")) return;
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
    const moreLin = e.target.closest("[data-lineage-more]");
    if (moreLin) {
      e.preventDefault();
      state.lineageModal = moreLin.dataset.lineageMore;
      render();
      return;
    }
    if (e.target.closest(".io-lin-modal-x") || e.target.classList.contains("io-lin-modal")) {
      e.preventDefault();
      state.lineageModal = "";
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
    else if (id === "product-env") {
      state.productEnv = e.target.value;
      state.lineageModal = "";
    }
    else if (id === "lineage-down-type") {
      state.lineageDownType = e.target.value;
      state.lineageModal = "";
    }
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
  app.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && state.lineageModal) {
      state.lineageModal = "";
      render();
    }
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
