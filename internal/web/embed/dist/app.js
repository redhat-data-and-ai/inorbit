const API = "";
const REFRESH_MS = 15000;

const state = {
  meta: null,
  snaps: [],
  filter: "",
  tableFilter: "",
  sourceFilter: "all",
  healthFilter: "all",
  listSort: "name",
  runFilter: "all",
  freshnessFilter: "all",
  qualityStatusFilter: "all",
  tableSort: "",
  tableSortDir: "asc",
  expanded: {},
};

let lastRouteKey = "";

function parseRoute() {
  const raw = (location.hash || "#/").replace(/^#/, "") || "/";
  const u = new URL(raw, "http://ui.local");
  const parts = u.pathname.replace(/\/+$/, "").split("/").filter(Boolean);
  if (parts[0] === "data-product" && parts[1]) {
    return { view: "detail", id: decodeURIComponent(parts[1]), tab: u.searchParams.get("tab") || "overview" };
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
  if (raw.toUpperCase() === "OK") return "OK";
  return raw.replace(/_/g, " ").toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
}

function pipelineSummary(rows) {
  const list = rows || [];
  if (!list.length) return { label: "No DAGs", cls: "muted", detail: "0 DAGs" };
  const failed = list.filter((d) => String(d.dag_status).toUpperCase() === "FAILED" || d.dag_overall_status === "FAILED").length;
  const running = list.filter((d) => String(d.dag_status).toUpperCase() === "RUNNING").length;
  const paused = list.filter((d) => d.dag_is_paused).length;
  if (failed) return { label: "Failed", cls: "bad", detail: failed + " failed · " + list.length + " DAGs" };
  if (running) return { label: "Running", cls: "warn", detail: running + " running · " + list.length + " DAGs" };
  if (paused === list.length) return { label: "Paused", cls: "bad", detail: list.length + " DAGs" };
  const worst = list.find((d) => d.dag_overall_status === "AT_RISK") ? "At risk" : "Trusted";
  return { label: worst, cls: worst === "Trusted" ? "ok" : "bad", detail: list.length + " DAGs" };
}

function qualitySummary(rows) {
  const list = (rows || []).filter((c) => c.source_type === "VALIDX" || c.source_type === "DBT_TEST");
  const failed = list.filter((c) => c.status === "FAILED").length;
  const warn = list.filter((c) => c.status === "WARNING").length;
  if (!list.length) return { label: "No checks", cls: "muted", detail: "Not configured" };
  if (failed) return { label: failed + " failed", cls: "bad", detail: list.length + " checks" };
  if (warn) return { label: warn + " warning", cls: "warn", detail: list.length + " checks" };
  return { label: "Passing", cls: "ok", detail: list.length + " checks" };
}

function label(status) {
  return `<span class="label ${healthClass(status)}">${esc(pretty(status))}</span>`;
}

function labelAs(status, text) {
  return `<span class="label ${healthClass(status)}">${esc(text)}</span>`;
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

function fmtMins(v) {
  if (v == null || v === "" || Number.isNaN(Number(v))) return "—";
  return Math.round(Number(v)) + "m";
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
  const badges = [
    d.is_primary_dag ? `<span class="flag">Primary</span>` : "",
    d.dag_is_paused ? `<span class="flag muted">Paused</span>` : "",
    d.dag_is_custom ? `<span class="flag muted">Custom</span>` : "",
  ].filter(Boolean).join("");
  const main = `<tr class="pipe-row ${open ? "is-open" : ""}">
    <td class="col-expand">
      <button type="button" class="expand" data-expand="${esc(key)}" aria-expanded="${open}" aria-label="${open ? "Collapse" : "Expand"} ${esc(d.dag_id)}">
        <span aria-hidden="true">${open ? "▾" : "▸"}</span>
      </button>
    </td>
    <td class="pipe-name">
      <div class="pipe-id">${dagNameCell(d)}</div>
      <div class="pipe-meta">${esc(d.astro_deployment_name || "Astro")} · ${esc(d.pipeline_type || "DAG")}${badges ? " " + badges : ""}</div>
      ${d.astro_url ? astroLink(d, true) : ""}
    </td>
    <td class="pipe-status">
      ${label(d.dag_status)}
      <div class="pipe-meta">${esc(d.trigger_type || "—")}</div>
      ${labelAs(d.dag_overall_status, overallPretty(d.dag_overall_status))}
      ${note ? `<div class="pipe-note">${esc(note)}</div>` : ""}
    </td>
    <td>${label(d.dag_pipeline_sla_status)}</td>
    <td>${fmtRunClock(lastRunAt(d))}</td>
    <td>${fmtRunClock(d.dag_next_expected_at)}</td>
    <td class="pipe-timing">
      <div><span>Frequency</span> ${esc(d.dag_frequency_display || "—")}</div>
      <div><span>Duration</span> ${esc(fmtDuration(d))}</div>
      <div><span>SLA</span> ${esc(fmtMins(d.dag_sla_minutes))}</div>
    </td>
    <td class="pipe-rel">${reliabilityCell(d)}</td>
  </tr>`;
  if (!open) return main;
  return main + `<tr class="pipe-detail"><td colspan="8">
    <div class="detail-grid">
      <div><span>Astro</span>${astroLink(d)}</div>
      <div><span>Run id</span>${esc(d.external_run_id || "—")}</div>
      <div><span>Last success</span>${fmtWhen(d.last_successful_at)}</div>
      <div><span>Started</span>${fmtWhen(d.dag_started_at)}</div>
      <div><span>Freshness</span>${label(d.dag_freshness_status)}</div>
      <div><span>Overall</span>${label(d.dag_overall_status)}</div>
      ${d.error_message ? `<div class="span2"><span>Error</span>${esc(d.error_message)}</div>` : ""}
    </div>
  </td></tr>`;
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

function donut(pct, title, sub, cls) {
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
      <div class="donut-center"><strong>${esc(title)}</strong></div>
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
  return `<div class="chart-card">
    <h3>Dimension scores</h3>
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
  </div>`;
}

function healthChart(points) {
  const pts = points || [];
  if (!pts.length) {
    return `<div class="chart-card"><h3>Health trend</h3><p class="empty">Trend fills as the SLA clock samples the live score.</p></div>`;
  }
  const w = 720, h = 200, padL = 42, padR = 18, padT = 18, padB = 36;
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
  const line = xy.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const area = `${line} L${xy[xy.length - 1][0].toFixed(1)},${h - padB} L${xy[0][0].toFixed(1)},${h - padB} Z`;
  const last = pts[pts.length - 1];
  const ticks = [0, 60, 80, 100];
  const xLabels = [0, Math.floor((pts.length - 1) / 2), pts.length - 1].filter((i, idx, arr) => arr.indexOf(i) === idx);
  return `<div class="chart-card">
    <h3>Health trend</h3>
    <svg class="spark" viewBox="0 0 ${w} ${h}" role="img" aria-label="Health score over time in local timezone">
      <rect class="band ok" x="${padL}" y="${yAt(100)}" width="${w - padL - padR}" height="${yAt(80) - yAt(100)}"></rect>
      <rect class="band warn" x="${padL}" y="${yAt(80)}" width="${w - padL - padR}" height="${yAt(60) - yAt(80)}"></rect>
      <rect class="band bad" x="${padL}" y="${yAt(60)}" width="${w - padL - padR}" height="${yAt(0) - yAt(60)}"></rect>
      ${ticks.map((t) => `<line class="guide" x1="${padL}" x2="${w - padR}" y1="${yAt(t)}" y2="${yAt(t)}"></line>
        <text class="axis-label" x="${padL - 6}" y="${yAt(t) + 3}">${t}</text>`).join("")}
      <path class="area" d="${area}"></path>
      <path class="line" d="${line}"></path>
      <circle class="dot" cx="${xy[xy.length - 1][0].toFixed(1)}" cy="${xy[xy.length - 1][1].toFixed(1)}" r="4"></circle>
      ${xLabels.map((i) => `<text class="axis-label x" x="${xy[i][0].toFixed(1)}" y="${h - 10}">${esc(fmtTimeShort(pts[i].t))}</text>`).join("")}
    </svg>
    <p class="sub">Last ${fmtScore(last.health_score)} · ${esc(pretty(last.status))} · ${esc(fmtTime(last.t))} · ${pts.length} sample${pts.length === 1 ? "" : "s"} · ${esc(localZone())}</p>
  </div>`;
}

function overviewCharts(snap) {
  const h = snap.health || {};
  const pipe = snap.pipeline || [];
  const dq = dqChecks(snap.quality);
  const score = h.total_checks ? Number(h.health_score) || 0 : 0;
  const hs = healthClass(h.status);
  const failedDQ = dq.filter((c) => c.status === "FAILED").length;
  const warnDQ = dq.filter((c) => c.status === "WARNING").length;
  const passedDQ = dq.filter((c) => c.status === "PASSED").length;
  const failedPipe = pipe.filter((d) => String(d.dag_status).toUpperCase() === "FAILED" || d.dag_overall_status === "FAILED").length;
  const running = pipe.filter((d) => String(d.dag_status).toUpperCase() === "RUNNING").length;
  const paused = pipe.filter((d) => d.dag_is_paused).length;
  const okPipe = Math.max(0, pipe.length - failedPipe - running - paused);
  return `
    <div class="chart-grid hero">
      ${donut(score / 100, fmtScore(h.total_checks ? h.health_score : null), pretty(h.status) + " health", hs)}
      ${segmentDonut([
        { value: okPipe, label: "Success", cls: "ok" },
        { value: running, label: "Running", cls: "info" },
        { value: paused, label: "Paused", cls: "warn" },
        { value: failedPipe, label: "Failed", cls: "bad" },
      ], String(pipe.length), "DAG mix")}
      ${segmentDonut([
        { value: passedDQ, label: "Passed", cls: "ok" },
        { value: warnDQ, label: "Warning", cls: "warn" },
        { value: failedDQ, label: "Failed", cls: "bad" },
      ], String(dq.length), "Quality mix")}
    </div>
    ${dimBars(h)}
    ${healthChart(snap.trend)}`;
}

function dqChecks(rows) {
  return (rows || []).filter((c) => c.source_type === "VALIDX" || c.source_type === "DBT_TEST");
}

function rowMatch(q, parts) {
  if (!q) return true;
  return parts.some((p) => String(p || "").toLowerCase().includes(q));
}

function tableToolbar(count, total, placeholder, extra) {
  return `<div class="table-toolbar">
    <div class="toolbar-filters">${extra || ""}</div>
    <div class="toolbar-search">
      <input class="search" id="table-filter" type="search" aria-label="${esc(placeholder)}" placeholder="${esc(placeholder)}" value="${esc(state.tableFilter)}" />
      <span class="sub shown-count">${count} of ${total} shown</span>
    </div>
  </div>`;
}

function chipGroup(options, current, attr) {
  return `<div class="chips" role="group">
    ${options.map(([id, title, n]) => {
      const count = n == null ? "" : ` <span class="chip-count">${n}</span>`;
      return `<button type="button" class="chip ${current === id ? "active" : ""}" data-${attr}="${esc(id)}" aria-pressed="${current === id}">${esc(title)}${count}</button>`;
    }).join("")}
  </div>`;
}

function emptyState(title, hint) {
  return `<div class="empty"><p class="empty-title">${esc(title)}</p>${hint ? `<p class="empty-hint">${esc(hint)}</p>` : ""}</div>`;
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
  const inner = `<div class="k">${esc(k)}</div><div class="v">${v}</div><div class="s">${s}</div>`;
  if (!href) return `<div class="tile">${inner}</div>`;
  return `<a class="tile clickable-tile" href="${href}">${inner}</a>`;
}

function tilesHTML(snap, hrefBase) {
  const pipe = pipelineSummary(snap.pipeline);
  const qual = qualitySummary(snap.quality);
  const h = snap.health || {};
  const f = snap.freshness || {};
  const score = h.total_checks ? fmtScore(h.health_score) : "—";
  return `
    <div class="tile-row">
      ${tile("Health", esc(score), label(h.status))}
      ${tile("Freshness", esc(pretty(f.freshness_status)), label(f.freshness_status), hrefBase ? hrefBase + "?tab=freshness" : "")}
      ${tile("Pipeline", esc(pipe.label), esc(pipe.detail), hrefBase ? hrefBase + "?tab=pipeline" : "")}
      ${tile("Quality", esc(qual.label), esc(qual.detail), hrefBase ? hrefBase + "?tab=quality" : "")}
    </div>`;
}

function fetchBanner(extra) {
  const m = state.meta || {};
  const warnings = m.warnings || [];
  const warn = warnings.length
    ? `<div class="alert-banner" role="alert">${warnings.map((w) => `<p>${esc(w)}</p>`).join("")}</div>`
    : "";
  const bits = [
    extra,
    "Airflow " + (parseDate(m.last_astro_run_poll) ? fmtRelative(m.last_astro_run_poll) : "—"),
    "Quality " + (parseDate(m.last_quality_poll) ? fmtRelative(m.last_quality_poll) : "—"),
    "Scores " + (parseDate(m.last_clock_tick) ? fmtRelative(m.last_clock_tick) : "—"),
  ].filter((x) => x && !String(x).endsWith("—"));
  return `${warn}<p class="fetch-banner" title="${esc("Airflow " + fmtTime(m.last_astro_run_poll) + " · Quality " + fmtTime(m.last_quality_poll) + " · " + localZone())}">Showing live snapshot · ${bits.map(esc).join(" · ")}</p>`;
}

function productName(s) {
  const p = s.data_product || {};
  return p.data_product_name || p.data_product_id || "";
}

function renderList() {
  const all = state.snaps || [];
  const healthCounts = { trusted: 0, caution: 0, at_risk: 0, other: 0 };
  all.forEach((s) => { healthCounts[productHealthBucket((s.health || {}).status)] += 1; });
  const q = state.filter.trim().toLowerCase();
  let snaps = all.filter((s) => {
    const bucket = productHealthBucket((s.health || {}).status);
    if (state.healthFilter !== "all" && bucket !== state.healthFilter) return false;
    if (!q) return true;
    const p = s.data_product || {};
    return [p.data_product_name, p.data_product_id, p.owner_team, p.dp_type].some((v) => String(v || "").toLowerCase().includes(q));
  });
  snaps = [...snaps].sort((a, b) => {
    const na = productName(a);
    const nb = productName(b);
    if (state.listSort === "health") return (Number((b.health || {}).health_score) || 0) - (Number((a.health || {}).health_score) || 0) || cmp(na, nb);
    if (state.listSort === "age") return (Number((b.freshness || {}).current_delay_mins) || 0) - (Number((a.freshness || {}).current_delay_mins) || 0) || cmp(na, nb);
    if (state.listSort === "dags") return ((b.pipeline || []).length) - ((a.pipeline || []).length) || cmp(na, nb);
    return cmp(na, nb);
  });
  const cards = snaps.map((s) => {
    const p = s.data_product || {};
    const id = p.data_product_id;
    const h = s.health || {};
    const f = s.freshness || {};
    const score = h.total_checks ? Number(h.health_score) || 0 : 0;
    const tone = healthClass(h.status);
    const updated = fmtRelative(s.updated_at) || fmtTimeShort(s.updated_at);
    return `
      <a class="card clickable tone-${tone}" href="#/data-product/${encodeURIComponent(id)}">
        <div class="card-head">
          <div>
            <h2>${esc(p.data_product_name || id)}</h2>
            <div class="owner" title="${esc(fmtTime(s.updated_at))}">${esc(p.owner_team || p.dp_type || "")}${p.dp_type && p.owner_team ? " · " + esc(p.dp_type) : ""} · updated ${esc(updated)}</div>
          </div>
          ${label(h.status)}
        </div>
        <div class="card-hero">${donut(score / 100, fmtScore(h.total_checks ? h.health_score : null), pretty(h.status), tone)}</div>
        ${tilesHTML(s)}
        <p class="card-foot">${esc((s.pipeline || []).length + " DAGs")} · freshness ${esc(pretty(f.freshness_status))}${f.current_delay_mins != null ? " · " + esc(fmtMins(f.current_delay_mins)) + " delay" : ""}</p>
      </a>`;
  }).join("");
  const empty = all.length === 0
    ? emptyState("No data products in the snapshot.", "Start the process with a config that lists data products, then wait for the first Airflow poll.")
    : emptyState("No products match these filters.", "Clear the search or health chip to see the full list.");
  return `
    <div class="toolbar">
      <div>
        <h1>Data products</h1>
        <p class="lede">Live health, freshness, and pipeline status from Airflow and warehouse quality checks.</p>
      </div>
      <input class="search" id="filter" type="search" aria-label="Filter by name or team" placeholder="Filter by name or team" value="${esc(state.filter)}" />
    </div>
    <div class="filter-bar">
      <div class="filter-cluster"><span class="filter-label">Health</span>${chipGroup([
        ["all", "All", all.length],
        ["trusted", "Trusted", healthCounts.trusted],
        ["caution", "Caution", healthCounts.caution],
        ["at_risk", "At risk", healthCounts.at_risk],
      ], state.healthFilter, "health")}</div>
      <div class="filter-cluster"><span class="filter-label">Sort</span>${chipGroup([
        ["name", "Name"],
        ["health", "Health"],
        ["age", "Data age"],
        ["dags", "DAGs"],
      ], state.listSort, "list-sort")}</div>
    </div>
    ${fetchBanner(snaps.length + " of " + all.length + " product" + (all.length === 1 ? "" : "s") + " · tiles read /v1/snapshots")}
    <div class="grid cards">${cards || empty}</div>`;
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

function renderDetail(route) {
  const snap = state.snaps.find((s) => s.data_product.data_product_id === route.id);
  if (!snap) {
    return `<p class="error">Data product not found: ${esc(route.id)}</p><p><a href="#/">Back to data products</a></p>`;
  }
  const p = snap.data_product;
  const tab = route.tab;
  const base = `#/data-product/${encodeURIComponent(route.id)}`;
  const tabs = ["overview", "freshness", "pipeline", "quality"].map((t) => {
    const href = `${base}?tab=${t}`;
    const title = t === "quality" ? "Quality signals" : pretty(t);
    return `<button class="tab ${tab === t ? "active" : ""}" data-href="${href}">${title}</button>`;
  }).join("");
  let body = "";
  if (tab === "pipeline" || tab === "freshness") {
    const q = state.tableFilter.trim().toLowerCase();
    const all = sortDAGs(snap.pipeline);
    const failed = all.filter((d) => dagRunBucket(d) === "failed").length;
    const running = all.filter((d) => dagRunBucket(d) === "running").length;
    const paused = all.filter((d) => dagRunBucket(d) === "paused").length;
    const success = all.filter((d) => dagRunBucket(d) === "success").length;
    const atRisk = all.filter((d) => dagFreshBucket(d) === "at_risk").length;
    const caution = all.filter((d) => dagFreshBucket(d) === "caution").length;
    const trusted = all.filter((d) => dagFreshBucket(d) === "trusted").length;
    const ok = success;
    const dagEmptyHint = "InOrbit matches Airflow tags to each product id and name from config (hyphens and underscores ignored). Add the DAG deployment under astro.deployments and the product under data_products.";
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
        ["all", "Any freshness", all.length],
        ["at_risk", "At risk", atRisk],
        ["caution", "Caution", caution],
        ["trusted", "Trusted", trusted],
      ], state.freshnessFilter, "fresh")
    );
    const cols = tab === "freshness" ? 10 : 8;
    const noDags = all.length === 0
      ? emptyRow(cols, "No DAGs matched this product.", dagEmptyHint)
      : emptyRow(cols, "No DAGs match these filters.", "Clear the run or freshness chips, or the search box.");
    const f = snap.freshness || {};
    if (tab === "freshness") {
      body = `
      <div class="tile-row" style="margin-bottom:16px">
        ${tile("Status", esc(pretty(f.freshness_status)), label(f.freshness_status))}
        ${tile("Data age", esc(fmtMins(f.current_delay_mins)), esc(f.status_reason || ""))}
        ${tile("Last success", fmtWhen(f.last_successful_at), "primary DAG")}
        ${tile("SLA", esc(fmtMins(f.sla_minutes)), "interval " + esc(fmtMins(f.expected_interval_mins)))}
      </div>
      ${dagFilterBar}
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>${sortBtn("dag", "DAG")}</th><th>Astro</th>
            <th>${sortBtn("freshness", "Freshness")}</th><th>${sortBtn("overall", "Overall")}</th>
            <th>${sortBtn("last", "Last success")}</th><th>${sortBtn("next", "Next expected")}</th>
            <th>${sortBtn("age", "Age")}</th><th>${sortBtn("interval", "Interval")}</th>
            <th>${sortBtn("slamins", "SLA")}</th><th>Flags</th>
          </tr></thead>
          <tbody>
            ${rows.map((d) => `
              <tr>
                <td class="check-name">${dagNameCell(d)}</td>
                <td class="astro-url">${astroLink(d)}</td>
                <td>${label(d.dag_freshness_status)}</td>
                <td>${label(d.dag_overall_status)}</td>
                <td>${fmtWhen(d.last_successful_at)}</td>
                <td>${fmtWhen(d.dag_next_expected_at)}</td>
                <td>${esc(fmtMins(d.dag_data_age_mins))}</td>
                <td>${esc(fmtMins(d.dag_expected_interval_mins))}</td>
                <td>${esc(fmtMins(d.dag_sla_minutes))}</td>
                <td>${[d.is_primary_dag ? "primary" : "", d.dag_is_paused ? "paused" : "", d.dag_is_custom ? "custom" : ""].filter(Boolean).join(" · ") || "—"}</td>
              </tr>`).join("") || noDags}
          </tbody>
        </table>
      </div>
      <p class="sub">${all.length} DAG${all.length === 1 ? "" : "s"} · hover a time for the exact local timestamp, then open the DAG in Astro to compare last / next run. ${atRisk} at risk · ${caution} caution</p>`;
    } else {
      body = `
      <div class="chart-grid hero">
        ${segmentDonut([
          { value: ok, label: "Success", cls: "ok" },
          { value: running, label: "Running", cls: "info" },
          { value: paused, label: "Paused", cls: "warn" },
          { value: failed, label: "Failed", cls: "bad" },
        ], `${ok}/${all.length || 0}`, "Run mix")}
        ${donut(all.length ? atRisk / all.length : 0, String(atRisk), "Freshness risk", atRisk ? "bad" : "ok")}
      </div>
      ${dagFilterBar}
      <div class="table-wrap">
        <table class="pipe-table">
          <thead><tr>
            <th class="col-expand"><span class="vh">Expand</span></th>
            <th>${sortBtn("dag", "Pipeline")}</th>
            <th>${sortBtn("run", "Status")}</th>
            <th>${sortBtn("sla", "SLA")}</th>
            <th>${sortBtn("last", "Last Run")}</th>
            <th>${sortBtn("next", "Next Run")}</th>
            <th>Timing</th>
            <th>${sortBtn("rel7", "Reliability")}</th>
          </tr></thead>
          <tbody>
            ${rows.map((d) => pipelineRows(d)).join("") || noDags}
          </tbody>
        </table>
      </div>
      <p class="sub">${all.length} DAG${all.length === 1 ? "" : "s"} including paused/custom · live Airflow latest run, duration, and 7/30/90d success rate from recent dag runs. <strong>Open in Astro</strong> is on each row (new tab). Expand for run id and last success.</p>`;
    }
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
    const vx = all.filter((c) => c.source_type === "VALIDX").length;
    const dbt = all.filter((c) => c.source_type === "DBT_TEST").length;
    const latestDbt = all.filter((c) => c.source_type === "DBT_TEST").map((c) => c.executed_at).filter(Boolean).sort().slice(-1)[0];
    const latestVx = all.filter((c) => c.source_type === "VALIDX").map((c) => c.executed_at).filter(Boolean).sort().slice(-1)[0];
    const emptyTitle = src === "DBT_TEST"
      ? "No Elementary rows in the latest dbt invocation for this product."
      : src === "VALIDX"
        ? "No validation rows in the latest warehouse run for this product."
        : "No validation or Elementary checks in the latest warehouse run.";
    const emptyHint = all.length
      ? "Clear the source or status chips, or the search box."
      : "Validation ingest is skipped when that table is missing. Elementary still loads from DBTLOGS when that table exists.";
    body = `
      <div class="chart-grid hero">
        ${segmentDonut([
          { value: passed, label: "Passed", cls: "ok" },
          { value: warn, label: "Warning", cls: "warn" },
          { value: failed, label: "Failed", cls: "bad" },
        ], `${passed}/${scoped.length || 0}`, "Latest run")}
        ${donut(all.length ? vx / Math.max(all.length, 1) : 0, String(vx), "Validation", "info")}
        ${donut(all.length ? dbt / Math.max(all.length, 1) : 0, String(dbt), "Elementary", "info")}
      </div>
      ${tableToolbar(rows.length, scoped.length, "Filter checks",
        chipGroup([
          ["all", "All", all.length],
          ["VALIDX", "Validation", vx],
          ["DBT_TEST", "Elementary", dbt],
        ], src, "source") +
        chipGroup([
          ["all", "Any status", scoped.length],
          ["FAILED", "Failed", failed],
          ["WARNING", "Warning", warn],
          ["PASSED", "Passed", passed],
        ], state.qualityStatusFilter, "qstatus")
      )}
      <div class="table-wrap">
        <table>
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
      <p class="sub">${vx} validation${latestVx ? " " + fmtRelative(latestVx) : ""} · ${dbt} Elementary${latestDbt ? " " + fmtRelative(latestDbt) : ""}. Latest warehouse invocation only.</p>`;
  } else {
    const h = snap.health || {};
    const f = snap.freshness || {};
    const cls = healthClass(h.status);
    body = `
      ${tilesHTML(snap, base)}
      ${overviewCharts(snap)}
      <p class="msg ${cls}">${esc(h.status_message || f.status_reason || "No status message")}</p>
      <p class="sub">Health Score v2 from live validation/Elementary + Astro freshness. Open the Freshness or Pipeline tab and click a DAG name to compare with Astro.</p>`;
  }
  const updatedRel = parseDate(snap.updated_at) ? fmtRelative(snap.updated_at) : "—";
  return `
    <p class="crumb"><a href="#/">Data products</a> / ${esc(p.data_product_name || p.data_product_id)}</p>
    <div class="title-row">
      <h1>${esc(p.data_product_name || p.data_product_id)}</h1>
      ${label((snap.health || {}).status)}
    </div>
    <p class="sub">${esc(p.owner_team || "")} ${p.dp_type ? "· " + esc(p.dp_type) : ""}</p>
    ${fetchBanner("Updated " + updatedRel)}
    <div class="tabs">${tabs}</div>
    ${body}`;
}

function renderMast() {
  const meta = document.getElementById("mast-meta");
  const m = state.meta || {};
  const sc = m.scale || {};
  const rss = fmtBytes(sc.estimated_rss_bytes_at_target);
  const json = fmtBytes(sc.estimated_snapshot_json_bytes_at_target);
  const air = parseDate(m.last_astro_run_poll) ? fmtRelative(m.last_astro_run_poll) : "—";
  const qual = parseDate(m.last_quality_poll) ? fmtRelative(m.last_quality_poll) : "—";
  meta.title = "Airflow " + fmtTime(m.last_astro_run_poll) + " · Quality " + fmtTime(m.last_quality_poll) + " · " + localZone();
  meta.innerHTML = `<div>${esc(m.mode || "unknown")} · ${m.data_product_count != null ? m.data_product_count + " products" : ""} · clock ${esc(String(m.clock_interval_seconds || 15))}s</div>
    <div>Airflow ${esc(air)} · Quality ${esc(qual)}</div>
    <div class="scale">${esc(String(sc.target_data_products || 200))}-DP estimate ${esc(json)} snapshots / ${esc(rss)} RSS</div>`;
}

function bindFilters(app) {
  const filter = document.getElementById("filter");
  if (filter) {
    filter.addEventListener("input", (e) => {
      state.filter = e.target.value;
      render();
      const again = document.getElementById("filter");
      if (again) {
        again.focus();
        again.setSelectionRange(state.filter.length, state.filter.length);
      }
    });
  }
  const tableFilter = document.getElementById("table-filter");
  if (tableFilter) {
    tableFilter.addEventListener("input", (e) => {
      state.tableFilter = e.target.value;
      render();
      const again = document.getElementById("table-filter");
      if (again) {
        again.focus();
        again.setSelectionRange(state.tableFilter.length, state.tableFilter.length);
      }
    });
  }
  app.querySelectorAll(".chip").forEach((el) => {
    el.addEventListener("click", () => {
      if (el.dataset.health) state.healthFilter = el.dataset.health;
      if (el.dataset.listSort) state.listSort = el.dataset.listSort;
      if (el.dataset.source) {
        state.sourceFilter = el.dataset.source;
        state.qualityStatusFilter = "all";
      }
      if (el.dataset.run) state.runFilter = el.dataset.run;
      if (el.dataset.fresh) state.freshnessFilter = el.dataset.fresh;
      if (el.dataset.qstatus) state.qualityStatusFilter = el.dataset.qstatus;
      render();
    });
  });
  app.querySelectorAll("[data-sort]").forEach((el) => {
    el.addEventListener("click", () => {
      const key = el.dataset.sort;
      if (state.tableSort === key) {
        state.tableSortDir = state.tableSortDir === "asc" ? "desc" : "asc";
      } else {
        state.tableSort = key;
        state.tableSortDir = (key === "last" || key === "age" || key === "executed" || key === "rel7") ? "desc" : "asc";
      }
      render();
    });
  });
  app.querySelectorAll("[data-expand]").forEach((el) => {
    el.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      const key = el.dataset.expand;
      state.expanded[key] = !state.expanded[key];
      render();
    });
  });
  app.querySelectorAll("a.astro-open, a.dag-link").forEach((el) => {
    el.addEventListener("click", (e) => e.stopPropagation());
  });
}

function render() {
  const route = parseRoute();
  resetViewState(route);
  const app = document.getElementById("app");
  app.innerHTML = route.view === "detail" ? renderDetail(route) : renderList();
  renderMast();
  app.querySelectorAll(".tab").forEach((el) => {
    el.addEventListener("click", () => go(el.dataset.href));
  });
  bindFilters(app);
}

async function refresh() {
  state.meta = await getJSON("/v1/meta");
  const route = parseRoute();
  if (route.view === "detail") {
    const base = "/v1/data-products/" + encodeURIComponent(route.id);
    const [snap, health, freshness, pipeline, quality, trend] = await Promise.all([
      getJSON(base),
      getJSON(base + "/health"),
      getJSON(base + "/freshness"),
      getJSON(base + "/pipeline"),
      getJSON(base + "/quality"),
      getJSON(base + "/health-trend"),
    ]);
    snap.health = health;
    snap.freshness = freshness;
    snap.pipeline = pipeline;
    snap.quality = quality;
    snap.trend = trend;
    state.snaps = [snap];
  } else {
    state.snaps = (await getJSON("/v1/snapshots")) || [];
  }
  render();
}

async function boot() {
  try {
    await refresh();
  } catch (err) {
    document.getElementById("app").innerHTML = `<p class="error">${esc(err.message)}</p>`;
  }
}

window.addEventListener("hashchange", boot);
boot();
setInterval(() => { boot(); }, REFRESH_MS);
