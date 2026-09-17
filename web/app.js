"use strict";
const $ = (s) => document.querySelector(s);
const state = { key: "", role: null, user: null, timer: null, lang: detectLang() };

// ---------- i18n ----------
const I18N = {
  zh: {
    title: "TokenBalancer · Qwen Token Plan 团队版平衡代理",
    keyPh: "粘贴代理 Key 或 admin Key",
    connect: "连接",
    welcome: "请先在上方粘贴 Key 并连接。",
    footer: "数据每 30 秒自动刷新 · 剩余量基于本地用量记账，可用“对账”校准",
    whoAdmin: "管理员",
    whoUser: "用户: ",
    connectFail: "连接失败: ",
    badKey: "Key 无效或已吊销",
    left: "剩余",
    tagExhausted: "额度耗尽",
    tagDisabled: "已禁用",
    tagReconciled: "已对账 ",
    btnEnable: "启用",
    btnDisable: "禁用",
    btnReconcile: "对账…",
    btnClear: "清除耗尽",
    btnPatch: "并发/额度…",
    cardEvents: "近14天 请求数",
    cardTokens: "近14天 Tokens",
    cardCredits: "近14天 Credits(估算)",
    hMyUsage: "我的用量（近14天）",
    hMyModels: "我的常用模型",
    hTeamHealth: "团队帐号状态（只读）",
    thModel: "模型",
    thEvents: "请求",
    thTokens: "Tokens",
    thCredits: "Credits(估算)",
    noData: "暂无数据",
    hAccounts: "帐号（{n}）",
    hTeamUsage: "团队用量（{d} 起）",
    hByUser: "按成员",
    hByModel: "按模型",
    none: "暂无",
    hMembers: "成员 Key（本月用量）",
    thName: "名称",
    thKey: "Key",
    thCreated: "创建",
    thAct: "",
    revoked: "已吊销",
    btnRevoke: "吊销",
    noMembers: "暂无成员 — 先创建",
    newnamePh: "新成员名称",
    btnNewUser: "创建代理 Key",
    newKey: "新 Key: ",
    shownOnce: "（仅此次显示）",
    confirmRevoke: "吊销该 Key？该成员将立即失去访问。",
    promptReconcile: "该帐号当前真实剩余量（从 Token Plan 控制台或 CLI 读取，单位与帐号一致）：",
    invalidNumber: "数字无效",
    promptMaxc: "最大并发数（当前 {n}）：",
    promptQuota: "月度额度（当前 {v}，留空不改）：",
  },
  en: {
    title: "TokenBalancer · Qwen Token Plan team-edition proxy",
    keyPh: "Paste your proxy key or admin key",
    connect: "Connect",
    welcome: "Paste a key above and connect to get started.",
    footer: "Auto-refreshes every 30 s · remaining amounts are self-accounted from proxied usage and can be calibrated via reconciliation",
    whoAdmin: "Admin",
    whoUser: "User: ",
    connectFail: "Connection failed: ",
    badKey: "Key is invalid or revoked",
    left: "left",
    tagExhausted: "Quota exhausted",
    tagDisabled: "Disabled",
    tagReconciled: "Reconciled ",
    btnEnable: "Enable",
    btnDisable: "Disable",
    btnReconcile: "Reconcile…",
    btnClear: "Clear exhausted",
    btnPatch: "Concurrency/quota…",
    cardEvents: "Requests (14d)",
    cardTokens: "Tokens (14d)",
    cardCredits: "Credits est. (14d)",
    hMyUsage: "My usage (last 14 days)",
    hMyModels: "My top models",
    hTeamHealth: "Team account status (read-only)",
    thModel: "Model",
    thEvents: "Requests",
    thTokens: "Tokens",
    thCredits: "Credits est.",
    noData: "No data yet",
    hAccounts: "Accounts ({n})",
    hTeamUsage: "Team usage (since {d})",
    hByUser: "By member",
    hByModel: "By model",
    none: "None",
    hMembers: "Member keys (this month)",
    thName: "Name",
    thKey: "Key",
    thCreated: "Created",
    thAct: "",
    revoked: "Revoked",
    btnRevoke: "Revoke",
    noMembers: "No members yet — create one below",
    newnamePh: "New member name",
    btnNewUser: "Create proxy key",
    newKey: "New key: ",
    shownOnce: " (shown once only)",
    confirmRevoke: "Revoke this key? The member will lose access immediately.",
    promptReconcile: "The account's actual remaining amount now (read from the Token Plan console or CLI; same unit as the account):",
    invalidNumber: "Invalid number",
    promptMaxc: "Max concurrent requests (currently {n}):",
    promptQuota: "Monthly quota (currently {v}, leave empty to keep):",
  },
};
function detectLang() {
  try { const s = localStorage.getItem("tb_lang"); if (s === "en" || s === "zh") return s; } catch (e) {}
  return ((navigator.language || "zh").toLowerCase().startsWith("en")) ? "en" : "zh";
}
function t(k) {
  const d = I18N[state.lang] || I18N.zh;
  return d[k] !== undefined ? d[k] : (I18N.zh[k] !== undefined ? I18N.zh[k] : k);
}
function tf(key, vars) {
  let s = t(key);
  for (const k in vars) s = s.split("{" + k + "}").join(vars[k]);
  return s;
}
function applyStaticText() {
  document.documentElement.lang = state.lang === "en" ? "en" : "zh-CN";
  document.title = t("title");
  $("#key").placeholder = t("keyPh");
  $("#connect").textContent = t("connect");
  $("#langBtn").textContent = state.lang === "zh" ? "EN" : "中文";
  $("#footer").textContent = t("footer");
  if (!state.role) $("#view").innerHTML = '<div class="muted">' + t("welcome") + "</div>";
}
function toggleLang() {
  state.lang = state.lang === "zh" ? "en" : "zh";
  try { localStorage.setItem("tb_lang", state.lang); } catch (e) {}
  applyStaticText();
  updateWho();
  if (state.role) refresh();
}
function updateWho() {
  if (!state.role) return;
  $("#who").textContent = state.role === "admin" ? t("whoAdmin") : t("whoUser") + (state.user ? state.user.name : state.user.id);
}

async function api(path, opts = {}) {
  const r = await fetch(path, {
    method: opts.method || "GET",
    headers: Object.assign(
      { "Authorization": "Bearer " + state.key },
      opts.body ? { "Content-Type": "application/json" } : {}
    ),
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (r.status === 401) { logout(); throw new Error(t("badKey")); }
  if (!r.ok) { const txt = await r.text(); throw new Error(txt || r.status); }
  return r.json();
}

function logout() {
  state.role = null; state.user = null;
  clearInterval(state.timer); state.timer = null;
  $("#who").textContent = "";
  applyStaticText();
}

// Escape HTML so user/admin-controllable strings are never injected as markup.
function esc(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

async function connect() {
  state.key = $("#key").value.trim();
  if (!state.key) return;
  try {
    const w = await api("/api/whoami");
    state.role = w.role; state.user = w.user || null;
    updateWho();
    refresh();
    clearInterval(state.timer);
    state.timer = setInterval(refresh, 30000);
  } catch (e) { $("#who").textContent = t("connectFail") + e.message; }
}

function refresh() { if (state.role === "admin") renderAdmin(); else renderUser(); }

// ---------- helpers ----------
function fmtNum(n) {
  if (n >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (n >= 1e4) return (n / 1e3).toFixed(1) + "k";
  return String(Math.round(n));
}
function bar(pct, cls) {
  const p = Math.max(0, Math.min(100, (pct || 0) * 100));
  return '<div class="bar ' + (cls || "") + '"><div style="width:' + p + '%"></div></div>';
}
function dailyBars(daily, key) {
  const days = [];
  const now = new Date();
  for (let i = 13; i >= 0; i--) {
    const d = new Date(now); d.setDate(d.getDate() - i);
    days.push(d.toISOString().slice(0, 10));
  }
  const map = {}; (daily || []).forEach(d => { map[d.date] = d; });
  const max = Math.max(1, ...days.map(dt => (map[dt] || {})[key] || 0));
  return days.map(dt => {
    const v = (map[dt] || {})[key] || 0;
    const h = Math.round((v / max) * 100);
    return '<div class="day" title="' + dt + ": " + fmtNum(v) + '"><div class="daybar" style="height:' + Math.max(2, h) + '%"></div><span>' + dt.slice(5) + "</span></div>";
  }).join("");
}
function acctCard(a, withActions) {
  const unitLabel = a.unit === "Credits" ? "credits" : "tokens";
  const quotaText = a.quota ? " · " + t("left") + " " + fmtNum(a.remaining) + " / " + fmtNum(a.quota) : " · " + t("left") + " " + fmtNum(a.remaining);
  return (
    '<div class="acct ' + (a.disabled ? "disabled" : "") + ' ' + (a.exhausted ? "exhausted" : "") + '">' +
    '<div class="acct-head"><span>' + esc(a.label || a.id) + ' <span class="muted">(' + esc(a.region || "cn") + " · " + unitLabel + ")</span></span>" +
    '<span class="muted">' + a.in_flight + "/" + a.max_concurrent + " · " + Math.round(a.remaining_pct * 100) + "%</span></div>" +
    bar(a.remaining_pct, a.exhausted ? "bad" : (a.disabled ? "off" : "ok")) +
    '<div class="tags">' +
      (a.exhausted ? '<span class="tag bad">' + t("tagExhausted") + "</span>" : "") +
      (a.disabled ? '<span class="tag off">' + t("tagDisabled") + "</span>" : "") +
      (a.reconciled_at ? '<span class="tag ok">' + t("tagReconciled") + String(a.reconciled_at).slice(0, 10) + "</span>" : "") +
    "</div>" +
    (withActions ? (
      '<div class="actions">' +
      '<button data-act="toggle" data-id="' + esc(a.id) + '">' + (a.disabled ? t("btnEnable") : t("btnDisable")) + "</button>" +
      '<button data-act="reconcile" data-id="' + esc(a.id) + '">' + t("btnReconcile") + "</button>" +
      '<button data-act="clear" data-id="' + esc(a.id) + '">' + t("btnClear") + "</button>" +
      '<button data-act="patch" data-id="' + esc(a.id) + '">' + t("btnPatch") + "</button>" +
      "</div>"
    ) : "") +
    "</div>"
  );
}

// ---------- user view ----------
async function renderUser() {
  let usage, health;
  try {
    [usage, health] = await Promise.all([api("/api/me/usage"), api("/api/accounts/health")]);
  } catch (e) { $("#view").innerHTML = '<div class="muted">' + esc(e.message) + "</div>"; return; }
  const totals = usage.totals || {};
  const cards =
    '<div class="cards">' +
    '<div class="card"><div class="card-label">' + t("cardEvents") + '</div><div class="card-value">' + fmtNum(totals.events || 0) + "</div></div>" +
    '<div class="card"><div class="card-label">' + t("cardTokens") + '</div><div class="card-value">' + fmtNum(totals.tokens || 0) + "</div></div>" +
    '<div class="card"><div class="card-label">' + t("cardCredits") + '</div><div class="card-value">' + fmtNum(totals.credits || 0) + "</div></div>" +
    "</div>" +
    '<section><h2>' + t("hMyUsage") + "</h2>" +
    (usage.daily && usage.daily.length ? '<div class="bars">' + dailyBars(usage.daily, "tokens") + "</div>" : '<div class="muted">' + t("noData") + "</div>") +
    (usage.top_models && usage.top_models.length ?
      '<table><tr><th>' + t("thModel") + "</th><th>" + t("thEvents") + '</th><th>' + t("thTokens") + '</th><th>' + t("thCredits") + "</th></tr>" +
      usage.top_models.map(m =>
        "<tr><td>" + esc(m.model) + "</td><td>" + m.events + "</td><td>" + fmtNum(m.tokens) + "</td><td>" + fmtNum(m.credits) + "</td></tr>"
      ).join("") + "</table>"
    : '<div class="muted">' + t("noData") + "</div>") +
    "</section>" +
    '<section><h2>' + t("hTeamHealth") + "</h2>" +
    '<div class="accts">' + (health.accounts || []).map(a => acctCard(a, false)).join("") + "</div>" +
    "</section>";
  $("#view").innerHTML = cards;
}

// ---------- admin view ----------
async function renderAdmin() {
  let accounts, analysis, users;
  try {
    [accounts, analysis, users] = await Promise.all([
      api("/api/admin/accounts"),
      api("/api/admin/analytics?from=" + new Date(Date.now() - 14 * 86400000).toISOString().slice(0, 10)),
      api("/api/admin/users"),
    ]);
  } catch (e) { $("#view").innerHTML = '<div class="muted">' + esc(e.message) + "</div>"; return; }
  const from = analysis.from ? new Date(analysis.from * 1000).toISOString().slice(0, 10) : "";
  const byUser = (analysis.by_user || []).map(u =>
    "<tr><td>" + esc(u.key) + "</td><td>" + fmtNum(u.events) + "</td><td>" + fmtNum(u.tokens) + "</td><td>" + fmtNum(u.credits) + "</td></tr>"
  ).join("");
  const byModel = (analysis.by_model || []).map(m =>
    "<tr><td>" + esc(m.key) + "</td><td>" + fmtNum(m.events) + "</td><td>" + fmtNum(m.tokens) + "</td><td>" + fmtNum(m.credits) + "</td></tr>"
  ).join("");
  const usersRows = (users.users || []).map(u =>
    "<tr><td>" + esc(u.name) + '</td><td class="mono">' + esc(u.key) + "</td><td>" + String(u.created_at).slice(0, 10) +
    "</td><td>" + fmtNum(u.month_events) + "</td><td>" + fmtNum(u.month_tokens) + "</td><td>" + fmtNum(u.month_credits) + "</td>" +
    "<td>" + (u.revoked ? '<span class="muted">' + t("revoked") + "</span>" : '<button data-revoke="' + esc(u.key) + '">' + t("btnRevoke") + "</button>") + "</td></tr>"
  ).join("");
  const html =
    '<section><h2>' + tf("hAccounts", { n: (accounts.accounts || []).length }) + "</h2>" +
    '<div class="accts">' + (accounts.accounts || []).map(a => acctCard(a, true)).join("") + "</div></section>" +
    '<section><h2>' + tf("hTeamUsage", { d: from }) + "</h2>" +
    (analysis.daily && analysis.daily.length ? '<div class="bars">' + dailyBars(analysis.daily, "tokens") + "</div>" : '<div class="muted">' + t("noData") + "</div>") +
    '<div class="two-col"><div><h3>' + t("hByUser") + "</h3>" +
      (byUser ? '<table><tr><th>' + t("thName") + "</th><th>" + t("thEvents") + '</th><th>' + t("thTokens") + '</th><th>' + t("thCredits") + "</th></tr>" + byUser + "</table>" : '<div class="muted">' + t("none") + "</div>") +
    '</div><div><h3>' + t("hByModel") + "</h3>" +
      (byModel ? '<table><tr><th>' + t("thModel") + "</th><th>" + t("thEvents") + '</th><th>' + t("thTokens") + '</th><th>' + t("thCredits") + "</th></tr>" + byModel + "</table>" : '<div class="muted">' + t("none") + "</div>") +
    "</div></section>" +
    '<section><h2>' + t("hMembers") + "</h2>" +
    (usersRows ?
      '<table><tr><th>' + t("thName") + "</th><th>" + t("thKey") + "</th><th>" + t("thCreated") + "</th><th>" + t("thEvents") + '</th><th>' + t("thTokens") + '</th><th>' + t("thCredits") + "</th><th>" + t("thAct") + "</th></tr>" + usersRows + "</table>"
    : '<div class="muted">' + t("noMembers") + "</div>") +
    '<div class="row"><input id="newname" placeholder="' + t("newnamePh") + '"><button id="newuser">' + t("btnNewUser") + '</button><span id="newkey" class="muted"></span></div>' +
    "</section>";
  $("#view").innerHTML = html;
  document.querySelectorAll("[data-act]").forEach(b => { b.onclick = async () => {
    const id = b.dataset.id, act = b.dataset.act;
    if (act === "toggle") {
      const a = (accounts.accounts || []).find(x => x.id === id);
      await api("/api/admin/accounts/" + id, { method: "PATCH", body: { disabled: !a.disabled } });
      refresh();
    } else if (act === "clear") {
      await api("/api/admin/accounts/" + id + "/clear-exhausted", { method: "POST" });
      refresh();
    } else if (act === "reconcile") {
      const v = prompt(t("promptReconcile"), "");
      if (v === null) return;
      const n = parseFloat(v);
      if (isNaN(n) || n < 0) { alert(t("invalidNumber")); return; }
      await api("/api/admin/accounts/" + id + "/reconcile", { method: "POST", body: { remaining: n } });
      refresh();
    } else if (act === "patch") {
      const a = (accounts.accounts || []).find(x => x.id === id);
      const mc = prompt(tf("promptMaxc", { n: a.max_concurrent }), String(a.max_concurrent));
      if (mc === null) return;
      const body = {};
      const mcN = parseInt(mc, 10);
      if (!isNaN(mcN) && mcN >= 1) body.max_concurrent = mcN;
      const q = prompt(tf("promptQuota", { v: a.quota ?? "?" }), "");
      if (q !== null && q !== "") body.monthly_quota = parseFloat(q);
      await api("/api/admin/accounts/" + id, { method: "PATCH", body });
      refresh();
    }
  }; });
  document.querySelectorAll("[data-revoke]").forEach(b => { b.onclick = async () => {
    if (!confirm(t("confirmRevoke"))) return;
    await api("/api/admin/users/" + b.dataset.revoke + "/revoke", { method: "POST" });
    refresh();
  }; });
  $("#newuser").onclick = async () => {
    const name = $("#newname").value.trim() || "member";
    const r = await api("/api/admin/users", { method: "POST", body: { name } });
    $("#newkey").textContent = t("newKey") + r.key + t("shownOnce");
    refresh();
  };
}

$("#connect").onclick = connect;
$("#langBtn").onclick = toggleLang;
$("#key").addEventListener("keydown", e => { if (e.key === "Enter") connect(); });
applyStaticText();
try {
  const k = localStorage.getItem("tb_key");
  if (k) { $("#key").value = k; state.key = k; connect(); }
} catch (e) {}
