"use strict";
const $ = (s) => document.querySelector(s);
const state = { key: "", role: null, user: null, timer: null };

async function api(path, opts = {}) {
  const r = await fetch(path, {
    method: opts.method || "GET",
    headers: Object.assign(
      { "Authorization": "Bearer " + state.key },
      opts.body ? { "Content-Type": "application/json" } : {}
    ),
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (r.status === 401) { logout(); throw new Error("Key 无效或已吊销"); }
  if (!r.ok) { const t = await r.text(); throw new Error(t || r.status); }
  return r.json();
}

function logout() {
  state.role = null; state.user = null;
  clearInterval(state.timer); state.timer = null;
  $("#who").textContent = "";
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
    $("#who").textContent = state.role === "admin" ? "管理员" : "用户: " + (state.user ? state.user.name : state.user.id);
    refresh();
    clearInterval(state.timer);
    state.timer = setInterval(refresh, 30000);
  } catch (e) { $("#who").textContent = "连接失败: " + e.message; }
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
  const quotaText = a.quota ? " · 剩余 " + fmtNum(a.remaining) + " / " + fmtNum(a.quota) : " · 剩余 " + fmtNum(a.remaining);
  return (
    '<div class="acct ' + (a.disabled ? "disabled" : "") + ' ' + (a.exhausted ? "exhausted" : "") + '">' +
    '<div class="acct-head"><span>' + esc(a.label || a.id) + ' <span class="muted">(' + esc(a.region || "cn") + " · " + unitLabel + ")</span></span>" +
    '<span class="muted">' + a.in_flight + "/" + a.max_concurrent + " · " + Math.round(a.remaining_pct * 100) + "%</span></div>" +
    bar(a.remaining_pct, a.exhausted ? "bad" : (a.disabled ? "off" : "ok")) +
    '<div class="tags">' +
      (a.exhausted ? '<span class="tag bad">额度耗尽</span>' : "") +
      (a.disabled ? '<span class="tag off">已禁用</span>' : "") +
      (a.reconciled_at ? '<span class="tag ok">已对账 ' + String(a.reconciled_at).slice(0, 10) + "</span>" : "") +
    "</div>" +
    (withActions ? (
      '<div class="actions">' +
      '<button data-act="toggle" data-id="' + esc(a.id) + '">' + (a.disabled ? "启用" : "禁用") + "</button>" +
      '<button data-act="reconcile" data-id="' + esc(a.id) + '">对账…</button>' +
      '<button data-act="clear" data-id="' + esc(a.id) + '">清除耗尽</button>' +
      '<button data-act="patch" data-id="' + esc(a.id) + '">并发/额度…</button>' +
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
  const t = usage.totals;
  const cards =
    '<div class="cards">' +
    '<div class="card"><div class="card-label">近14天 请求数</div><div class="card-value">' + t.events + "</div></div>" +
    '<div class="card"><div class="card-label">近14天 Tokens</div><div class="card-value">' + fmtNum(t.tokens) + "</div></div>" +
    '<div class="card"><div class="card-label">近14天 Credits(估算)</div><div class="card-value">' + fmtNum(t.credits) + "</div></div>" +
    "</div>";
  const top = (usage.top_models || []).map(m =>
    "<tr><td>" + esc(m.model) + "</td><td>" + m.events + "</td><td>" + fmtNum(m.tokens) + "</td><td>" + fmtNum(m.credits) + "</td></tr>").join("");
  const accounts = health.accounts.map(a => acctCard(a, false)).join("");
  $("#view").innerHTML =
    cards +
    "<h2>我的用量（近14天）</h2>" +
    '<div class="chart">' + dailyBars(usage.daily, "tokens") + "</div>" +
    "<h2>我的常用模型</h2>" +
    '<table><thead><tr><th>模型</th><th>请求</th><th>Tokens</th><th>Credits(估算)</th></tr></thead><tbody>' +
    (top || '<tr><td colspan="4" class="muted">暂无数据</td></tr>') + "</tbody></table>" +
    "<h2>团队帐号状态（只读）</h2>" +
    '<div class="accts">' + accounts + "</div>";
}

// ---------- admin view ----------
async function renderAdmin() {
  let accts, users, ana;
  try {
    [accts, users, ana] = await Promise.all([api("/api/admin/accounts"), api("/api/admin/users"), api("/api/admin/analytics")]);
  } catch (e) { $("#view").innerHTML = '<div class="muted">' + esc(e.message) + "</div>"; return; }
  const accounts = accts.accounts.map(a => acctCard(a, true)).join("");
  const usersRows = users.users.map(u =>
    '<tr class="' + (u.revoked ? "revoked" : "") + '">' +
    "<td>" + esc(u.name) + '</td><td class="mono">' + esc(u.key) + "</td><td>" + u.created_at.slice(0, 10) + "</td>" +
    "<td>" + u.month_events + "</td><td>" + fmtNum(u.month_tokens) + "</td><td>" + fmtNum(u.month_credits) + "</td>" +
    "<td>" + (u.revoked ? "已吊销" : '<button data-revoke="' + esc(u.key) + '">吊销</button>') + "</td></tr>").join("");
  const row = (r) => "<tr><td>" + esc(r.key) + "</td><td>" + r.events + "</td><td>" + fmtNum(r.tokens) + "</td><td>" + fmtNum(r.credits) + "</td></tr>";
  const byUser = (ana.by_user || []).slice(0, 10).map(row).join("");
  const byModel = (ana.by_model || []).slice(0, 10).map(row).join("");
  const fromDay = ana.from ? new Date(ana.from * 1000).toISOString().slice(0, 10) : "";
  $("#view").innerHTML =
    "<h2>帐号（" + accts.accounts.length + "）</h2>" +
    '<div class="accts">' + accounts + "</div>" +
    "<h2>团队用量（" + fromDay + " 起）</h2>" +
    '<div class="chart tall">' + dailyBars(ana.daily, "tokens") + "</div>" +
    '<div class="cols"><div><h3>按成员</h3><table><thead><tr><th>成员</th><th>请求</th><th>Tokens</th><th>Credits</th></tr></thead><tbody>' +
    (byUser || '<tr><td colspan="4" class="muted">暂无</td></tr>') + "</tbody></table></div>" +
    '<div><h3>按模型</h3><table><thead><tr><th>模型</th><th>请求</th><th>Tokens</th><th>Credits</th></tr></thead><tbody>' +
    (byModel || '<tr><td colspan="4" class="muted">暂无</td></tr>') + "</tbody></table></div></div>" +
    "<h2>成员 Key（本月用量）</h2>" +
    '<table><thead><tr><th>名称</th><th>Key</th><th>创建</th><th>请求</th><th>Tokens</th><th>Credits</th><th></th></tr></thead><tbody>' +
    (usersRows || '<tr><td colspan="7" class="muted">暂无成员 — 先创建</td></tr>') + "</tbody></table>" +
    '<div class="create"><input id="newname" placeholder="新成员名称"><button id="newuser">创建代理 Key</button><span id="newkey" class="mono"></span></div>';
  bindAdmin(accts);
}

function bindAdmin(accts) {
  document.querySelectorAll("[data-act]").forEach(b => { b.onclick = async () => {
    const id = b.dataset.id;
    if (b.dataset.act === "toggle") {
      const a = accts.accounts.find(x => x.id === id);
      await api("/api/admin/accounts/" + id, { method: "PATCH", body: { disabled: !a.disabled } });
      refresh();
    } else if (b.dataset.act === "clear") {
      await api("/api/admin/accounts/" + id + "/clear-exhausted", { method: "POST" }); refresh();
    } else if (b.dataset.act === "reconcile") {
      const v = prompt("该帐号当前真实剩余量（从 Token Plan 控制台或 CLI 读取，单位与帐号一致）：");
      if (v === null) return;
      const remaining = parseFloat(v);
      if (isNaN(remaining) || remaining < 0) return alert("数字无效");
      await api("/api/admin/accounts/" + id + "/reconcile", { method: "POST", body: { remaining } });
      refresh();
    } else if (b.dataset.act === "patch") {
      const a = accts.accounts.find(x => x.id === id);
      const mc = prompt("最大并发数（当前 " + a.max_concurrent + "）：", String(a.max_concurrent));
      if (mc === null) return;
      const body = { max_concurrent: parseInt(mc, 10) };
      const q = prompt("月度额度（当前 " + (a.quota ?? "未知") + "，留空不改）：", "");
      if (q !== null && q !== "") body.monthly_quota = parseFloat(q);
      await api("/api/admin/accounts/" + id, { method: "PATCH", body });
      refresh();
    }
  }; });
  document.querySelectorAll("[data-revoke]").forEach(b => { b.onclick = async () => {
    if (!confirm("吊销该 Key？该成员将立即失去访问。")) return;
    await api("/api/admin/users/" + b.dataset.revoke + "/revoke", { method: "POST" });
    refresh();
  }; });
  $("#newuser").onclick = async () => {
    const name = $("#newname").value.trim() || "member";
    const r = await api("/api/admin/users", { method: "POST", body: { name } });
    $("#newkey").textContent = "新 Key: " + r.key + "（仅此次显示）";
    refresh();
  };
}

$("#connect").onclick = connect;
$("#key").addEventListener("keydown", e => { if (e.key === "Enter") connect(); });
try {
  const k = localStorage.getItem("tb_key");
  if (k) { $("#key").value = k; state.key = k; connect(); }
} catch (e) {}
