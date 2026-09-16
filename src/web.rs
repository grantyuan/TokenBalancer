// src/web.rs
use rand::Rng;
use std::sync::atomic::Ordering;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::proxy::AppState;

const INDEX_HTML: &[u8] = include_bytes!("../web/index.html");
const APP_JS: &[u8] = include_bytes!("../web/app.js");
const STYLES: &[u8] = include_bytes!("../web/styles.css");

pub async fn static_index() -> impl IntoResponse {
  ([("content-type", "text/html; charset=utf-8")], INDEX_HTML)
}
pub async fn static_app_js() -> impl IntoResponse {
  ([("content-type", "text/javascript; charset=utf-8")], APP_JS)
}
pub async fn static_styles() -> impl IntoResponse {
  ([("content-type", "text/css; charset=utf-8")], STYLES)
}

fn bearer(headers: &HeaderMap) -> Option<String> {
  headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
    .and_then(|v| v.strip_prefix("Bearer ")).map(|s| s.trim().to_string())
}

fn unauthorized() -> Response {
  let body = json!({"error": {"message": "Unauthorized", "code": "unauthorized"}}).to_string();
  Response::builder().status(StatusCode::UNAUTHORIZED)
    .header(header::CONTENT_TYPE, "application/json").body(axum::body::Body::from(body)).unwrap()
}

fn require_admin(st: &AppState, headers: &HeaderMap) -> Result<(), Response> {
  match bearer(headers) {
    Some(k) if k == st.runtime.admin_key() => Ok(()),
    _ => Err(unauthorized()),
  }
}

fn require_user(st: &AppState, headers: &HeaderMap) -> Result<(String, String), Response> {
  let k = bearer(headers).ok_or_else(unauthorized)?;
  if k == st.runtime.admin_key() { return Ok(("admin".into(), "admin".into())); }
  st.runtime.store().find_live_user_by_key(&k)
    .map_err(|_| unauthorized())?
    .map(|u| (u.id, u.name))
    .ok_or_else(unauthorized)
}

pub fn gen_user_key() -> String {
  const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let mut rng = rand::thread_rng();
  let s: String = (0..16).map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char).collect();
  format!("tbu_{s}")
}

pub async fn whoami(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  match bearer(&headers) {
    Some(k) if k == st.runtime.admin_key() => Ok(Json(json!({"role": "admin"}))),
    Some(k) => st.runtime.store().find_live_user_by_key(&k).map_err(|_| unauthorized())?
      .map(|u| Json(json!({"role": "user", "user": {"id": u.id, "name": u.name}})))
      .ok_or_else(unauthorized),
    None => Err(unauthorized()),
  }
}

pub async fn accounts_health(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_user(&st, &headers)?;
  let mut accounts = Vec::new();
  for a in st.runtime.iter_accounts() {
    let (_, pct) = st.runtime.remaining_of(&a);
    accounts.push(json!({
      "id": a.id, "label": a.label, "remaining_pct": pct,
      "in_flight": a.in_flight.load(Ordering::Acquire), "max_concurrent": a.max_concurrent.load(Ordering::Acquire),
      "disabled": a.disabled.load(Ordering::Acquire), "exhausted": a.exhausted.load(Ordering::Acquire),
    }));
  }
  Ok(Json(json!({"accounts": accounts})))
}

pub async fn me_usage(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  let (uid, _name) = require_user(&st, &headers)?;
  let now = Utc::now().timestamp();
  let since = now - 14 * 86_400;
  let (i, ca, o, cr, n) = st.runtime.store().totals_for(&uid, since).unwrap_or((0, 0, 0, 0.0, 0));
  let daily: Vec<Value> = st.runtime.store().daily_for(&uid, since, now + 86_400).unwrap_or_default()
    .into_iter().map(|(d, ev, di, do_, c)| json!({"date": d, "events": ev, "input": di, "output": do_, "tokens": di + do_, "credits": c})).collect();
  let models: Vec<Value> = st.runtime.store().top_models_for(&uid, since, 5).unwrap_or_default()
    .into_iter().map(|(m, ev, tok, c)| json!({"model": m, "events": ev, "tokens": tok, "credits": c})).collect();
  Ok(Json(json!({
    "totals": {"events": n, "input": i, "cached": ca, "output": o, "tokens": i + o, "credits": cr},
    "daily": daily, "top_models": models, "window_days": 14
  })))
}

pub async fn admin_accounts(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  let rows = st.runtime.store().list_accounts().unwrap_or_default();
  let mut accounts = Vec::new();
  for a in st.runtime.iter_accounts() {
    let row = rows.iter().find(|r| r.id == a.id);
    let (rem, pct) = st.runtime.remaining_of(&a);
    accounts.push(json!({
      "id": a.id, "label": a.label,
      "region": row.as_ref().map(|r| r.region.clone()).unwrap_or_default(),
      "unit": format!("{:?}", *a.unit.lock().unwrap()), "quota": *a.quota.lock().unwrap(),
      "remaining": rem, "remaining_pct": pct,
      "in_flight": a.in_flight.load(Ordering::Acquire), "max_concurrent": a.max_concurrent.load(Ordering::Acquire),
      "disabled": a.disabled.load(Ordering::Acquire), "exhausted": a.exhausted.load(Ordering::Acquire),
      "reconciled_at": row.and_then(|r| r.reconciled_at.clone()),
    }));
  }
  Ok(Json(json!({"accounts": accounts})))
}

pub async fn admin_users(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  let users = st.runtime.store().list_users().unwrap_or_default();
  let since = crate::config::month_start_unix();
  let by_user = st.runtime.store().group_since("user_id", since).unwrap_or_default();
  let mut out = Vec::new();
  for u in users {
    let (i, _ca, o, cr, n) = by_user.iter()
      .find(|(k, ..)| k == &u.id)
      .map(|(_, a, b, c, d, e)| (*a, *b, *c, *d, *e))
      .unwrap_or((0, 0, 0, 0.0, 0));
    out.push(json!({
      "id": u.id, "name": u.name, "key": u.key, "created_at": u.created_at, "revoked": u.revoked,
      "month_events": n, "month_tokens": i + o, "month_credits": cr,
    }));
  }
  Ok(Json(json!({"users": out, "window": "month-to-date"})))
}

#[derive(Deserialize)]
pub struct CreateUser { name: String }

pub async fn admin_create_user(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<CreateUser>) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  let key = gen_user_key();
  st.runtime.store().create_user(&key, &body.name).map_err(|_e| unauthorized())?;
  Ok(Json(json!({"key": key, "name": body.name})))
}

#[derive(Deserialize)]
pub struct PatchAccount {
  max_concurrent: Option<u32>,
  monthly_quota: Option<f64>,
  balance_unit: Option<String>,
  disabled: Option<bool>,
}

pub async fn admin_patch_account(Path(id): Path<String>, State(st): State<AppState>, headers: HeaderMap, Json(body): Json<PatchAccount>) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  if st.runtime.account(&id).is_none() {
    let body = json!({"error": {"message": "Unknown account", "code": "not_found"}}).to_string();
    return Err(Response::builder().status(StatusCode::NOT_FOUND)
      .header(header::CONTENT_TYPE, "application/json").body(axum::body::Body::from(body)).unwrap());
  }
  if let Some(d) = body.disabled { st.runtime.set_disabled(&id, d).await; }
  let unit = match body.balance_unit.as_deref() {
    Some("tokens") => Some(crate::config::BalanceUnit::Tokens),
    Some("credits") => Some(crate::config::BalanceUnit::Credits),
    _ => None,
  };
  st.runtime.patch(&id, body.max_concurrent, body.monthly_quota, unit);
  Ok(Json(json!({"ok": true, "id": id})))
}

#[derive(Deserialize)]
pub struct Reconcile { remaining: f64 }

pub async fn admin_reconcile(Path(id): Path<String>, State(st): State<AppState>, headers: HeaderMap, Json(body): Json<Reconcile>) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  if st.runtime.account(&id).is_none() {
    return Err(Response::builder().status(StatusCode::NOT_FOUND)
      .header(header::CONTENT_TYPE, "application/json")
      .body(axum::body::Body::from(json!({"error": {"message": "Unknown account", "code": "not_found"}}).to_string())).unwrap());
  }
  st.runtime.reconcile(&id, body.remaining).await;
  Ok(Json(json!({"ok": true, "id": id, "remaining": body.remaining})))
}

pub async fn admin_clear_exhausted(Path(id): Path<String>, State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  st.runtime.clear_exhausted(&id).await;
  Ok(Json(json!({"ok": true, "id": id})))
}

pub async fn admin_revoke_user(Path(key): Path<String>, State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  st.runtime.store().revoke_user(&key).map_err(|_| unauthorized())?;
  Ok(Json(json!({"ok": true, "revoked": key})))
}

#[derive(Deserialize)]
pub struct AnalyticsQuery { from: Option<String>, to: Option<String> } // "YYYY-MM-DD"

pub async fn admin_analytics(Query(q): Query<AnalyticsQuery>, State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  let now = Utc::now().timestamp();
  let to = q.to.as_deref().and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
    .map(|d| d.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp())
    .unwrap_or(now);
  let from = q.from.as_deref().and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
    .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
    .unwrap_or(now - 29 * 86_400);
  let daily: Vec<Value> = st.runtime.store().daily_stats(from, to + 1).unwrap_or_default()
    .into_iter().map(|(d, ev, i, c, o)| json!({"date": d, "events": ev, "input": i, "cached": c, "output": o, "tokens": i + o})).collect();
  let since = from;
  let to_v = |rows: Vec<(String, u64, u64, u64, f64, u64)>| -> Vec<Value> {
    rows.into_iter().map(|(k, i, _ca, o, cr, n)| json!({"key": k, "input": i, "output": o, "tokens": i + o, "credits": cr, "events": n})).collect()
  };
  Ok(Json(json!({
    "from": from, "to": to,
    "daily": daily,
    "by_user": to_v(st.runtime.store().group_since("user_id", since).unwrap_or_default()),
    "by_account": to_v(st.runtime.store().group_since("account_id", since).unwrap_or_default()),
    "by_model": to_v(st.runtime.store().group_since("model", since).unwrap_or_default()),
  })))
}
