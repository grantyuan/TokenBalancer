// src/proxy.rs
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Json, Response};
use axum::routing::{any, get};
use axum::Router;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{Future, Stream};
use rand::SeedableRng;
use serde_json::{json, Value};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::balance;
use crate::state::Runtime;
use crate::store::UsageEvent;
use crate::usage::{parse_anthropic_message, parse_openai_usage, SseTap, UsageTokens};

#[derive(Clone)]
pub struct AppState {
  pub runtime: Arc<Runtime>,
  pub client: reqwest::Client,
  pub queue_timeout: Duration,
  /// Abort/end the stream relay if the upstream sends nothing for this long.
  pub stream_inactivity: Duration,
}

const MAX_BODY: usize = 10 * 1024 * 1024;
/// Plain-body reads: no data from the upstream for this long -> 504.
const UPSTREAM_INACTIVITY: Duration = Duration::from_secs(300);

pub fn router(state: AppState) -> Router {
  Router::new()
    .route("/healthz", get(healthz))
    .route("/", get(crate::web::static_index))
    .route("/app.js", get(crate::web::static_app_js))
    .route("/styles.css", get(crate::web::static_styles))
    .route("/api/whoami", get(crate::web::whoami))
    .route("/api/accounts/health", get(crate::web::accounts_health))
    .route("/api/me/usage", get(crate::web::me_usage))
    .route("/api/admin/accounts", get(crate::web::admin_accounts))
    .route("/api/admin/users", get(crate::web::admin_users).post(crate::web::admin_create_user))
    .route("/api/admin/users/:key/revoke", axum::routing::post(crate::web::admin_revoke_user))
    .route("/api/admin/accounts/:id", axum::routing::patch(crate::web::admin_patch_account))
    .route("/api/admin/accounts/:id/reconcile", axum::routing::post(crate::web::admin_reconcile))
    .route("/api/admin/accounts/:id/clear-exhausted", axum::routing::post(crate::web::admin_clear_exhausted))
    .route("/api/admin/analytics", get(crate::web::admin_analytics))
    .route("/*rest", any(proxy_handler))
    .with_state(state)
}

fn json_err(status: StatusCode, code: &str, message: &str, retry_after: Option<u32>) -> Response {
  let body = json!({"error": {"message": message, "type": "invalid_request_error", "code": code}}).to_string();
  let mut builder = Response::builder().status(status).header(header::CONTENT_TYPE, "application/json");
  if let Some(ra) = retry_after {
    builder = builder.header(header::RETRY_AFTER, ra);
  }
  builder.body(Body::from(body)).unwrap()
}

fn unauth(code: &str, message: &str) -> Response {
  json_err(StatusCode::UNAUTHORIZED, code, message, None)
}

fn bearer(headers: &HeaderMap) -> Option<String> {
  headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
    .and_then(|v| v.strip_prefix("Bearer ")).map(|s| s.trim().to_string())
}

fn authenticate(st: &AppState, headers: &HeaderMap) -> Result<String, Response> {
  let key = bearer(headers).ok_or_else(|| unauth("authentication_error", "Missing Authorization: Bearer <key>"))?;
  if key == st.runtime.admin_key() {
    return Ok("admin".to_string());
  }
  match st.runtime.store().find_live_user_by_key(&key) {
    Ok(Some(u)) => Ok(u.id),
    _ => Err(unauth("invalid_api_key", "Unknown or revoked proxy key")),
  }
}

/// Extract model; inject stream_options.include_usage for OpenAI streams; report stream flag.
pub fn prepare_body(proto: crate::usage::Protocol, body: &Bytes) -> (String, Bytes, bool) {
  if body.is_empty() {
    return (String::from("unknown"), body.clone(), false);
  }
  match serde_json::from_slice::<Value>(body) {
    Ok(mut v) => {
      let model = v.get("model").and_then(|m| m.as_str()).unwrap_or("unknown").to_string();
      let is_stream = v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
      if proto == crate::usage::Protocol::OpenAi && is_stream {
        match v.get_mut("stream_options") {
          Some(Value::Object(o)) => { o.insert("include_usage".into(), Value::Bool(true)); }
          _ => { v["stream_options"] = json!({"include_usage": true}); }
        }
        if let Ok(b) = serde_json::to_vec(&v) {
          return (model, Bytes::from(b), true);
        }
      }
      (model, body.clone(), is_stream)
    }
    Err(_) => (String::from("unknown"), body.clone(), false),
  }
}

fn record_usage_for(proto: crate::usage::Protocol, path: &str) -> bool {
  match proto {
    crate::usage::Protocol::OpenAi => path.ends_with("/chat/completions") || path.ends_with("/responses"),
    crate::usage::Protocol::Anthropic => path.ends_with("/messages"),
  }
}

/// Minimal liveness probe: proves the process answers, nothing else.
/// Account detail (remaining, in-flight, exhausted, ...) lives behind the
/// authenticated /api/accounts/health and /api/admin/accounts endpoints.
async fn healthz() -> Json<Value> {
  Json(json!({"status": "ok"}))
}

async fn proxy_handler(State(st): State<AppState>, req: Request) -> Result<Response, Response> {
  let (parts, body) = req.into_parts();
  let path = parts.uri.path().trim_end_matches('/').to_string();

  let proto = if path.starts_with("/apps/anthropic") {
    crate::usage::Protocol::Anthropic
  } else if path.starts_with("/v1") {
    crate::usage::Protocol::OpenAi
  } else {
    return Err(json_err(StatusCode::NOT_FOUND, "not_found",
      "Unknown route. Use /v1/* (OpenAI-compatible) or /apps/anthropic/* (Anthropic-compatible).", None));
  };

  let user_id = authenticate(&st, &parts.headers)?;
  let body_bytes = axum::body::to_bytes(body, MAX_BODY).await
    .map_err(|_| json_err(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large", "Request body exceeds 10 MB.", None))?;

  let (model, fwd_body, is_stream) = prepare_body(proto, &body_bytes);

  // ---- balance + queue ----
  // StdRng (not thread_rng): ThreadRng is !Send and would persist across the
  // sleep().await below, which would make this handler's future !Send.
  let mut rng = rand::rngs::StdRng::from_entropy();
  let deadline = Instant::now() + st.queue_timeout;
  let (slot, acct) = loop {
    let snaps = st.runtime.build_snapshots().await;
    match balance::select(&snaps, &mut rng) {
      balance::Selection::Chosen(i) => {
        if let Some(a) = st.runtime.account_by_idx(i) {
          if let Some(s) = st.runtime.try_acquire(&a.id) {
            break (s, a);
          }
        }
        // lost the slot to a race — retry immediately
      }
      balance::Selection::NoneAvailable => {
        return Err(json_err(StatusCode::SERVICE_UNAVAILABLE, "no_available_accounts",
          "All upstream accounts are disabled or quota-exhausted. Ask the admin to reconcile or enable an account.", None));
      }
      balance::Selection::AllSlotsFull => {
        if Instant::now() >= deadline {
          return Err(json_err(StatusCode::SERVICE_UNAVAILABLE, "all_accounts_busy",
            "All upstream accounts are at their concurrency cap. Try again shortly.",
            Some(st.queue_timeout.as_secs().min(u32::MAX as u64) as u32)));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
      }
    }
  };

  let started = Instant::now();
  let (base, rest) = match proto {
    crate::usage::Protocol::OpenAi => (&acct.base_url_openai, path.strip_prefix("/v1").unwrap_or(&path)),
    crate::usage::Protocol::Anthropic => (&acct.base_url_anthropic, path.strip_prefix("/apps/anthropic").unwrap_or(&path)),
  };
  let mut url = if rest.starts_with('/') { format!("{base}{rest}") } else { format!("{base}/{rest}") };
  if let Some(q) = parts.uri.query() {
    url.push('?');
    url.push_str(q);
  }

  let headers = crate::forward::build_upstream_headers(&parts.headers, &acct.api_key);
  let upstream = crate::forward::open_stream(&st.client, &parts.method, &url, &headers, Some(fwd_body)).await
    .map_err(|e| json_err(StatusCode::BAD_GATEWAY, "upstream_error", &format!("Upstream unreachable: {e}"), None))?;
  let status = upstream.status().as_u16();

  if status >= 400 || !is_stream {
    // ---- plain pass-through (incl. error bodies on streaming requests) ----
    let content_type = upstream.headers().get(header::CONTENT_TYPE)
      .and_then(|v| v.to_str().ok()).unwrap_or("application/octet-stream").to_string();
    let plain_body = match tokio::time::timeout(UPSTREAM_INACTIVITY, upstream.bytes()).await {
      Ok(b) => b.map_err(|e| json_err(StatusCode::BAD_GATEWAY, "upstream_error", &format!("Upstream read failed: {e}"), None))?,
      Err(_) => {
        drop(slot);
        return Err(json_err(StatusCode::GATEWAY_TIMEOUT, "upstream_timeout", "Upstream stopped sending data.", None));
      }
    };
    if crate::forward::is_quota_exhausted(status, &plain_body) {
      st.runtime.mark_exhausted(&acct.id).await;
    }
    if status < 400 && record_usage_for(proto, &path) {
      let u = match proto {
        crate::usage::Protocol::OpenAi => parse_openai_usage(std::str::from_utf8(&plain_body).unwrap_or("")).unwrap_or(UsageTokens::missing()),
        crate::usage::Protocol::Anthropic => parse_anthropic_message(std::str::from_utf8(&plain_body).unwrap_or("")).unwrap_or(UsageTokens::missing()),
      };
      let ev = UsageEvent {
        ts: chrono::Utc::now(), user_id: user_id.clone(), account_id: acct.id.clone(),
        model: model.clone(), input: u.input, cached: u.cached, output: u.output,
        credits: 0.0, latency_ms: started.elapsed().as_millis() as u64,
        status, stream: false, parse_error: u.parse_error,
      };
      st.runtime.record_event(&ev).await;
    }
    drop(slot);
    return Ok(Response::builder()
      .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
      .header(header::CONTENT_TYPE, content_type)
      .body(Body::from(plain_body))
      .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "response_build", "Failed to build response.", None))?);
  }

  // ---- stream relay ----
  let content_type = upstream.headers().get(header::CONTENT_TYPE)
    .and_then(|v| v.to_str().ok()).unwrap_or("text/event-stream").to_string();
  let tap_stream = TapStream {
    inner: Box::pin(upstream.bytes_stream()),
    tap: SseTap::new(proto),
    usage: None,
    finalized: false,
    slot: Some(slot),
    runtime: st.runtime.clone(),
    user_id,
    acct_id: acct.id.clone(),
    model,
    status,
    started,
    inactivity: st.stream_inactivity,
    inactivity_timer: None,
  };
  Ok(Response::builder()
    .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    .header(header::CONTENT_TYPE, content_type)
    .body(Body::from_stream(tap_stream))
    .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "response_build", "Failed to build response.", None))?)
}

/// Relays the upstream byte stream and captures usage exactly once.
struct TapStream {
  inner: BoxStream<'static, Result<Bytes, reqwest::Error>>,
  tap: SseTap,
  usage: Option<UsageTokens>,
  finalized: bool,
  slot: Option<crate::state::Slot>, // released when the stream ends (or is dropped)
  runtime: Arc<Runtime>,
  user_id: String,
  acct_id: String,
  model: String,
  status: u16,
  started: Instant,
  /// End the relay if the upstream sends nothing for this long (releases the
  /// slot and records partial usage).
  inactivity: Duration,
  /// Armed on the first Pending after the last chunk; when it fires, the
  /// relay ends (releases the slot, records partial usage).
  inactivity_timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Stream for TapStream {
  type Item = Result<Bytes, reqwest::Error>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    // Inactivity deadline first: once it has fired, end the relay.
    if let Some(t) = self.inactivity_timer.as_mut() {
      if t.as_mut().poll(cx).is_ready() {
        // upstream stopped sending data: end the relay cleanly (the Drop
        // impl also finalizes if the client goes away first)
        self.finalize();
        return Poll::Ready(None);
      }
    }
    match self.inner.as_mut().poll_next(cx) {
      Poll::Ready(Some(Ok(chunk))) => {
        self.inactivity_timer = None; // new activity: reset the deadline
        if self.usage.is_none() {
          self.usage = self.tap.feed(&chunk);
        }
        Poll::Ready(Some(Ok(chunk)))
      }
      Poll::Ready(x @ Some(Err(_))) => { self.finalize(); Poll::Ready(x) }
      Poll::Ready(None) => { self.finalize(); Poll::Ready(None) }
      Poll::Pending => {
        if self.inactivity_timer.is_none() {
          self.inactivity_timer = Some(Box::pin(tokio::time::sleep(self.inactivity)));
        }
        Poll::Pending
      }
    }
  }
}

impl Drop for TapStream {
  fn drop(&mut self) { self.finalize(); }
}

impl TapStream {
  fn finalize(&mut self) {
    if self.finalized { return; }
    self.finalized = true;
    let usage = self.usage.take().unwrap_or(UsageTokens::missing());
    let runtime = self.runtime.clone();
    let user_id = self.user_id.clone();
    let acct_id = self.acct_id.clone();
    let model = self.model.clone();
    let status = self.status;
    let latency_ms = self.started.elapsed().as_millis() as u64;
    let _slot = self.slot.take(); // releases the in-flight slot
    tokio::spawn(async move {
      let ev = UsageEvent {
        ts: chrono::Utc::now(), user_id, account_id: acct_id, model,
        input: usage.input, cached: usage.cached, output: usage.output,
        credits: 0.0, latency_ms, status, stream: true, parse_error: usage.parse_error,
      };
      runtime.record_event(&ev).await;
      drop(_slot);
    });
  }
}
