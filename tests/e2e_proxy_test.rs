// tests/e2e_proxy_test.rs
use std::sync::Arc;
use std::time::Duration;
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use serde_json::{json, Value};

// ---------- mock upstream (single server; accounts distinguished by Bearer key) ----------
const SSE_FINAL: &str = concat!(
  "data: {\"id\":\"x\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
  "data: {\"id\":\"x\",\"choices\":[],\"usage\":{\"prompt_tokens\":300,\"completion_tokens\":150,\"total_tokens\":450}}\n\n",
  "data: [DONE]\n\n"
);

#[derive(Clone)]
struct Mock {
  calls: Arc<std::sync::Mutex<Vec<(String, String)>>>, // (auth, path)
  mode: Arc<std::sync::Mutex<String>>,                // "ok" | "quota-A" | "slow"
}

async fn mock_handler(
  axum::extract::State(m): axum::extract::State<Mock>,
  req: Request<Body>,
) -> axum::response::Response {
  let auth = req.headers().get(header::AUTHORIZATION)
    .and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
  let path = req.uri().path().to_string();
  m.calls.lock().unwrap().push((auth.clone(), path.clone()));
  let body = axum::body::to_bytes(req.into_body(), 1_000_000).await.unwrap();
  let bv: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
  let want_stream = bv.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
  let mode = m.mode.lock().unwrap().clone();

  if mode == "quota-A" && auth == "Bearer sk-sp-A" {
    return axum::response::Response::builder().status(StatusCode::TOO_MANY_REQUESTS)
      .header(header::CONTENT_TYPE, "application/json")
      .body(Body::from(r#"{"error":{"message":"Throttling.AllocationQuota: insufficient_quota","code":"AllocationQuota"}}"#))
      .unwrap();
  }
  if mode == "slow" {
    tokio::time::sleep(Duration::from_millis(300)).await;
  }
  if want_stream {
    return axum::response::Response::builder().status(StatusCode::OK)
      .header(header::CONTENT_TYPE, "text/event-stream")
      .body(Body::from(SSE_FINAL)).unwrap();
  }
  axum::response::Response::builder().status(StatusCode::OK)
    .header(header::CONTENT_TYPE, "application/json")
    .body(Body::from(
      r#"{"id":"x","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":300,"completion_tokens":150,"total_tokens":450}}"#))
    .unwrap()
}

// ---------- harness ----------
#[derive(Clone)]
struct Harness { port: u16, mock: Mock, store: Arc<tokenbalancer::store::Store> }

fn account_conf(id: &str, key: &str, base: &str, quota: Option<f64>, maxc: u32) -> tokenbalancer::config::AccountConf {
  tokenbalancer::config::AccountConf {
    id: id.into(), label: Some(id.into()), api_key: key.into(), region: None,
    base_url_openai: Some(base.into()), base_url_anthropic: None,
    seat_tier: None, balance_unit: None, monthly_quota: quota, cycle_start: None,
    max_concurrent: Some(maxc), disabled: None,
  }
}

async fn start(a_quota: Option<f64>, a_max: u32, b_quota: Option<f64>, b_max: u32) -> Harness {
  let mock = Mock { calls: Arc::new(std::sync::Mutex::new(Vec::new())), mode: Arc::new(std::sync::Mutex::new("ok".into())) };
  let mock_app = Router::new().route("/*rest", post(mock_handler)).with_state(mock.clone());
  let ml = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let mport = ml.local_addr().unwrap().port();
  tokio::spawn(async move { let _ = axum::serve(ml, mock_app).await; });
  tokio::time::sleep(Duration::from_millis(100)).await;
  let base = format!("http://127.0.0.1:{mport}");

  let store = Arc::new(tokenbalancer::store::Store::open(":memory:").unwrap());
  let cfg = tokenbalancer::config::Config {
    server: tokenbalancer::config::ServerConf { listen: "127.0.0.1:0".into(), admin_key: "tba_e2e".into(), db_path: ":memory:".into(), queue_timeout_secs: 5 },
    defaults: tokenbalancer::config::DefaultsConf { region: tokenbalancer::config::Region::Cn, balance_unit: tokenbalancer::config::BalanceUnit::Credits, max_concurrent: 2 },
    accounts: vec![
      account_conf("A", "sk-sp-A", &base, a_quota, a_max),
      account_conf("B", "sk-sp-B", &base, b_quota, b_max),
    ],
    credit_rates: Default::default(),
    users: vec![tokenbalancer::config::UserConf { key: "tbu_e2e".into(), name: "tester".into() }],
  };
  let runtime = tokenbalancer::state::Runtime::load(&cfg, store.clone()).await.unwrap();
  let state = tokenbalancer::proxy::AppState { runtime, client: reqwest::Client::new(), queue_timeout: Duration::from_secs(5) };
  let app = tokenbalancer::proxy::router(state);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = listener.local_addr().unwrap().port();
  tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
  tokio::time::sleep(Duration::from_millis(100)).await;
  Harness { port, mock, store }
}


async fn call(h: &Harness, path: &str, key: &str, body: Value) -> (StatusCode, String, Vec<(String, String)>) {
  let url = format!("http://127.0.0.1:{}{}", h.port, path);
  let r = reqwest::Client::new()
    .post(&url)
    .header(header::AUTHORIZATION, format!("Bearer {key}"))
    .json(&body)
    .send().await.unwrap();
  let status = r.status();
  let text = r.text().await.unwrap();
  let calls = h.mock.calls.lock().unwrap().clone();
  (status, text, calls)
}

// Default balance unit is Credits, so seed events must carry credits —
// token fields do not move the credits window.
fn seed_usage(store: &Arc<tokenbalancer::store::Store>, account: &str, credits: f64) {
  let ev = tokenbalancer::store::UsageEvent {
    ts: chrono::Utc::now(), user_id: "seed".into(), account_id: account.into(),
    model: "seed".into(), input: 0, cached: 0, output: 0,
    credits, latency_ms: 0, status: 200, stream: false, parse_error: false,
  };
  store.insert_event(&ev).unwrap();
}

fn chat(stream: bool) -> Value {
  json!({"model":"qwen3.7-max","stream":stream,"messages":[{"role":"user","content":"hi"}]})
}

#[tokio::test]
async fn bad_key_401() {
  let h = start(None, 2, None, 2).await; // (None,None) quotas
  let (status, _, _) = call(&h, "/v1/chat/completions", "tbu_wrong", chat(false)).await;
  assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn routes_to_highest_remaining_and_records_usage() {
  let h = start(Some(1_000_000.0), 2, Some(1_000_000.0), 2).await;
  // pre-consume B so A is clearly higher
  seed_usage(&h.store, "B", 900_000.0); // B pct 0.1 -> A strictly preferred
  let (status, text, calls) = call(&h, "/v1/chat/completions", "tbu_e2e", chat(false)).await;
  assert_eq!(status, StatusCode::OK);
  let v: Value = serde_json::from_str(&text).unwrap();
  assert_eq!(v["usage"]["prompt_tokens"], 300);
  assert_eq!(calls.len(), 1);
  assert_eq!(calls[0].0, "Bearer sk-sp-A");
  // usage recorded for the user
  let (i, _c, o, _cr, n) = h.store.totals_for("tbu_e2e", 0).unwrap();
  assert_eq!((i, o, n), (300, 150, 1));
}

#[tokio::test]
async fn balancing_shifts_to_other_account_after_consumption() {
  let h = start(Some(5.0), 2, Some(1_000_000.0), 2).await;
  seed_usage(&h.store, "B", 50_000.0); // B pct 0.95 vs A 1.0
  // each request costs 2.1 credits (300/500 in + 150/100 out); A falls below B after req1; B serves req2/req3
  for _ in 0..3 {
    let (status, _, _) = call(&h, "/v1/chat/completions", "tbu_e2e", chat(false)).await;
    assert_eq!(status, StatusCode::OK);
  }
  let calls = h.mock.calls.lock().unwrap().clone();
  assert_eq!(calls[0].0, "Bearer sk-sp-A");
  assert_eq!(calls[1].0, "Bearer sk-sp-B"); // A remaining fraction (0.58) < B (0.95)
  assert_eq!(calls[2].0, "Bearer sk-sp-B");
}

#[tokio::test]
async fn concurrency_cap_queues_then_proceeds() {
  let h = start_single().await; // one account, max_concurrent=1, slow mock
  *h.mock.mode.lock().unwrap() = "slow".into();
  let (a, b) = (h.clone(), h.clone());
  let r1 = tokio::spawn(async move { call(&a, "/v1/chat/completions", "tbu_e2e", chat(false)).await });
  tokio::time::sleep(Duration::from_millis(50)).await; // first request in flight
  let r2 = tokio::spawn(async move { call(&b, "/v1/chat/completions", "tbu_e2e", chat(false)).await });
  let (s1, _, _) = r1.await.unwrap();
  let (s2, _, calls) = r2.await.unwrap();
  assert_eq!(s1, StatusCode::OK);
  assert_eq!(s2, StatusCode::OK); // queued ~300ms then served (queue_timeout 5s)
  assert_eq!(calls.len(), 2);
  assert!(calls.iter().all(|(k, _)| k == "Bearer sk-sp-A"));
}

async fn start_single() -> Harness {
  // single-account variant: A max_concurrent=1 quota 10_000 credits
  let mock = Mock { calls: Arc::new(std::sync::Mutex::new(Vec::new())), mode: Arc::new(std::sync::Mutex::new("ok".into())) };
  let mock_app = Router::new().route("/*rest", post(mock_handler)).with_state(mock.clone());
  let ml = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let mport = ml.local_addr().unwrap().port();
  tokio::spawn(async move { let _ = axum::serve(ml, mock_app).await; });
  tokio::time::sleep(Duration::from_millis(100)).await;
  let base = format!("http://127.0.0.1:{mport}");
  let store = Arc::new(tokenbalancer::store::Store::open(":memory:").unwrap());
  let cfg = tokenbalancer::config::Config {
    server: tokenbalancer::config::ServerConf { listen: "127.0.0.1:0".into(), admin_key: "tba".into(), db_path: ":memory:".into(), queue_timeout_secs: 5 },
    defaults: tokenbalancer::config::DefaultsConf { region: tokenbalancer::config::Region::Cn, balance_unit: tokenbalancer::config::BalanceUnit::Credits, max_concurrent: 2 },
    accounts: vec![account_conf("A", "sk-sp-A", &base, Some(10_000.0), 1)],
    credit_rates: Default::default(),
    users: vec![tokenbalancer::config::UserConf { key: "tbu_e2e".into(), name: "t".into() }],
  };
  let runtime = tokenbalancer::state::Runtime::load(&cfg, store.clone()).await.unwrap();
  let state = tokenbalancer::proxy::AppState { runtime, client: reqwest::Client::new(), queue_timeout: Duration::from_secs(5) };
  let app = tokenbalancer::proxy::router(state);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = listener.local_addr().unwrap().port();
  tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
  tokio::time::sleep(Duration::from_millis(100)).await;
  Harness { port, mock, store }
}

#[tokio::test]
async fn quota_429_marks_exhausted_and_fails_over() {
  let h = start(Some(1_000_000.0), 2, Some(1_000_000.0), 2).await;
  seed_usage(&h.store, "B", 500_000.0); // A pct 1.0 vs B 0.5
  *h.mock.mode.lock().unwrap() = "quota-A".into();
  let (s1, t1, _) = call(&h, "/v1/chat/completions", "tbu_e2e", chat(false)).await;
  assert_eq!(s1, StatusCode::TOO_MANY_REQUESTS);
  assert!(t1.contains("AllocationQuota"));
  let (s2, _, calls) = call(&h, "/v1/chat/completions", "tbu_e2e", chat(false)).await;
  assert_eq!(s2, StatusCode::OK); // failed over to B
  assert_eq!(calls.last().unwrap().0, "Bearer sk-sp-B");
  // healthz reflects exhaustion
  let r = reqwest::get(format!("http://127.0.0.1:{}/healthz", h.port)).await.unwrap();
  let v: Value = r.json().await.unwrap();
  let a = &v["accounts"][0];
  assert_eq!(a["id"], "A");
  assert_eq!(a["exhausted"], true);
}

#[tokio::test]
async fn sse_stream_relay_and_usage_recorded() {
  let h = start(None, 2, None, 2).await;
  let url = format!("http://127.0.0.1:{}/v1/chat/completions", h.port);
  let r = reqwest::Client::new().post(&url)
    .header(header::AUTHORIZATION, "Bearer tbu_e2e")
    .json(&chat(true)).send().await.unwrap();
  assert!(r.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap().contains("text/event-stream"));
  let text = r.text().await.unwrap();
  assert!(text.contains("data:"));
  assert!(text.contains("usage"));
  // wait for the record to land (spawned task)
  tokio::time::sleep(Duration::from_millis(300)).await;
  let (i, _c, o, _cr, n) = h.store.totals_for("tbu_e2e", 0).unwrap();
  assert_eq!((i, o, n), (300, 150, 1));
}
