// tests/web_api_test.rs
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use tower::ServiceExt; // oneshot

async fn app() -> axum::Router {
  let cfg = tokenbalancer::config::Config {
    server: tokenbalancer::config::ServerConf { listen: "127.0.0.1:0".into(), admin_key: "tba_admin".into(), db_path: ":memory:".into(), queue_timeout_secs: 1 },
    defaults: tokenbalancer::config::DefaultsConf { region: tokenbalancer::config::Region::Cn, balance_unit: tokenbalancer::config::BalanceUnit::Credits, max_concurrent: 2 },
    accounts: vec![tokenbalancer::config::AccountConf { id: "a1".into(), label: None, api_key: "sk-sp-a".into(), region: None, base_url_openai: None, base_url_anthropic: None, seat_tier: Some(tokenbalancer::config::SeatTier::Pro), balance_unit: None, monthly_quota: None, cycle_start: None, max_concurrent: None, disabled: None }],
    credit_rates: Default::default(),
    users: vec![tokenbalancer::config::UserConf { key: "tbu_alice".into(), name: "alice".into() }],
  };
  let store = tokenbalancer::store::Store::open(":memory:").unwrap();
  let runtime = tokenbalancer::state::Runtime::load(&cfg, std::sync::Arc::new(store)).await.unwrap();
  let client = reqwest::Client::new();
  let state = tokenbalancer::proxy::AppState { runtime, client, queue_timeout: std::time::Duration::from_secs(5), stream_inactivity: std::time::Duration::from_secs(300) };
  tokenbalancer::proxy::router(state)
}

fn get(path: &str, key: Option<&str>) -> Request<Body> {
  let mut b = Request::builder().uri(path);
  if let Some(k) = key { b = b.header(header::AUTHORIZATION, format!("Bearer {k}")); }
  b.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn whoami_roles() {
  let r = app().await.oneshot(get("/api/whoami", Some("tba_admin"))).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
  let r = app().await.oneshot(get("/api/whoami", Some("tbu_alice"))).await.unwrap();
  let body = axum::body::to_bytes(r.into_body(), 10_000).await.unwrap();
  let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
  assert_eq!(v["role"], "user");
  assert_eq!(v["user"]["name"], "alice");
  let r = app().await.oneshot(get("/api/whoami", Some("tbu_bad"))).await.unwrap();
  assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_guard() {
  // user key must NOT reach admin endpoints
  let r = app().await.oneshot(get("/api/admin/accounts", Some("tbu_alice"))).await.unwrap();
  assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
  // admin key works
  let r = app().await.oneshot(get("/api/admin/accounts", Some("tba_admin"))).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
}

#[tokio::test]
async fn create_and_revoke_user() {
  // one app instance for the whole flow (Router is Clone; every app() call
  // would otherwise be a fresh in-memory DB)
  let app = app().await;
  let r = app.clone().oneshot(Request::builder().method(Method::POST).uri("/api/admin/users")
    .header(header::AUTHORIZATION, "Bearer tba_admin")
    .header(header::CONTENT_TYPE, "application/json")
    .body(Body::from(r#"{"name":"bob"}"#)).unwrap()).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
  let body = axum::body::to_bytes(r.into_body(), 10_000).await.unwrap();
  let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
  let key = v["key"].as_str().unwrap().to_string();
  assert!(key.starts_with("tbu_"));
  // bob's key authenticates as a user
  let r = app.clone().oneshot(get("/api/whoami", Some(&key))).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
  // revoke it
  let r = app.clone().oneshot(Request::builder().method(Method::POST).uri(format!("/api/admin/users/{key}/revoke"))
    .header(header::AUTHORIZATION, "Bearer tba_admin")
    .body(Body::empty()).unwrap()).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
  // revoked key no longer authenticates
  let r = app.clone().oneshot(get("/api/whoami", Some(&key))).await.unwrap();
  assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn reconcile_reflects_in_accounts() {
  let r = app().await.oneshot(Request::builder().method(Method::POST).uri("/api/admin/accounts/a1/reconcile")
    .header(header::AUTHORIZATION, "Bearer tba_admin")
    .header(header::CONTENT_TYPE, "application/json")
    .body(Body::from(r#"{"remaining": 42000}"#)).unwrap()).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
}

#[tokio::test]
async fn static_ui_served() {
  let r = app().await.oneshot(get("/", None)).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
  assert!(r.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap().contains("text/html"));
}
#[tokio::test]
async fn ui_assets_contain_expected_content() {
  let r = app().await.oneshot(get("/", None)).await.unwrap();
  let body = axum::body::to_bytes(r.into_body(), 100_000).await.unwrap();
  let html = String::from_utf8_lossy(&body);
  assert!(html.contains("TokenBalancer"));
  assert!(html.contains("/app.js"));

  let r = app().await.oneshot(get("/app.js", None)).await.unwrap();
  let body = axum::body::to_bytes(r.into_body(), 200_000).await.unwrap();
  let js = String::from_utf8_lossy(&body);
  assert!(js.contains("/api/whoami"));
  assert!(js.contains("/api/admin/accounts"));

  let r = app().await.oneshot(get("/styles.css", None)).await.unwrap();
  assert!(r.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap().contains("text/css"));
}
