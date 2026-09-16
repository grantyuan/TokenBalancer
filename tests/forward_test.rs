use std::sync::{Arc, Mutex};
use axum::{body::Body, extract::State, http::{header, Request, StatusCode}, response::IntoResponse, routing::post, Router};
use serde_json::json;
use tokenbalancer::forward::*;

#[test]
fn quota_marker_detection() {
  // documented Token Plan exhaustion marker
  assert!(is_quota_exhausted(429, b"{\"error\":{\"message\":\"Throttling.AllocationQuota: quota exhausted\"}}"));
  assert!(is_quota_exhausted(429, b"{\"error\":{\"code\":\"insufficient_quota\"}}"));
  // transient TPM rate limit is NOT exhaustion
  assert!(!is_quota_exhausted(429, b"{\"error\":{\"message\":\"Rate limit exceeded. Try again later.\"}}"));
  assert!(!is_quota_exhausted(400, b"quota"));
}

#[tokio::test]
async fn header_rewrite_drops_auth_and_injects_bearer() {
  let mut hd = http::HeaderMap::new();
  hd.insert(header::AUTHORIZATION, "Bearer tbu_downstream".parse().unwrap());
  hd.insert("x-api-key", "somesecret".parse().unwrap());
  hd.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
  hd.insert("x-request-id", "abc".parse().unwrap());
  let out = build_upstream_headers(&hd, "sk-sp-upstream");
  let names: Vec<&str> = out.iter().map(|(k, _)| k.as_str()).collect();
  assert!(!names.iter().any(|n| n.eq_ignore_ascii_case("authorization") && out.iter().any(|(_, v)| v == "Bearer tbu_downstream")));
  assert!(out.iter().any(|(k, v)| k == "Authorization" && v == "Bearer sk-sp-upstream"));
  assert!(!names.iter().any(|n| n.eq_ignore_ascii_case("x-api-key")));
  assert!(names.iter().any(|n| n.eq_ignore_ascii_case("x-request-id")));
  assert!(names.iter().any(|n| n.eq_ignore_ascii_case("content-type")));
}

// Mock upstream that records the Authorization header it received.
async fn capture_auth(
  State(seen): State<Arc<Mutex<Vec<String>>>>,
  req: Request<Body>,
) -> impl IntoResponse {
  let auth = req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
  seen.lock().unwrap().push(auth);
  (
    [(header::CONTENT_TYPE, "application/json")],
    json!({"id":"x","usage":{"prompt_tokens":10,"completion_tokens":5}}).to_string(),
  )
}

#[tokio::test]
async fn forward_plain_roundtrip() {
  let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
  let app = Router::new()
    .route("/v1/chat/completions", post(capture_auth))
    .with_state(Arc::clone(&seen));
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = listener.local_addr().unwrap().port();
  tokio::spawn(async move { axum::serve(listener, app).await });
  tokio::time::sleep(std::time::Duration::from_millis(100)).await;

  let client = reqwest::Client::new();
  let mut hd = http::HeaderMap::new();
  hd.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
  let headers = build_upstream_headers(&hd, "sk-sp-test");
  let target = UpstreamTarget { url: format!("http://127.0.0.1:{port}/v1/chat/completions"), api_key: "sk-sp-test".into() };
  let out = forward_plain(&client, &reqwest::Method::POST, &target, &headers, Some(bytes::Bytes::from(r#"{"model":"qwen3.7-max"}"#))).await.unwrap();
  assert_eq!(out.status, 200);
  assert!(out.content_type.contains("application/json"));
  assert!(!out.quota_exhausted);
  let body: serde_json::Value = serde_json::from_slice(&out.body).unwrap();
  assert_eq!(body["usage"]["prompt_tokens"], 10);
  assert_eq!(*seen.lock().unwrap(), vec!["Bearer sk-sp-test".to_string()]);
}
