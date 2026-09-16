// src/forward.rs
use bytes::Bytes;
use http::HeaderMap;
use reqwest::Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol { OpenAi, Anthropic }

pub struct UpstreamTarget {
  pub url: String,
  pub api_key: String,
}

pub struct PlainOutcome {
  pub status: u16,
  pub content_type: String,
  pub body: Bytes,
  pub quota_exhausted: bool,
}

const DROP: [&str; 12] = [
  "host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailers", "transfer-encoding", "upgrade", "content-length",
  "authorization", "x-api-key",
];

/// Copy client headers upstream, minus hop-by-hop/auth; add the upstream key as Bearer.
/// (Token Plan endpoints require `Authorization: Bearer`, never `x-api-key`.)
pub fn build_upstream_headers(downstream: &HeaderMap, upstream_key: &str) -> Vec<(String, String)> {
  let mut out: Vec<(String, String)> = Vec::new();
  for (k, v) in downstream.iter() {
    let name = k.as_str().to_ascii_lowercase();
    if DROP.contains(&name.as_str()) { continue; }
    if let Ok(sv) = v.to_str() {
      out.push((k.as_str().to_string(), sv.to_string()));
    }
  }
  out.push(("Authorization".to_string(), format!("Bearer {upstream_key}")));
  out
}

/// Strict markers that mean "this Token Plan account's credits are gone"
/// (as opposed to a transient per-second/per-minute rate limit).
pub fn is_quota_exhausted(status: u16, body: &[u8]) -> bool {
  if status != 429 && status != 402 && status != 403 { return false; }
  let s = String::from_utf8_lossy(body).to_ascii_lowercase();
  s.contains("allocationquota")
    || s.contains("insufficient_quota")
    || s.contains("insufficient quota")
    || s.contains("quota exhausted")
    || s.contains("out of quota")
    || s.contains("exceed your quota")
}

pub async fn open_stream(
  client: &Client,
  method: &reqwest::Method,
  url: &str,
  headers: &[(String, String)],
  body: Option<Bytes>,
) -> anyhow::Result<reqwest::Response> {
  let mut req = client.request(method.clone(), url);
  for (k, v) in headers {
    req = req.header(k.as_str(), v.as_str());
  }
  if let Some(b) = body {
    req = req.body(b);
  }
  Ok(req.send().await?)
}

pub async fn forward_plain(
  client: &Client,
  method: &reqwest::Method,
  target: &UpstreamTarget,
  headers: &[(String, String)],
  body: Option<Bytes>,
) -> anyhow::Result<PlainOutcome> {
  let resp = open_stream(client, method, &target.url, headers, body).await?;
  let status = resp.status().as_u16();
  let content_type = resp
    .headers()
    .get(reqwest::header::CONTENT_TYPE)
    .and_then(|v| v.to_str().ok())
    .unwrap_or("application/octet-stream")
    .to_string();
  let body = resp.bytes().await?;
  let quota_exhausted = is_quota_exhausted(status, &body);
  Ok(PlainOutcome { status, content_type, body, quota_exhausted })
}
