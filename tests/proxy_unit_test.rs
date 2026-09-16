// tests/proxy_unit_test.rs
use bytes::Bytes;
use tokenbalancer::usage::Protocol;

// prepare_body is a pub fn in proxy.rs
#[test]
fn openai_stream_request_gets_include_usage() {
  let body = Bytes::from(r#"{"model":"qwen3.7-max","stream":true,"messages":[]}"#);
  let (model, out, is_stream) = tokenbalancer::proxy::prepare_body(Protocol::OpenAi, &body);
  assert_eq!(model, "qwen3.7-max");
  assert!(is_stream);
  let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
  assert_eq!(v["stream_options"]["include_usage"], true);
  assert_eq!(v["model"], "qwen3.7-max");
}

#[test]
fn openai_nonstream_untouched() {
  let body = Bytes::from(r#"{"model":"qwen3.7-max","stream":false}"#);
  let (model, out, is_stream) = tokenbalancer::proxy::prepare_body(Protocol::OpenAi, &body);
  assert_eq!(model, "qwen3.7-max");
  assert!(!is_stream);
  let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
  assert!(v.get("stream_options").is_none());
}

#[test]
fn non_json_body_passthrough() {
  let body = Bytes::from("binary-ish");
  let (_m, out, is_stream) = tokenbalancer::proxy::prepare_body(Protocol::OpenAi, &body);
  assert_eq!(out, body);
  assert!(!is_stream);
}

#[test]
fn anthropic_body_model_extracted_no_injection() {
  let body = Bytes::from(r#"{"model":"qwen3.7-max","stream":true}"#);
  let (model, _out, is_stream) = tokenbalancer::proxy::prepare_body(Protocol::Anthropic, &body);
  assert_eq!(model, "qwen3.7-max");
  assert!(is_stream);
}
