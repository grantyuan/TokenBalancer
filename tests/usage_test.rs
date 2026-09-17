// tests/usage_test.rs
use tokenbalancer::forward::Protocol;
use tokenbalancer::usage::{parse_openai_usage, parse_openai_usage_from_sse, AnthropicStreamParser, SseTap, UsageTokens};

#[test]
fn parse_openai_json_body() {
  let body = r#"{"id":"x","choices":[],"usage":{"prompt_tokens":1000,"completion_tokens":200,"total_tokens":1200,"prompt_tokens_details":{"cached_tokens":500}}}"#;
  let u = parse_openai_usage(body).unwrap();
  assert_eq!(u.input, 1000);
  assert_eq!(u.cached, 500);
  assert_eq!(u.output, 200);
  assert!(!u.parse_error);
}

#[test]
fn parse_openai_sse_final_chunk() {
  let line = r#"data: {"id":"x","choices":[],"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150}}"#;
  let u = parse_openai_usage_from_sse(line).unwrap();
  assert_eq!(u.unwrap().input, 100);
  assert_eq!(u.unwrap().output, 50);
  // non-usage line -> None; [DONE] -> None
  assert!(parse_openai_usage_from_sse("data: {\"choices\":[{\"delta\":{}}]}").unwrap().is_none());
  assert!(parse_openai_usage_from_sse("data: [DONE]").unwrap().is_none());
}

#[test]
fn parse_openai_no_usage_is_missing() {
  assert!(parse_openai_usage(r#"{"id":"x","choices":[]}"#).is_none());
  let u = UsageTokens::missing();
  assert!(u.parse_error);
  assert_eq!(u.total(), 0);
}

#[test]
fn parse_anthropic_stream() {
  let mut p = AnthropicStreamParser::new();
  let stream = concat!(
    "event: message_start\n",
    "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1000,\"cache_read_input_tokens\":300,\"cache_creation_input_tokens\":50}}}\n",
    "\n",
    "event: content_block_delta\n",
    "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hi\"}}\n",
    "\n",
    "event: message_delta\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":250}}\n",
    "\n",
    "event: message_stop\n",
    "data: {\"type\":\"message_stop\"}\n",
    "\n"
  );
  p.feed(stream.as_bytes());
  let u = p.finish();
  assert_eq!(u.input, 1000);
  assert_eq!(u.cached, 350); // cache_read + cache_creation
  assert_eq!(u.output, 250);
  assert!(!u.parse_error);
}

#[test]
fn parse_anthropic_split_chunks() {
  // same stream fed in 7-byte slices; result must be identical
  let stream = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1000,\"cache_read_input_tokens\":300}}}\n",
    "\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":250}}\n",
    "\n"
  );
  let mut p = AnthropicStreamParser::new();
  for chunk in stream.as_bytes().chunks(7) { p.feed(chunk); }
  let u = p.finish();
  assert_eq!(u.input, 1000);
  assert_eq!(u.cached, 300);
  assert_eq!(u.output, 250);
  assert!(!u.parse_error);
}

#[test]
fn parse_anthropic_no_usage_is_error() {
  let mut p = AnthropicStreamParser::new();
  p.feed(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
  let u = p.finish();
  assert!(u.parse_error);
}

#[test]
fn openai_tap_captures_final_usage_once() {
  let stream = concat!(
    "data: {\"id\":\"x\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
    "data: {\"id\":\"x\",\"choices\":[]}\n\n",
    "data: {\"id\":\"x\",\"choices\":[],\"usage\":{\"prompt_tokens\":300,\"completion_tokens\":150,\"total_tokens\":450}}\n\n",
    "data: [DONE]\n\n"
  );
  let mut tap = SseTap::new(Protocol::OpenAi);
  let mut captured = Vec::new();
  for chunk in stream.as_bytes().chunks(13) {
    if let Some(u) = tap.feed(chunk) { captured.push(u); }
  }
  assert_eq!(captured.len(), 1, "usage must be captured exactly once");
  assert_eq!(captured[0].input, 300);
  assert_eq!(captured[0].output, 150);
  // feeding more after capture yields nothing
  assert!(tap.feed(b"junk").is_none());
}

#[test]
fn anthropic_tap_captures_once_on_message_delta() {
  let stream = concat!(
    "event: message_start\n",
    "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1000,\"cache_read_input_tokens\":300}}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"x\"}}\n\n",
    "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":250}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n"
  );
  let mut tap = SseTap::new(Protocol::Anthropic);
  let mut captured = Vec::new();
  for chunk in stream.as_bytes().chunks(11) {
    if let Some(u) = tap.feed(chunk) { captured.push(u); }
  }
  assert_eq!(captured.len(), 1);
  assert_eq!(captured[0].input, 1000);
  assert_eq!(captured[0].cached, 300);
  assert_eq!(captured[0].output, 250);
  assert!(tap.feed(b"more").is_none());
}

#[test]
fn openai_tap_no_usage_stays_empty() {
  let stream = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";
  let mut tap = SseTap::new(Protocol::OpenAi);
  for chunk in stream.as_bytes().chunks(7) {
    assert!(tap.feed(chunk).is_none());
  }
}
