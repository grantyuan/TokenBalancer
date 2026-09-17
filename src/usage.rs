// src/usage.rs
use serde_json::Value;

/// Upstream protocol family for an account/stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol { OpenAi, Anthropic }

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageTokens {
  /// Total input tokens (OpenAI: prompt_tokens; Anthropic: input_tokens).
  pub input: u64,
  /// Cached input tokens (subset of input for OpenAI; cache_read+cache_creation for Anthropic).
  pub cached: u64,
  pub output: u64,
  pub parse_error: bool,
}

impl UsageTokens {
  /// Total billed tokens = input (already includes cached) + output.
  pub fn total(&self) -> u64 { self.input + self.output }
  /// Sentinel for "no usage captured".
  pub fn missing() -> Self { UsageTokens { input: 0, cached: 0, output: 0, parse_error: true } }
}

fn u64_of(v: &Value) -> u64 { v.as_u64().unwrap_or(0) }

/// Parse usage from a complete OpenAI-compatible JSON response body.
pub fn parse_openai_usage(body: &str) -> Option<UsageTokens> {
  let v: Value = serde_json::from_str(body).ok()?;
  let usage = v.get("usage")?;
  Some(UsageTokens {
    input: u64_of(&usage.get("prompt_tokens").unwrap_or(&Value::Null)),
    cached: usage.get("prompt_tokens_details").and_then(|d| d.get("cached_tokens")).map(u64_of).unwrap_or(0),
    output: u64_of(&usage.get("completion_tokens").unwrap_or(&Value::Null)),
    parse_error: false,
  })
}

/// Parse usage from a complete (non-stream) Anthropic message JSON body.
pub fn parse_anthropic_message(body: &str) -> Option<UsageTokens> {
  let v: Value = serde_json::from_str(body).ok()?;
  let usage = v.get("usage")?;
  Some(UsageTokens {
    input: u64_of(&usage.get("input_tokens").unwrap_or(&Value::Null)),
    cached: u64_of(&usage.get("cache_read_input_tokens").unwrap_or(&Value::Null))
      + u64_of(&usage.get("cache_creation_input_tokens").unwrap_or(&Value::Null)),
    output: u64_of(&usage.get("output_tokens").unwrap_or(&Value::Null)),
    parse_error: false,
  })
}

/// Parse usage from ONE SSE `data:` line of an OpenAI stream.
/// Some(...) only when the line carries a `usage` object (final chunk with
/// stream_options.include_usage); Ok(None) otherwise (incl. [DONE]).
pub fn parse_openai_usage_from_sse(data_line: &str) -> Result<Option<UsageTokens>, String> {
  let payload = data_line.trim()
    .strip_prefix("data:")
    .ok_or_else(|| "not an SSE data line".to_string())?
    .trim();
  if payload == "[DONE]" { return Ok(None); }
  let v: Value = serde_json::from_str(payload).map_err(|e| e.to_string())?;
  let Some(usage) = v.get("usage") else { return Ok(None); };
  Ok(Some(UsageTokens {
    input: u64_of(&usage.get("prompt_tokens").unwrap_or(&Value::Null)),
    cached: usage.get("prompt_tokens_details").and_then(|d| d.get("cached_tokens")).map(u64_of).unwrap_or(0),
    output: u64_of(&usage.get("completion_tokens").unwrap_or(&Value::Null)),
    parse_error: false,
  }))
}

/// Stateful parser for an Anthropic SSE stream. Feed raw stream bytes in any
/// chunking; call finish() at stream end.
pub struct AnthropicStreamParser {
  input: u64,
  cached: u64,
  output: u64,
  saw_start: bool,
  saw_delta: bool,
  buf: Vec<u8>,
}

impl AnthropicStreamParser {
  pub fn new() -> Self {
    Self { input: 0, cached: 0, output: 0, saw_start: false, saw_delta: false, buf: Vec::new() }
  }

  /// Feed a raw SSE chunk. Extracts complete events (terminated by a blank line),
  /// keeps the partial tail in the internal buffer.
  pub fn feed(&mut self, chunk: &[u8]) {
    self.buf.extend_from_slice(chunk);
    loop {
      match find_subseq(&self.buf, b"\n\n") {
        Some(end) => {
          let event: Vec<u8> = self.buf.drain(..end + 2).collect();
          let text = String::from_utf8_lossy(&event).into_owned();
          self.parse_event(&text);
        }
        None => break,
      }
    }
  }

  fn parse_event(&mut self, text: &str) {
    for line in text.lines() {
      let Some(payload) = line.trim().strip_prefix("data:") else { continue };
      let payload = payload.trim();
      if payload.is_empty() { continue; }
      let Ok(v) = serde_json::from_str::<Value>(payload) else { continue };
      let t = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
      match t {
        "message_start" => {
          if let Some(usage) = v.pointer("/message/usage") {
            self.input = u64_of(&usage.get("input_tokens").unwrap_or(&Value::Null));
            self.cached = u64_of(&usage.get("cache_read_input_tokens").unwrap_or(&Value::Null))
              + u64_of(&usage.get("cache_creation_input_tokens").unwrap_or(&Value::Null));
            self.saw_start = true;
          }
        }
        "message_delta" => {
          if let Some(usage) = v.get("usage") {
            self.output = u64_of(&usage.get("output_tokens").unwrap_or(&Value::Null));
            self.saw_delta = true;
          }
        }
        _ => {}
      }
    }
  }

  pub fn is_complete(&self) -> bool { self.saw_start && self.saw_delta }

  /// Consume accumulated usage. Returns missing() if not complete or already taken.
  pub fn take(&mut self) -> UsageTokens {
    if self.saw_start && self.saw_delta {
      let u = UsageTokens { input: self.input, cached: self.cached, output: self.output, parse_error: false };
      self.saw_start = false;
      self.saw_delta = false;
      u
    } else {
      UsageTokens::missing()
    }
  }

  pub fn finish(mut self) -> UsageTokens { self.take() }
}

/// First index i such that hay[i..i+needle.len()] == needle, else None.
fn find_subseq(hay: &[u8], needle: &[u8]) -> Option<usize> {
  if needle.len() > hay.len() { return None; }
  (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

/// One-shot usage capture for a relayed SSE stream. Feed upstream bytes in any
/// chunking; feed() returns the UsageTokens exactly once when known.
pub struct SseTap {
  proto: Protocol,
  line_buf: Vec<u8>,
  openai_done: Option<UsageTokens>,
  anthropic: AnthropicStreamParser,
}

impl SseTap {
  pub fn new(proto: Protocol) -> Self {
    Self { proto, line_buf: Vec::new(), openai_done: None, anthropic: AnthropicStreamParser::new() }
  }

  pub fn feed(&mut self, chunk: &[u8]) -> Option<UsageTokens> {
    match self.proto {
      Protocol::OpenAi => {
        if self.openai_done.is_some() { return None; }
        self.line_buf.extend_from_slice(chunk);
        while let Some(nl) = self.line_buf.iter().position(|b| *b == b'\n') {
          let line_bytes: Vec<u8> = self.line_buf.drain(..=nl).collect();
          let line = String::from_utf8_lossy(&line_bytes);
          match parse_openai_usage_from_sse(line.trim()) {
            Ok(Some(u)) => { self.openai_done = Some(u); return Some(u); }
            Ok(None) => {}
            Err(_) => { /* partial/garbled line mid-stream; ignore */ }
          }
        }
        None
      }
      Protocol::Anthropic => {
        self.anthropic.feed(chunk);
        if self.anthropic.is_complete() { Some(self.anthropic.take()) } else { None }
      }
    }
  }
}
