// src/credit.rs
use crate::config::CreditRatesConf;

/// Credits consumed by one request.
/// `input` is total input tokens (may include `cached`, as OpenAI reports);
/// cached tokens are priced separately (cheaper) and split out.
/// Rates are *tokens per credit* (higher = cheaper).
pub fn credits_for(model: &str, rates: &CreditRatesConf, input: u64, cached: u64, output: u64) -> f64 {
  let r = rates.models.get(model).unwrap_or(&rates.default);
  let cached = cached.min(input);
  let non_cached_input = input - cached;
  non_cached_input as f64 / r.input + cached as f64 / r.cached + output as f64 / r.output
}
