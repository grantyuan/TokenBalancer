// tests/credit_test.rs
use std::collections::HashMap;
use tokenbalancer::config::{CreditRatesConf, ModalityRates};
use tokenbalancer::credit::credits_for;

fn rates(models: &[(&str, (f64, f64, f64))]) -> CreditRatesConf {
  let mut m = HashMap::new();
  for (name, (i, c, o)) in models {
    m.insert(name.to_string(), ModalityRates { input: *i, cached: *c, output: *o });
  }
  CreditRatesConf { default: ModalityRates { input: 500.0, cached: 2500.0, output: 100.0 }, models: m }
}

#[test]
fn uses_model_rate_when_present() {
  // qwen3.7-max: input 250 tokens/credit, cached 1250, output 40
  let r = rates(&[("qwen3.7-max", (250.0, 1250.0, 40.0))]);
  // input includes cached: 1000 in, 500 cached -> non-cached 500
  let cr = credits_for("qwen3.7-max", &r, 1000, 500, 200);
  let expect = 500.0 / 250.0 + 500.0 / 1250.0 + 200.0 / 40.0;
  assert!((cr - expect).abs() < 1e-9);
}

#[test]
fn falls_back_to_default_rate() {
  let r = rates(&[]);
  let cr = credits_for("some-model", &r, 1000, 0, 100);
  let expect = 1000.0 / 500.0 + 100.0 / 100.0;
  assert!((cr - expect).abs() < 1e-9);
}

#[test]
fn cached_never_exceeds_input() {
  let r = rates(&[]);
  // cached > input is clamped to input: non-cached part is 0
  let cr = credits_for("m", &r, 100, 500, 0);
  let expect = 100.0 / 2500.0;
  assert!((cr - expect).abs() < 1e-9);
}

#[test]
fn zero_tokens_zero_credits() {
  let r = rates(&[]);
  assert_eq!(credits_for("m", &r, 0, 0, 0), 0.0);
}
