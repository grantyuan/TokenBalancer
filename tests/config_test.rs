// tests/config_test.rs
use tokenbalancer::config::*;

#[test]
fn parses_full_config() {
  let toml_str = r#"
[server]
listen = "0.0.0.0:8787"
admin_key = "tba_test_admin"
db_path = "data/test.sqlite"
queue_timeout_secs = 5

[defaults]
region = "cn"
balance_unit = "tokens"
max_concurrent = 2

[[accounts]]
id = "acct-1"
label = "seat A"
api_key = "sk-sp-aaaa"
region = "cn"
seat_tier = "pro"
balance_unit = "credits"
monthly_quota = 100000
max_concurrent = 3

[[accounts]]
id = "acct-2"
api_key = "sk-sp-bbbb"
region = "intl"
base_url_openai = "https://custom.example.com/v1"

[credit_rates.default]
input = 500
cached = 2500
output = 100

[credit_rates.models."qwen3.7-max"]
input = 250
cached = 1250
output = 40

[[users]]
key = "tbu_seeduser1"
name = "alice"
"#;
  let c: Config = toml::from_str(toml_str).expect("parse");
  assert_eq!(c.server.listen, "0.0.0.0:8787");
  assert_eq!(c.server.admin_key, "tba_test_admin");
  assert_eq!(c.server.queue_timeout_secs, 5);
  assert_eq!(c.defaults.region, Region::Cn);
  assert_eq!(c.defaults.balance_unit, BalanceUnit::Tokens);
  assert_eq!(c.defaults.max_concurrent, 2);
  assert_eq!(c.accounts.len(), 2);
  let a1 = &c.accounts[0];
  assert_eq!(a1.id, "acct-1");
  assert_eq!(a1.seat_tier, Some(SeatTier::Pro));
  assert_eq!(a1.balance_unit, Some(BalanceUnit::Credits));
  assert_eq!(a1.monthly_quota, Some(100000.0));
  assert_eq!(a1.max_concurrent, Some(3));
  assert!(a1.label.as_deref() == Some("seat A"));
  let a2 = &c.accounts[1];
  assert_eq!(a2.region, Some(Region::Intl));
  assert!(a2.base_url_openai.as_deref() == Some("https://custom.example.com/v1"));
  assert!(a2.label.is_none() && a2.seat_tier.is_none());
  assert_eq!(c.credit_rates.default.input, 500.0);
  assert_eq!(c.credit_rates.models["qwen3.7-max"].output, 40.0);
  assert_eq!(c.users[0].key, "tbu_seeduser1");
  assert_eq!(c.users[0].name, "alice");
}

#[test]
fn defaults_fill_missing_fields() {
  let c: Config = toml::from_str(
    r#"
[server]
admin_key = "tba_x"

[[accounts]]
id = "a"
api_key = "sk-sp-z"
"#).unwrap();
  assert_eq!(c.server.listen, "127.0.0.1:8787");
  assert_eq!(c.server.db_path, "data/tokenbalancer.sqlite");
  assert_eq!(c.defaults.region, Region::Cn);
  assert_eq!(c.defaults.max_concurrent, 2);
  let a = &c.accounts[0];
  assert!(a.region.is_none() && a.seat_tier.is_none());
}

#[test]
fn region_base_urls() {
  assert!(base_url_openai(Region::Cn).contains("cn-beijing"));
  assert!(base_url_openai(Region::Intl).contains("ap-southeast-1"));
  assert!(base_url_anthropic(Region::Cn).ends_with("/apps/anthropic"));
}

#[test]
fn rejects_empty_admin_key() {
  // minimal: config with [server] but no admin_key must fail
  let r = load_str("# no admin key\n[server]\n");
  assert!(r.is_err());
}
