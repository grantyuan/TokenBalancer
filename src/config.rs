// src/config.rs
use std::collections::HashMap;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Region { #[default] Cn, Intl }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BalanceUnit { #[default] Tokens, Credits }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SeatTier { #[default] Standard, Pro, Max }

impl SeatTier {
  /// Monthly credits for the tier (Token Plan 团队版 seat quotas).
  pub fn monthly_credits(self) -> f64 {
    match self {
      SeatTier::Standard => 25_000.0,
      SeatTier::Pro => 100_000.0,
      SeatTier::Max => 250_000.0,
    }
  }
}

pub fn base_url_openai(r: Region) -> &'static str {
  match r {
    Region::Cn => "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
    Region::Intl => "https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1",
  }
}
pub fn base_url_anthropic(r: Region) -> &'static str {
  match r {
    Region::Cn => "https://token-plan.cn-beijing.maas.aliyuncs.com/apps/anthropic",
    Region::Intl => "https://token-plan.ap-southeast-1.maas.aliyuncs.com/apps/anthropic",
  }
}

fn default_listen() -> String { "127.0.0.1:8787".into() }
fn default_db_path() -> String { "data/tokenbalancer.sqlite".into() }
fn default_queue() -> u64 { 5 }
fn default_maxc() -> u32 { 2 }

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConf {
  #[serde(default = "default_listen")] pub listen: String,
  pub admin_key: String,
  #[serde(default = "default_db_path")] pub db_path: String,
  #[serde(default = "default_queue")] pub queue_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DefaultsConf {
  #[serde(default)] pub region: Region,
  #[serde(default)] pub balance_unit: BalanceUnit,
  #[serde(default = "default_maxc")] pub max_concurrent: u32,
}
impl Default for DefaultsConf {
  fn default() -> Self { Self { region: Region::default(), balance_unit: BalanceUnit::default(), max_concurrent: default_maxc() } }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModalityRates {
  /// Tokens per credit for each modality (higher = cheaper).
  #[serde(default = "def_input")] pub input: f64,
  #[serde(default = "def_cached")] pub cached: f64,
  #[serde(default = "def_output")] pub output: f64,
}
fn def_input() -> f64 { 500.0 }
fn def_cached() -> f64 { 2500.0 }
fn def_output() -> f64 { 100.0 }
impl Default for ModalityRates {
  fn default() -> Self { Self { input: def_input(), cached: def_cached(), output: def_output() } }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreditRatesConf {
  #[serde(default)] pub default: ModalityRates,
  #[serde(default)] pub models: HashMap<String, ModalityRates>,
}
impl Default for CreditRatesConf {
  fn default() -> Self { Self { default: ModalityRates::default(), models: HashMap::new() } }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountConf {
  pub id: String,
  #[serde(default)] pub label: Option<String>,
  pub api_key: String,
  #[serde(default)] pub region: Option<Region>,
  #[serde(default)] pub base_url_openai: Option<String>,
  #[serde(default)] pub base_url_anthropic: Option<String>,
  #[serde(default)] pub seat_tier: Option<SeatTier>,
  #[serde(default)] pub balance_unit: Option<BalanceUnit>,
  /// In the unit's terms: credits if balance_unit=credits, tokens if tokens.
  #[serde(default)] pub monthly_quota: Option<f64>,
  /// "YYYY-MM-DD" start of the usage window (default: 1st of current month, UTC+8).
  #[serde(default)] pub cycle_start: Option<String>,
  #[serde(default)] pub max_concurrent: Option<u32>,
  #[serde(default)] pub disabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserConf { pub key: String, #[serde(default)] pub name: String }

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
  #[serde(default)] pub server: ServerConf,
  #[serde(default)] pub defaults: DefaultsConf,
  #[serde(default)] pub accounts: Vec<AccountConf>,
  #[serde(default)] pub credit_rates: CreditRatesConf,
  #[serde(default)] pub users: Vec<UserConf>,
}

impl ServerConf { fn default() -> Self { Self { listen: default_listen(), admin_key: String::new(), db_path: default_db_path(), queue_timeout_secs: default_queue() } } }
impl Default for ServerConf { fn default() -> Self { Self::default() } }

/// Resolve an account's effective region (account -> defaults).
pub fn account_region(a: &AccountConf, d: &DefaultsConf) -> Region { a.region.unwrap_or(d.region) }
/// Effective balance unit.
pub fn account_balance_unit(a: &AccountConf, d: &DefaultsConf) -> BalanceUnit { a.balance_unit.unwrap_or(d.balance_unit) }
/// Effective max concurrent.
pub fn account_max_concurrent(a: &AccountConf, d: &DefaultsConf) -> u32 { a.max_concurrent.unwrap_or(d.max_concurrent).max(1) }
/// Effective monthly quota in the account's unit:
/// explicit monthly_quota > seat_tier credits (for credits unit; for tokens unit
/// the tier credits * 1000 tokens/credit estimate) > error at runtime.
pub fn account_monthly_quota(a: &AccountConf, d: &DefaultsConf) -> Option<f64> {
  if let Some(q) = a.monthly_quota { return Some(q); }
  if let Some(t) = a.seat_tier {
    let cr = t.monthly_credits();
    return Some(match account_balance_unit(a, d) {
      BalanceUnit::Credits => cr,
      BalanceUnit::Tokens => cr * 1000.0, // documented estimate
    });
  }
  None
}

pub fn load_str(text: &str) -> anyhow::Result<Config> {
  let c: Config = toml::from_str(text)?;
  validate(&c)?;
  Ok(c)
}
pub fn load(path: &str) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("read {path}: {e}"))?;
    let c: Config = toml::from_str(&text).map_err(|e| anyhow::anyhow!("parse {path}: {e}"))?;
    validate(&c)?;
    Ok(c)
}
fn validate(c: &Config) -> anyhow::Result<()> {
  if c.server.admin_key.is_empty() { anyhow::bail!("server.admin_key is required"); }
  for a in &c.accounts { if a.api_key.trim().is_empty() { anyhow::bail!("account {} has empty api_key", a.id); } }
  Ok(())
}
