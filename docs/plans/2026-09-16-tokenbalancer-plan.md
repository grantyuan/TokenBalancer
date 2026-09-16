# TokenBalancer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** A single-binary Rust LLM proxy that auto-balances requests across multiple Qwen "Token Plan 团队版" accounts with per-account concurrency caps, self-usage accounting, and a web management UI.

**Architecture:** One axum server on one port serves (a) an OpenAI-compatible proxy (/v1/*) and Anthropic-compatible proxy (/apps/anthropic/*), (b) an embedded static web UI, (c) a JSON API. Each request is authenticated by a downstream proxy key, routed to the upstream account with the highest remaining-quota fraction that has a free in-flight slot, forwarded (SSE-aware), and usage recorded from the response. SQLite (rusqlite) persists accounts/users/usage; config.toml seeds static config.

**Tech Stack:** Rust 2021 (cargo 1.98), tokio, axum 0.7, reqwest 0.12 (rustls, stream), rusqlite (bundled), serde, serde_json, toml 0.8, chrono, rand, tracing, futures-util, url, anyhow, http, bytes, tower-http; dev-dep tower (util, for oneshot in tests).

**Worktree:** /home/yus/work/TokenBalancer/.worktrees/feature/tokenbalancer (branch feature/tokenbalancer)

**Conventions:**
- TDD: failing test first, minimal impl, green, commit. Every task ends with a commit.
- Run tests with: cargo test (from the worktree root)
- Run a specific test: cargo test <name>
- All file paths below are relative to the worktree root.
- The design doc (docs/plans/2026-09-16-tokenbalancer-design.md) is the source of truth for behavior; this plan is the build order.
- Keep deps minimal; no web build step (vanilla JS/CSS embedded via include_bytes!).

## File map (final)

```
Cargo.toml
config.example.toml
.gitignore                     (inherited from main; ensure present)
src/main.rs                    CLI: serve (default) | key new | admin-key
src/config.rs                  config.toml types + load + region base URLs
src/store.rs                   SQLite: accounts, users, usage_events
src/credit.rs                  pure token->credit conversion
src/usage.rs                   pure usage parsers (OpenAI JSON/SSE, Anthropic SSE)
src/balance.rs                 pure account selection
src/state.rs                   runtime accounts, in-flight slots, remaining calc
src/forward.rs                 upstream forwarding (plain + SSE), quota-429 detection
src/proxy.rs                   axum proxy routes (auth, routing, accounting)
src/web.rs                     static UI + JSON API routes
web/index.html, web/app.js, web/styles.css
tests/store_test.rs
tests/e2e_proxy_test.rs
```

---

### Task 1: Scaffold + config parsing

**Files:**
- Create: Cargo.toml
- Create: src/main.rs
- Create: src/config.rs
- Create: config.example.toml
- Test: tests/config_test.rs

**Step 1: Write the failing test**

```rust
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
  let r = load_from_str("server.admin_key_missing");
  // minimal: config with [server] but no admin_key must fail
  let r = load_str("# no admin key
[server]
");
  assert!(r.is_err());
}
```

**Step 2: Run test to verify it fails**

Run: cargo test --test config_test
Expected: compile FAIL ("use of undeclared crate tokenbalancer" / no lib target).

**Step 3: Write Cargo.toml and lib.rs**

```toml
# Cargo.toml
[package]
name = "tokenbalancer"
version = "0.1.0"
edition = "2021"

[lib]
name = "tokenbalancer"
path = "src/lib.rs"

[[bin]]
name = "tokenbalancer"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["full"] }
axum = "0.7"
reqwest = { version = "0.12", default-features = false, features = ["json", "stream", "rustls-tls"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"
rusqlite = { version = "0.31", features = ["bundled"] }
chrono = { version = "0.4", features = ["serde"] }
rand = "0.8"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
futures-util = "0.3"
url = { version = "2", features = ["serde"] }
anyhow = "1"
http = "1"
bytes = "1"
tower-http = { version = "0.5", features = ["cors"] }

[dev-dependencies]
tower = { version = "0.5", features = ["util"] }
```

```rust
// src/lib.rs
pub mod balance;
pub mod config;
pub mod credit;
pub mod forward;
pub mod proxy;
pub mod state;
pub mod store;
pub mod usage;
pub mod web;
```

(NOTE: lib.rs references modules not yet written — create empty placeholder files
`src/{balance,credit,forward,proxy,state,store,usage,web}.rs` containing just
`// implemented in Task N` so the crate compiles; they will be replaced in their tasks.)

**Step 4: Implement src/config.rs**

```rust
// src/config.rs
use std::collections::HashMap;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Region { Cn, #[default] Cn, Intl }

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

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DefaultsConf {
  #[serde(default)] pub region: Region,
  #[serde(default)] pub balance_unit: BalanceUnit,
  #[serde(default = "default_maxc")] pub max_concurrent: u32,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ModalityRates {
  /// Tokens per credit for each modality (higher = cheaper).
  #[serde(default = "def_input")] pub input: f64,
  #[serde(default = "def_cached")] pub cached: f64,
  #[serde(default = "def_output")] pub output: f64,
}
fn def_input() -> f64 { 500.0 }
fn def_cached() -> f64 { 2500.0 }
fn def_output() -> f64 { 100.0 }

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
pub fn load(path: &str) -> anyhow::Result<Config> { load_str(&std::fs::read_to_string(path).unwrap_or_else(|e| anyhow::bail!("read {path}: {e}"))) }
fn validate(c: &Config) -> anyhow::Result<()> {
  if c.server.admin_key.is_empty() { anyhow::bail!("server.admin_key is required"); }
  for a in &c.accounts { if a.api_key.trim().is_empty() { anyhow::bail!("account {} has empty api_key", a.id); } }
  Ok(())
}
```

**Step 5: Write main.rs skeleton**

```rust
// src/main.rs
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env().add_directive("tokenbalancer=info".into())).init();
  let args: Vec<String> = std::env::args().collect();
  let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("serve");
  match cmd {
    "serve" => {
      let path = args.get(2).cloned().unwrap_or_else(|| "config.toml".into());
      let cfg = tokenbalancer::config::load(&path)?;
      println!("tokenbalancer: {} account(s), config OK (listen {})", cfg.accounts.len(), cfg.server.listen);
      // full server starts in Task 9; until then just report
      Ok(())
    }
    _ => { eprintln!("usage: tokenbalancer [serve [config.toml]]"); Ok(()) }
  }
}
```

**Step 6: Write config.example.toml**

```toml
# TokenBalancer configuration — all Token Plan keys live here (keep this file private!).
[server]
listen = "0.0.0.0:8787"
admin_key = "tba_replace_me"        # grants web-admin + may also proxy (attribution "admin")
db_path = "data/tokenbalancer.sqlite"
queue_timeout_secs = 5              # wait for a free slot before 503

[defaults]
region = "cn"                       # "cn" (千问云) or "intl" (QwenCloud)
balance_unit = "tokens"             # "tokens" or "credits"
max_concurrent = 2                  # max simultaneous requests per account

# Each Token Plan seat key (sk-sp-...) becomes one account.
[[accounts]]
id = "seat-a"
label = "张三-标准席"
api_key = "sk-sp_replace_me_1"
region = "cn"
seat_tier = "standard"              # standard|pro|max → default quota
# monthly_quota = 25000             # explicit override, in balance_unit terms
# cycle_start = "2026-09-01"        # usage window start (default: 1st of month, UTC+8)
# max_concurrent = 2
# balance_unit = "credits"
# base_url_openai = "..."           # per-account base URL override (rare)
# disabled = false

[[accounts]]
id = "seat-b"
api_key = "sk-sp_replace_me_2"
region = "intl"
monthly_quota = 10000000            # tokens (balance_unit=tokens default)

# token->credit rates (tokens per credit). Defaults are conservative.
[credit_rates.default]
input = 500
cached = 2500
output = 100

[credit_rates.models."qwen3.7-max"]
input = 250
cached = 1250
output = 40

# Optional seed users (proxy keys). More are created in the web UI (stored in DB).
[[users]]
key = "tbu_alice"
name = "alice"
```

**Step 7: Run tests**

Run: cargo test --test config_test
Expected: PASS (4 tests)

**Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock config.example.toml src/ tests/config_test.rs
git commit -m "feat: project scaffold + config parsing (T1)"
```

---

### Task 2: SQLite store

**Files:**
- Create: src/store.rs (replace placeholder)
- Test: tests/store_test.rs

**Step 1: Write the failing test**

```rust
// tests/store_test.rs
use tokenbalancer::store::*;
use chrono::Utc;

fn acct(id: &str) -> AccountRow {
  AccountRow { id: id.into(), label: "l".into(), api_key: "sk-sp-x".into(), region: "cn".into(),
    base_url_openai: "https://b/v1".into(), base_url_anthropic: "https://b/apps/anthropic".into(),
    seat_tier: "standard".into(), balance_unit: "tokens".into(), monthly_quota: 1000.0,
    cycle_start: "2026-09-01".into(), max_concurrent: 2,
    disabled: false, exhausted: false, reconciled_at: None, reconciled_remaining: None }
}

#[test]
fn accounts_upsert_and_flags() {
  let s = Store::open(":memory:").unwrap();
  s.upsert_account(&acct("a1")).unwrap();
  let rows = s.list_accounts().unwrap();
  assert_eq!(rows.len(), 1) && assert_eq!(rows[0].id, "a1");
  // upsert again with different static fields keeps flags
  let mut a2 = acct("a1"); a2.label = "new".into(); a2.max_concurrent = 5;
  s.set_flags("a1", true, false).unwrap();
  s.upsert_account(&a2).unwrap();
  let rows = s.list_accounts().unwrap();
  assert_eq!(rows[0].label, "new");
  assert_eq!(rows[0].max_concurrent, 5);
  assert!(rows[0].disabled); // flag preserved by upsert
  s.set_flags("a1", false, true).unwrap();
  assert!(s.list_accounts().unwrap()[0].exhausted);
}

#[test]
fn users_crud() {
  let s = Store::open(":memory:").unwrap();
  s.create_user("tbu_1", "alice").unwrap();
  s.create_user("tbu_2", "bob").unwrap();
  assert_eq!(s.find_user_by_key("tbu_1").unwrap().unwrap().name, "alice");
  assert!(s.find_user_by_key("tbu_missing").unwrap().is_none());
  assert_eq!(s.list_users().unwrap().len(), 2);
  s.revoke_user("tbu_1").unwrap();
  assert!(s.find_user_by_key("tbu_1").unwrap().unwrap().revoked);
  assert!(s.find_live_user_by_key("tbu_1").unwrap().is_none());
}

#[test]
fn events_and_sums() {
  let s = Store::open(":memory:").unwrap();
  s.upsert_account(&acct("a1")).unwrap();
  s.insert_event(&UsageEvent { ts: Utc::now(), user_id: "u1".into(), account_id: "a1".into(),
    model: "qwen3.7-max".into(), input: 100, cached: 20, output: 50, credits: 0.5,
    latency_ms: 120, status: 200, stream: true, parse_error: false }).unwrap();
  s.insert_event(&UsageEvent { ts: Utc::now(), user_id: "u1".into(), account_id: "a1".into(),
    model: "qwen3.7-max".into(), input: 300, cached: 0, output: 100, credits: 1.2,
    latency_ms: 80, status: 200, stream: false, parse_error: false }).unwrap();
  let (i, c, o, cr, n) = s.sum_since("a1", Utc::now().timestamp() - 3600).unwrap();
  assert_eq!((i, c, o, cr, n), (400, 20, 150, 1.7, 2));
  // future since → nothing
  let (i2, _, _, _, n2) = s.sum_since("a1", Utc::now().timestamp() + 3600).unwrap();
  assert_eq!((i2, n2), (0, 0));
}
```

**Step 2: Run test to verify it fails**

Run: cargo test --test store_test
Expected: FAIL (store not implemented).

**Step 3: Implement src/store.rs**

```rust
// src/store.rs
use std::sync::Mutex;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

pub struct Store { conn: Mutex<Connection> }

#[derive(Debug, Clone)]
pub struct AccountRow {
  pub id: String, pub label: String, pub api_key: String, pub region: String,
  pub base_url_openai: String, pub base_url_anthropic: String, pub seat_tier: String,
  pub balance_unit: String, pub monthly_quota: f64, pub cycle_start: String,
  pub max_concurrent: u32, pub disabled: bool, pub exhausted: bool,
  pub reconciled_at: Option<String>, pub reconciled_remaining: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct UserRow { pub id: String, pub key: String, pub name: String, pub created_at: String, pub revoked: bool }

#[derive(Debug, Clone)]
pub struct UsageEvent {
  pub ts: DateTime<Utc>, pub user_id: String, pub account_id: String, pub model: String,
  pub input: u64, pub cached: u64, pub output: u64, pub credits: f64,
  pub latency_ms: u64, pub status: u16, pub stream: bool, pub parse_error: bool,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS accounts(
  id TEXT PRIMARY KEY, label TEXT NOT NULL, api_key TEXT NOT NULL, region TEXT NOT NULL,
  base_url_openai TEXT NOT NULL, base_url_anthropic TEXT NOT NULL, seat_tier TEXT NOT NULL,
  balance_unit TEXT NOT NULL, monthly_quota REAL NOT NULL, cycle_start TEXT NOT NULL,
  max_concurrent INTEGER NOT NULL, disabled INTEGER NOT NULL DEFAULT 0,
  exhausted INTEGER NOT NULL DEFAULT 0, reconciled_at TEXT, reconciled_remaining REAL);
CREATE TABLE IF NOT EXISTS users(
  id TEXT PRIMARY KEY, key TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
  created_at TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS usage_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, user_id TEXT NOT NULL,
  account_id TEXT NOT NULL, model TEXT NOT NULL, input INTEGER NOT NULL,
  cached INTEGER NOT NULL, output INTEGER NOT NULL, credits REAL NOT NULL,
  latency_ms INTEGER NOT NULL, status INTEGER NOT NULL, stream INTEGER NOT NULL,
  parse_error INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_ev_acct_ts ON usage_events(account_id, ts);
CREATE INDEX IF NOT EXISTS idx_ev_user_ts ON usage_events(user_id, ts);
"#;

fn ts(dt: DateTime<Utc>) -> String { dt.to_rfc3339() }

impl Store {
  pub fn open(path: &str) -> anyhow::Result<Self> {
    if path != ":memory:" {
      if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent).ok();
      }
    }
    let conn = Connection::open(path)?;
    conn.execute_batch(SCHEMA)?;
    Ok(Self { conn: Mutex::new(conn) })
  }

  pub fn upsert_account(&self, a: &AccountRow) -> anyhow::Result<()> {
    let c = self.conn.lock().unwrap();
    c.execute(
      "INSERT INTO accounts(id,label,api_key,region,base_url_openai,base_url_anthropic,
         seat_tier,balance_unit,monthly_quota,cycle_start,max_concurrent)
       VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
       ON CONFLICT(id) DO UPDATE SET label=excluded.label, api_key=excluded.api_key,
         region=excluded.region, base_url_openai=excluded.base_url_openai,
         base_url_anthropic=excluded.base_url_anthropic, seat_tier=excluded.seat_tier,
         balance_unit=excluded.balance_unit, monthly_quota=excluded.monthly_quota,
         cycle_start=excluded.cycle_start, max_concurrent=excluded.max_concurrent",
      params![a.id, a.label, a.api_key, a.region, a.base_url_openai, a.base_url_anthropic,
        a.seat_tier, a.balance_unit, a.monthly_quota, a.cycle_start, a.max_concurrent as i64])?;
    Ok(())
  }

  pub fn set_flags(&self, id: &str, disabled: bool, exhausted: bool) -> anyhow::Result<()> {
    self.conn.lock().unwrap()
      .execute("UPDATE accounts SET disabled=?1, exhausted=?2 WHERE id=?3",
        params![disabled as i64, exhausted as i64, id])?;
    Ok(())
  }

  pub fn reconcile(&self, id: &str, at: DateTime<Utc>, remaining: f64) -> anyhow::Result<()> {
    self.conn.lock().unwrap()
      .execute("UPDATE accounts SET reconciled_at=?1, reconciled_remaining=?2 WHERE id=?3",
        params![ts(at), remaining, id])?;
    Ok(())
  }

  pub fn list_accounts(&self) -> anyhow::Result<Vec<AccountRow>> {
    let c = self.conn.lock().unwrap();
    let mut st = c.prepare("SELECT id,label,api_key,region,base_url_openai,base_url_anthropic,seat_tier,balance_unit,monthly_quota,cycle_start,max_concurrent,disabled,exhausted,reconciled_at,reconciled_remaining FROM accounts ORDER BY id")?;
    let rows = st.query_map([], |r| {
      Ok(AccountRow {
        id: r.get(0)?, label: r.get(1)?, api_key: r.get(2)?, region: r.get(3)?,
        base_url_openai: r.get(4)?, base_url_anthropic: r.get(5)?, seat_tier: r.get(6)?,
        balance_unit: r.get(7)?, monthly_quota: r.get(8)?, cycle_start: r.get(9)?,
        max_concurrent: r.get::<_, i64>(10)? as u32, disabled: r.get::<_, i64>(11)? != 0,
        exhausted: r.get::<_, i64>(12)? != 0,
        reconciled_at: r.get(13)?, reconciled_remaining: r.get(14)?,
      })
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
  }

  pub fn create_user(&self, key: &str, name: &str) -> anyhow::Result<()> {
    self.conn.lock().unwrap()
      .execute("INSERT INTO users(id,key,name,created_at) VALUES(?1,?2,?3,?4)",
        params![key, key, name, ts(Utc::now())])?;
    Ok(())
  }

  pub fn revoke_user(&self, key: &str) -> anyhow::Result<()> {
    self.conn.lock().unwrap()
      .execute("UPDATE users SET revoked=1 WHERE key=?1", params![key])?;
    Ok(())
  }

  pub fn find_user_by_key(&self, key: &str) -> anyhow::Result<Option<UserRow>> {
    let c = self.conn.lock().unwrap();
    let mut st = c.prepare("SELECT id,key,name,created_at,revoked FROM users WHERE key=?1")?;
    let mut it = st.query_map(params![key], |r| {
      Ok(UserRow { id: r.get(0)?, key: r.get(1)?, name: r.get(2)?, created_at: r.get(3)?, revoked: r.get::<_, i64>(4)? != 0 })
    })?;
    Ok(it.next().transpose().ok())
  }

  pub fn find_live_user_by_key(&self, key: &str) -> anyhow::Result<Option<UserRow>> {
    Ok(self.find_user_by_key(key)?.filter(|u| !u.revoked))
  }

  pub fn list_users(&self) -> anyhow::Result<Vec<UserRow>> {
    let c = self.conn.lock().unwrap();
    let mut st = c.prepare("SELECT id,key,name,created_at,revoked FROM users ORDER BY created_at")?;
    let rows = st.query_map([], |r| {
      Ok(UserRow { id: r.get(0)?, key: r.get(1)?, name: r.get(2)?, created_at: r.get(3)?, revoked: r.get::<_, i64>(4)? != 0 })
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
  }

  pub fn insert_event(&self, e: &UsageEvent) -> anyhow::Result<()> {
    self.conn.lock().unwrap().execute(
      "INSERT INTO usage_events(ts,user_id,account_id,model,input,cached,output,credits,latency_ms,status,stream,parse_error)
       VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
      params![ts(e.ts), e.user_id, e.account_id, e.model, e.input as i64, e.cached as i64,
        e.output as i64, e.credits, e.latency_ms as i64, e.status as i64, e.stream as i64, e.parse_error as i64])?;
    Ok(())
  }

  /// Sum (input, cached, output, credits, count) for an account since a unix-seconds boundary.
  /// `credits` is the SUM of per-event credits (computed at insert time by the
  /// same rate table), so the credits-unit balance needs no re-conversion.
  pub fn sum_since(&self, account_id: &str, since_unix: i64) -> anyhow::Result<(u64, u64, u64, f64, u64)> {
    let since = DateTime::<Utc>::from_timestamp(since_unix, 0).unwrap_or_default().to_rfc3339();
    let c = self.conn.lock().unwrap();
    let (i, ca, o, cr, n) = c.query_row(
      "SELECT COALESCE(SUM(input),0),COALESCE(SUM(cached),0),COALESCE(SUM(output),0),
         COALESCE(SUM(credits),0.0),COUNT(*)
       FROM usage_events WHERE account_id=?1 AND ts>=?2", params![account_id, since],
      |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? as u64,
             r.get::<_, f64>(3)?, r.get::<_, i64>(4)? as u64)))?;
    Ok((i, ca, o, cr, n))
  }

  /// Daily aggregates: (date, events, input, cached, output) between two unix-seconds bounds.
  pub fn daily_stats(&self, from_unix: i64, to_unix: i64) -> anyhow::Result<Vec<(String, u64, u64, u64, u64)>> {
    let f = DateTime::<Utc>::from_timestamp(from_unix, 0).unwrap_or_default().to_rfc3339();
    let t = DateTime::<Utc>::from_timestamp(to_unix, 0).unwrap_or_default().to_rfc3339();
    let c = self.conn.lock().unwrap();
    let mut st = c.prepare("SELECT substr(ts,1,10) d, COUNT(*), COALESCE(SUM(input),0), COALESCE(SUM(cached),0), COALESCE(SUM(output),0) FROM usage_events WHERE ts>=?1 AND ts<?2 GROUP BY d ORDER BY d")?;
    let rows = st.query_map(params![f, t], |r| {
      Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? as u64, r.get::<_, i64>(3)? as u64, r.get::<_, i64>(4)? as u64))
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
  }
}
```

**Step 4: Run tests**

Run: cargo test --test store_test
Expected: PASS (3 tests)

**Step 5: Commit**

```bash
git add src/store.rs tests/store_test.rs
git commit -m "feat: sqlite store for accounts/users/usage (T2)"
```

---

### Task 3: Credit conversion (pure)

**Files:**
- Create: `src/credit.rs` (replace placeholder)
- Test: `tests/credit_test.rs`

**Step 1: Write the failing test**

```rust
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
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test credit_test`
Expected: FAIL (credit module empty).

**Step 3: Implement src/credit.rs**

```rust
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
```

**Step 4: Run tests**

Run: `cargo test --test credit_test`
Expected: PASS (4 tests)

**Step 5: Commit**

```bash
git add src/credit.rs tests/credit_test.rs
git commit -m "feat: pure token->credit conversion (T3)"
```

---

### Task 4: Usage parsers (OpenAI + Anthropic)

**Files:**
- Create: `src/usage.rs` (replace placeholder)
- Test: `tests/usage_test.rs`

**Step 1: Write the failing test**

```rust
// tests/usage_test.rs
use tokenbalancer::usage::{parse_openai_usage, parse_openai_usage_from_sse, AnthropicStreamParser, UsageTokens};

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
  assert!(parse_openai_usage_from_sse("data: {\"choices\":[{\"delta\":{}}}]}").unwrap().is_none());
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
```

NOTE: test code uses ordinary Rust string escapes (backslash-quote
for quotes inside non-raw literals). Copy the code as-is into the .rs file.

**Step 2: Run test to verify it fails**

Run: `cargo test --test usage_test`
Expected: FAIL.

**Step 3: Implement src/usage.rs**

```rust
// src/usage.rs
use serde_json::Value;

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

  pub fn finish(self) -> UsageTokens {
    if self.saw_start && self.saw_delta {
      UsageTokens { input: self.input, cached: self.cached, output: self.output, parse_error: false }
    } else {
      UsageTokens::missing()
    }
  }
}

/// First index i such that hay[i..i+needle.len()] == needle, else None.
fn find_subseq(hay: &[u8], needle: &[u8]) -> Option<usize> {
  if needle.len() > hay.len() { return None; }
  (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}
```

NOTE: OpenAI stream usage only appears in the final chunk when
`stream_options.include_usage=true`. The forwarder (Task 8) must inject/merge that
option into `stream` requests before forwarding so usage is always captured.

**Step 4: Run tests**

Run: `cargo test --test usage_test`
Expected: PASS (6 tests).

**Step 5: Commit**

```bash
git add src/usage.rs tests/usage_test.rs
git commit -m "feat: usage parsers for OpenAI + Anthropic (T4)"
```

---

### Task 5: Balancer selection (pure)

**Files:**
- Create: `src/balance.rs` (replace placeholder)
- Test: `tests/balance_test.rs`

**Step 1: Write the failing test**

```rust
// tests/balance_test.rs
use tokenbalancer::balance::{select, AccountSnapshot, Selection};

fn snap(idx: usize, rem: f64, inflight: u32, max: u32) -> AccountSnapshot {
  AccountSnapshot { idx, remaining_pct: rem, in_flight: inflight, max_concurrent: max, disabled: false, exhausted: false }
}

fn with_exhausted(mut s: AccountSnapshot) -> AccountSnapshot { s.exhausted = true; s }

#[test]
fn picks_highest_remaining_with_free_slot() {
  let accts = vec![snap(0, 0.3, 0, 2), snap(1, 0.8, 1, 2), snap(2, 0.6, 0, 2)];
  assert!(matches!(select(&accts, &mut rand::thread_rng()), Selection::Chosen(1)));
}

#[test]
fn skips_full_exhausted_disabled() {
  // a is slot-full (2/2); b exhausted -> only live account is full -> AllSlotsFull
  let accts = vec![snap(0, 0.9, 2, 2), with_exhausted(snap(1, 0.9, 0, 2))];
  assert!(matches!(select(&accts, &mut rand::thread_rng()), Selection::AllSlotsFull));
}

#[test]
fn all_dead_means_none_available() {
  let mut a = snap(0, 0.5, 0, 2); a.disabled = true;
  let mut b = snap(1, 0.5, 0, 2); b.exhausted = true;
  assert!(matches!(select(&[a, b], &mut rand::thread_rng()), Selection::NoneAvailable));
}

#[test]
fn empty_accounts_none_available() {
  assert!(matches!(select(&[], &mut rand::thread_rng()), Selection::NoneAvailable));
}

#[test]
fn ties_pick_among_candidates() {
  let accts = vec![snap(0, 0.5, 0, 2), snap(1, 0.5, 0, 2), snap(2, 0.5, 0, 2)];
  for _ in 0..50 {
    match select(&accts, &mut rand::thread_rng()) {
      Selection::Chosen(i) => assert!((0..3).contains(&i)),
      other => panic!("unexpected {other:?}"),
    }
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test balance_test`
Expected: FAIL.

**Step 3: Implement src/balance.rs**

```rust
// src/balance.rs
use rand::Rng;

#[derive(Debug, Clone, Copy)]
pub struct AccountSnapshot {
  /// Stable index of the account in the runtime list.
  pub idx: usize,
  /// Remaining fraction in [0,1]; higher = more quota left. 0 when exhausted.
  pub remaining_pct: f64,
  pub in_flight: u32,
  pub max_concurrent: u32,
  pub disabled: bool,
  pub exhausted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
  Chosen(usize),
  /// At least one live account exists but every live account is at its concurrency cap.
  AllSlotsFull,
  /// No usable account (all disabled/exhausted, or no accounts).
  NoneAvailable,
}

pub fn select<'a>(accounts: &'a [AccountSnapshot], rng: &mut impl Rng) -> Selection {
  let cands: Vec<usize> = (0..accounts.len())
    .filter(|&i| {
      let a = &accounts[i];
      !a.disabled && !a.exhausted && a.in_flight < a.max_concurrent
    })
    .collect();
  if cands.is_empty() {
    let any_live = accounts.iter().any(|a| !a.disabled && !a.exhausted);
    return if any_live { Selection::AllSlotsFull } else { Selection::NoneAvailable };
  }
  let best = cands.iter().fold(f64::NEG_INFINITY, |m, &i| m.max(accounts[i].remaining_pct));
  let ties: Vec<usize> = cands
    .into_iter()
    .filter(|&i| (accounts[i].remaining_pct - best).abs() < 1e-9)
    .collect();
  let pick = rng.gen_range(0..ties.len());
  Selection::Chosen(ties[pick])
}
```

**Step 4: Run tests**

Run: `cargo test --test balance_test`
Expected: PASS (5 tests).

**Step 5: Commit**

```bash
git add src/balance.rs tests/balance_test.rs
git commit -m "feat: pure account selection by remaining fraction (T5)"
```

---

### Task 6: Runtime state (accounts, slots, remaining via DB window)

**Files:**
- Create: `src/state.rs` (replace placeholder)
- Test: `tests/state_test.rs`

Glue layer: builds in-memory account records from config + store, tracks in-flight
slots (atomic counter + RAII guard), computes **remaining from the SQLite usage window**
(no in-memory consumed cache, no cycle-rollover job: the usage window is
`[cycle_start, now)` and remaining is derived on demand from
`store.sum_since`). Reconciliation and exhaustion are applied on top.

Design note (why no in-memory consumed counter): the usage window start
(`cycle_start`, default 1st of the current month in the region's zone) makes
`SUM(events since window_start)` the source of truth for "consumed this cycle".
This survives restarts for free and needs no rollover logic. Team scale (a handful
of accounts, low RPS) makes a per-request aggregate query affordable.

**Step 1: Write the failing test**

```rust
// tests/state_test.rs
use std::sync::Arc;
use chrono::Utc;
use tokenbalancer::balance::{self, Selection};
use tokenbalancer::config::*;
use tokenbalancer::state::Runtime;
use tokenbalancer::store::Store;

fn acct(id: &str, tier: SeatTier, maxc: Option<u32>) -> AccountConf {
  AccountConf { id: id.into(), label: None, api_key: "sk-sp-a".into(), region: None,
    base_url_openai: None, base_url_anthropic: None, seat_tier: Some(tier),
    balance_unit: None, monthly_quota: None, cycle_start: None,
    max_concurrent: maxc, disabled: None }
}

fn cfg(accounts: Vec<AccountConf>) -> Config {
  Config {
    server: ServerConf { listen: "127.0.0.1:0".into(), admin_key: "tba".into(), db_path: ":memory:".into(), queue_timeout_secs: 1 },
    defaults: DefaultsConf { region: Region::Cn, balance_unit: BalanceUnit::Credits, max_concurrent: 2 },
    accounts, credit_rates: CreditRatesConf::default(), users: vec![],
  }
}

fn ev(account: &str, model: &str, input: u64, output: u64) -> tokenbalancer::store::UsageEvent {
  tokenbalancer::store::UsageEvent {
    ts: Utc::now(), user_id: "tester".into(), account_id: account.into(), model: model.into(),
    input, cached: 0, output, credits: 0.0, latency_ms: 120, status: 200, stream: true, parse_error: false,
  }
}

#[tokio::test]
async fn slot_guard_decrements_on_drop() {
  let rt = Runtime::load(&cfg(vec![acct("a", SeatTier::Pro, Some(1))]), Arc::new(Store::open(":memory:").unwrap())).await.unwrap();
  let guard = rt.try_acquire("a").unwrap();
  assert_eq!(rt.in_flight("a"), 1);
  drop(guard);
  assert_eq!(rt.in_flight("a"), 0);
  // second acquire beyond cap must fail
  let g1 = rt.try_acquire("a").unwrap();
  assert!(rt.try_acquire("a").is_none());
  drop(g1);
}

#[tokio::test]
async fn remaining_shrinks_after_usage() {
  let rt = Runtime::load(&cfg(vec![acct("a", SeatTier::Pro, None)]), Arc::new(Store::open(":memory:").unwrap())).await.unwrap();
  let before = rt.remaining("a");
  assert_eq!(before, 100_000.0); // Pro seat, credits mode, nothing consumed yet
  rt.record_event(&ev("a", "qwen3.7-max", 1000, 500)).await;
  let after = rt.remaining("a");
  assert!(after < before, "remaining should shrink: before={before} after={after}");
  assert!(after > 99_000.0, "a tiny request must not eat the whole quota: {after}");
}

#[tokio::test]
async fn exhausted_reports_zero_remaining() {
  let rt = Runtime::load(&cfg(vec![acct("a", SeatTier::Pro, None)]), Arc::new(Store::open(":memory:").unwrap())).await.unwrap();
  rt.mark_exhausted("a").await;
  assert_eq!(rt.remaining("a"), 0.0);
  assert!(rt.build_snapshots().await.iter().any(|a| a.exhausted));
  // must be skipped by selection
  assert!(matches!(balance::select(&rt.build_snapshots().await, &mut rand::thread_rng()), Selection::NoneAvailable));
}

#[tokio::test]
async fn reconciliation_overrides_quota() {
  let rt = Runtime::load(&cfg(vec![acct("a", SeatTier::Pro, None)]), Arc::new(Store::open(":memory:").unwrap())).await.unwrap();
  // console says 50k credits left right now
  rt.reconcile("a", 50_000.0).await;
  assert_eq!(rt.remaining("a"), 50_000.0);
  rt.record_event(&ev("a", "qwen3.6-flash", 1_000_000, 0)).await;
  assert!(rt.remaining("a") < 50_000.0);
}

#[tokio::test]
async fn unknown_quota_is_neutral() {
  // no seat_tier, no monthly_quota -> quota None -> pct must be neutral 0.5, still selectable
  let c = AccountConf { id: "a".into(), label: None, api_key: "k".into(), region: None,
    base_url_openai: None, base_url_anthropic: None, seat_tier: None,
    balance_unit: None, monthly_quota: None, cycle_start: None,
    max_concurrent: None, disabled: None };
  let rt = Runtime::load(&cfg(vec![c]), Arc::new(Store::open(":memory:").unwrap())).await.unwrap();
  let snaps = rt.build_snapshots().await;
  assert_eq!(snaps[0].remaining_pct, 0.5);
  assert!(matches!(balance::select(&snaps, &mut rand::thread_rng()), Selection::Chosen(0)));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test state_test`
Expected: FAIL.

**Step 3: Implement src/state.rs**

```rust
// src/state.rs
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use chrono::{DateTime, TimeZone, Utc};

use crate::balance::AccountSnapshot;
use crate::config::{self as cfg_mod, BalanceUnit, Config, Region};
use crate::credit::credits_for;
use crate::store::{AccountRow, Store, UsageEvent};

pub struct Runtime {
  inner: Arc<Inner>,
}

struct Inner {
  id_to_idx: HashMap<String, usize>,
  accounts: Vec<Arc<RuntimeAccount>>,
  store: Arc<Store>,
  rates: cfg_mod::CreditRatesConf,
  admin_key: String,
}

pub struct RuntimeAccount {
  pub id: String,
  pub label: String,
  pub api_key: String,
  pub base_url_openai: String,
  pub base_url_anthropic: String,
  pub unit: BalanceUnit,
  pub quota: Option<f64>,      // in unit terms; None = unknown -> neutral pct 0.5
  pub window_start_unix: i64,  // usage window start (cycle start)
  pub max_concurrent: u32,
  disabled: AtomicBool,
  exhausted: AtomicBool,
  pub in_flight: AtomicU32,
  /// (at_unix, remaining in unit terms) — admin reconciliation baseline.
  reconciled: Mutex<Option<(i64, f64)>>,
}

pub struct Slot {
  rt: Arc<Runtime>,
  idx: usize,
}

impl Drop for Slot {
  fn drop(&mut self) {
    self.rt.inner.accounts[self.idx].in_flight.fetch_sub(1, Ordering::Release);
  }
}

impl std::fmt::Debug for Runtime {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Runtime").field("accounts", &self.inner.accounts.len()).finish()
  }
}

/// 1st of the current month in the region's zone (UTC+8 for Cn, UTC otherwise).
fn first_of_month(region: Region) -> i64 {
  let now = Utc::now();
  let off = match region {
    Region::Cn => chrono::FixedOffset::east_opt(8 * 3600).unwrap(),
    _ => chrono::FixedOffset::east_opt(0).unwrap(),
  };
  let local = off.from_utc_datetime(&now);
  let first_naive = chrono::NaiveDate::from((local.year(), local.month(), 1)).and_hms_opt(0, 0, 0).unwrap();
  off.from_local_datetime(&first_naive).unwrap().timestamp()
}

fn parse_date_unix(s: &str) -> i64 {
  match chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
    Ok(d) => d.and_hms_opt(0, 0, 0).map(|dt| dt.and_utc().timestamp())
      .unwrap_or_else(|| first_of_month(Region::Cn)),
    Err(_) => first_of_month(Region::Cn),
  }
}

/// Consumption in the account's unit terms from a sum_since row
/// (input, cached, output, credits, count).
fn unit_consumption(unit: &BalanceUnit, s: &(u64, u64, u64, f64, u64)) -> f64 {
  match unit {
    // credits: use the per-event credit sum stored at insert time (same rate table)
    BalanceUnit::Credits => s.3,
    // tokens: input already includes cached; total = input + output
    BalanceUnit::Tokens => (s.0 + s.2) as f64,
  }
}

impl Runtime {
  pub async fn load(c: &Config, store: Arc<Store>) -> anyhow::Result<Arc<Runtime>> {
    for a in &c.accounts {
      let region = cfg_mod::account_region(a, &c.defaults);
      let unit = cfg_mod::account_balance_unit(a, &c.defaults);
      let maxc = cfg_mod::account_max_concurrent(a, &c.defaults);
      let quota = cfg_mod::account_monthly_quota(a, &c.defaults).unwrap_or(0.0);
      let cycle_start = a.cycle_start.clone().unwrap_or_else(|| default_cycle_start(region));
      let row = AccountRow {
        id: a.id.clone(),
        label: a.label.clone().unwrap_or_else(|| a.id.clone()),
        api_key: a.api_key.clone(),
        region: region.to_string(),
        base_url_openai: a.base_url_openai.clone().unwrap_or_else(|| cfg_mod::base_url_openai(region).into()),
        base_url_anthropic: a.base_url_anthropic.clone().unwrap_or_else(|| cfg_mod::base_url_anthropic(region).into()),
        seat_tier: a.seat_tier.map(|t| format!("{t:?}")).unwrap_or_default(),
        balance_unit: format!("{unit:?}"),
        monthly_quota: quota,
        cycle_start,
        max_concurrent: maxc,
        disabled: false,
        exhausted: false,
        reconciled_at: None,
        reconciled_remaining: None,
      };
      store.upsert_account(&row)?;
    }
    // Seed users from config (idempotent — skip keys that already exist).
    for u in &c.users {
      if store.find_user_by_key(&u.key)?.is_none() {
        store.create_user(&u.key, &u.name)?;
      }
    }
    let rows = store.list_accounts()?;
    let accounts: Vec<Arc<RuntimeAccount>> = rows.into_iter().map(|r| {
      let unit = if r.balance_unit == "Credits" { BalanceUnit::Credits } else { BalanceUnit::Tokens };
      let region = if r.region == "Intl" { Region::Intl } else { Region::Cn };
      Arc::new(RuntimeAccount {
        id: r.id,
        label: r.label,
        api_key: r.api_key,
        base_url_openai: r.base_url_openai,
        base_url_anthropic: r.base_url_anthropic,
        unit,
        quota: if r.monthly_quota > 0.0 { Some(r.monthly_quota) } else { None },
        window_start_unix: parse_date_unix(&r.cycle_start).max(1).min(first_of_month(region)),
        max_concurrent: r.max_concurrent,
        disabled: AtomicBool::new(r.disabled),
        exhausted: AtomicBool::new(r.exhausted),
        in_flight: AtomicU32::new(0),
        reconciled: Mutex::new(r.reconciled_remaining.map(|rem| {
          let at = r.reconciled_at.as_deref().map(|s| DateTime::parse_from_rfc3339(s).map(|d| d.timestamp()).unwrap_or_else(|_| Utc::now().timestamp())).unwrap_or_else(|| Utc::now().timestamp());
          (at, rem)
        })),
      })
    }).collect();
    let mut id_to_idx = HashMap::new();
    for (i, a) in accounts.iter().enumerate() {
      id_to_idx.insert(a.id.clone(), i);
    }
    Ok(Arc::new(Runtime {
      inner: Arc::new(Inner {
        id_to_idx,
        accounts,
        store,
        rates: c.credit_rates.clone(),
        admin_key: c.server.admin_key.clone(),
      }),
    }))
  }

  pub fn account_count(&self) -> usize { self.inner.accounts.len() }
  pub fn store(&self) -> &Store { &self.inner.store }
  pub fn admin_key(&self) -> &str { &self.inner.admin_key }
  pub fn rates(&self) -> &cfg_mod::CreditRatesConf { &self.inner.rates }

  pub fn account(&self, id: &str) -> Option<Arc<RuntimeAccount>> {
    self.inner.id_to_idx.get(id).map(|&i| Arc::clone(&self.inner.accounts[i]))
  }
  pub fn account_by_idx(&self, i: usize) -> Option<Arc<RuntimeAccount>> {
    self.inner.accounts.get(i).cloned()
  }
  pub fn in_flight(&self, id: &str) -> u32 {
    self.account(id).map(|a| a.in_flight.load(Ordering::Acquire)).unwrap_or(0)
  }

  /// Try to take a concurrency slot for the account; None when at cap.
  pub fn try_acquire(&self, id: &str) -> Option<Slot> {
    let idx = *self.inner.id_to_idx.get(id)?;
    let acct = &self.inner.accounts[idx];
    loop {
      let cur = acct.in_flight.load(Ordering::Acquire);
      if cur >= acct.max_concurrent { return None; }
      match acct.in_flight.compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => return Some(Slot { rt: Arc::clone(self), idx }),
        Err(_) => continue,
      }
    }
  }

  /// Full snapshots for balance::select, with remaining fractions from the DB window.
  pub async fn build_snapshots(&self) -> Vec<AccountSnapshot> {
    self.inner.accounts.iter().enumerate().map(|(i, a)| {
      let pct = self.remaining_pct_of(a);
      AccountSnapshot {
        idx: i,
        remaining_pct: pct,
        in_flight: a.in_flight.load(Ordering::Acquire),
        max_concurrent: a.max_concurrent,
        disabled: a.disabled.load(Ordering::Acquire),
        exhausted: a.exhausted.load(Ordering::Acquire),
      }
    }).collect()
  }

  /// (remaining in unit terms, remaining fraction 0..1) for one account.
  pub fn remaining_of(&self, a: &RuntimeAccount) -> (f64, f64) {
    if a.exhausted.load(Ordering::Acquire) { return (0.0, 0.0); }
    let window_sum = self.inner.store
      .sum_since(&a.id, a.window_start_unix)
      .unwrap_or((0, 0, 0, 0.0, 0));
    let consumed = unit_consumption(&a.unit, &window_sum);
    let rem = match *a.reconciled.lock().unwrap() {
      Some((at_unix, rem0)) if at_unix >= a.window_start_unix => {
        // reconciliation baseline: true remaining at `at_unix`, minus what flowed after
        let post = self.inner.store.sum_since(&a.id, at_unix).unwrap_or((0, 0, 0, 0.0, 0));
        (rem0 - unit_consumption(&a.unit, &post)).max(0.0)
      }
      _ => (a.quota.unwrap_or(0.0) - consumed).max(0.0),
    };
    let pct = match a.quota {
      Some(q) if q > 0.0 => (rem / q).clamp(0.0, 1.0),
      _ => 0.5, // unknown quota: neutral — neither favored nor starved
    };
    (rem, pct)
  }

  pub fn remaining_pct_of(&self, a: &RuntimeAccount) -> f64 {
    self.remaining_of(a).1
  }

  pub fn remaining(&self, id: &str) -> f64 {
    self.account(id).map(|a| self.remaining_of(&a).0).unwrap_or(0.0)
  }

  /// Record one completed request's usage event (credits computed here).
  pub async fn record_event(&self, ev: &UsageEvent) {
    let credits = credits_for(&ev.model, &self.inner.rates, ev.input, ev.cached, ev.output);
    let ev = UsageEvent { credits, ..std::clone::Clone::clone(ev) };
    let _ = self.inner.store.insert_event(ev);
  }

  pub async fn mark_exhausted(&self, id: &str) {
    if let Some(a) = self.account(id) {
      a.exhausted.store(true, Ordering::Release);
      let _ = self.inner.store.set_flags(&a.id, a.disabled.load(Ordering::Acquire), true);
    }
  }

  pub async fn set_disabled(&self, id: &str, disabled: bool) {
    if let Some(a) = self.account(id) {
      a.disabled.store(disabled, Ordering::Release);
      let _ = self.inner.store.set_flags(&a.id, disabled, a.exhausted.load(Ordering::Acquire));
    }
  }

  pub async fn clear_exhausted(&self, id: &str) {
    if let Some(a) = self.account(id) {
      a.exhausted.store(false, Ordering::Release);
      *a.reconciled.lock().unwrap() = None;
      let _ = self.inner.store.set_flags(&a.id, a.disabled.load(Ordering::Acquire), false);
    }
  }

  /// Admin reconciliation: actual remaining (unit terms) observed from console/CLI.
  pub async fn reconcile(&self, id: &str, remaining: f64) {
    let Some(a) = self.account(id) else { return };
    let now = Utc::now().timestamp();
    *a.reconciled.lock().unwrap() = Some((now, remaining));
    a.exhausted.store(false, Ordering::Release);
    let _ = self.inner.store.reconcile(&a.id, DateTime::<Utc>::from_timestamp(now, 0).unwrap(), remaining);
    let _ = self.inner.store.set_flags(&a.id, a.disabled.load(Ordering::Acquire), false);
  }
}

fn default_cycle_start(region: Region) -> String {
  let off = match region {
    Region::Cn => chrono::FixedOffset::east_opt(8 * 3600).unwrap(),
    _ => chrono::FixedOffset::east_opt(0).unwrap(),
  };
  let local = off.from_utc_datetime(&Utc::now());
  chrono::NaiveDate::from((local.year(), local.month(), 1)).to_string()
}
```

CORRECTIONS TO APPLY while implementing (do not copy blindly):
- `record_event` receives the event WITHOUT credits pre-filled (the proxy builds the
  event with `credits: 0.0`); this method recomputes credits via `credits_for` and
  stores the corrected event. Keep the struct clone shallow — UsageEvent is all
  value types.
- `remaining`/`remaining_of` are **synchronous** (store calls are sync —
  rusqlite under a std Mutex); `record_event`/`mark_exhausted`/`reconcile`
  are async.
- `window_start_unix`: clamp to `[1, first_of_month(region)]` so a future-dated or
  garbage `cycle_start` cannot make the window start in the future (which would
  report negative consumption). A configured cycle_start AFTER the 1st of the
  current month (e.g. mid-cycle purchase) is respected as-is; only clamp when it
  would exceed first-of-month of the CURRENT month.
- The `reconciled` baseline is only valid while `at_unix >= window_start_unix`;
  a reconciliation from a previous cycle is ignored (the window sum already
  reflects the new cycle).

**Step 4: Run tests**

Run: `cargo test --test state_test`
Expected: PASS (5 tests).

**Step 5: Commit**

```bash
git add src/state.rs tests/state_test.rs
git commit -m "feat: runtime account state, in-flight slots, DB-window remaining (T6)"
```

---

### Task 7: Upstream forwarding (plain) + header rewrite + quota detection

**Files:**
- Create: `src/forward.rs` (replace placeholder)
- Test: `tests/forward_test.rs`

**Step 1: Write the failing test**

```rust
// tests/forward_test.rs
use std::sync::{Arc, Mutex};
use axum::{body::Body, http::{header, Request, StatusCode}, routing::post, Router};
use serde_json::json;
use tokenbalancer::forward::*;

#[derive(Clone)]
struct MockState { seen_auth: Arc<Mutex<Vec<String>>> }

async fn mock_ok(Request<Body>) -> (StatusCode, &'static str, Body) {
  (StatusCode::OK, "application/json", Body::from(r#"{"ok":true}"#))
}

async fn mock_auth_capture(Saw(auth: String)) -> ... // see helper below

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

#[tokio::test]
async fn forward_plain_roundtrip() {
  let state = MockState { seen_auth: Arc::new(Mutex::new(Vec::new())) };
  let app = Router::new().route("/v1/chat/completions", post(move |req: Request<Body>| {
    let seen = Arc::clone(&state.seen_auth);
    async move {
      let auth = req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
      seen.lock().unwrap().push(auth);
      Ok::<_, http::Error>((
        [(header::CONTENT_TYPE, "application/json")],
        json!({"id":"x","usage":{"prompt_tokens":10,"completion_tokens":5}}).to_string(),
      ))
    }
  }));
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = listener.local_addr().unwrap().port();
  tokio::spawn(axum::serve(listener, app));
  tokio::sleep(std::time::Duration::from_millis(100)).await;

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
  assert_eq!(*state.seen_auth.lock().unwrap(), vec!["Bearer sk-sp-test".to_string()]);
}
```

(Drop the stray `mock_ok`/`mock_auth_capture` sketch lines above — the
`forward_plain_roundtrip` test contains the real mock. Keep exactly the three
tests: `quota_marker_detection`, `header_rewrite_drops_auth_and_injects_bearer`,
`forward_plain_roundtrip`.)

**Step 2: Run test to verify it fails**

Run: `cargo test --test forward_test`
Expected: FAIL (forward not implemented).

**Step 3: Implement src/forward.rs**

```rust
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
  Ok(PlainOutcome { status, content_type, body, quota_exhausted: is_quota_exhausted(status, &body) })
}
```

**Step 4: Run tests**

Run: `cargo test --test forward_test`
Expected: PASS (3 tests).

**Step 5: Commit**

```bash
git add src/forward.rs tests/forward_test.rs
git commit -m "feat: upstream forwarding, header rewrite, quota-429 detection (T7)"
```

---

### Task 8: SSE tap (usage capture while relaying streams)

**Files:**
- Modify: `src/usage.rs` (add `SseTap`, `is_complete`, `take`)
- Modify: `tests/usage_test.rs` (add SseTap tests)

**Step 1: Add failing tests to tests/usage_test.rs**

```rust
use tokenbalancer::forward::Protocol;
use tokenbalancer::usage::{SseTap, UsageTokens};

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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test usage_test`
Expected: FAIL (SseTap not found).

**Step 3: Extend src/usage.rs**

```rust
// --- additions to src/usage.rs (keep all existing items) ---

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
```

And add to `AnthropicStreamParser`:

```rust
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

  pub fn finish(self) -> UsageTokens { self.take() }
```

(Replace the old `finish(self)` body with `self.take()` so the Task 4 tests
keep passing; `finish` consumes self, `take` borrows mut — both stay available.)

NOTE: `usage.rs` now references `crate::forward::Protocol`. To avoid a
forward→usage→forward import cycle, move the `Protocol` enum into `usage.rs`
and `pub use` it from `forward.rs` (i.e. define `Protocol` in usage.rs, and in
forward.rs: `use crate::usage::Protocol;`). Do that instead of the placement
shown in Task 7.

**Step 4: Run tests**

Run: `cargo test --test usage_test`
Expected: PASS (9 tests: 6 from Task 4 + 3 new).

**Step 5: Commit**

```bash
git add src/usage.rs src/forward.rs tests/usage_test.rs
git commit -m "feat: SseTap one-shot usage capture for relayed streams (T8)"
```

---

### Task 9: Proxy routing (auth, balancing, forwarding, accounting)

**Files:**
- Create: `src/proxy.rs` (replace placeholder)
- Modify: `src/state.rs` (add `iter_accounts`, `patch`, `month_start_unix`)
- Test: `tests/proxy_unit_test.rs`

**Step 0: Small additions to src/state.rs**

```rust
// additions to impl Runtime
/// All accounts in stable index order.
pub fn iter_accounts(&self) -> Vec<std::sync::Arc<RuntimeAccount>> {
  self.inner.accounts.iter().cloned().collect()
}

/// Admin field patch (persists to store, updates in-memory account).
pub fn patch(&self, id: &str, max_concurrent: Option<u32>, monthly_quota: Option<f64>, unit: Option<BalanceUnit>) {
  if let Some(a) = self.account(id) {
    if let Some(m) = max_concurrent { a.max_concurrent = m.max(1); }
    if let Some(q) = monthly_quota { a.quota = if q > 0.0 { Some(q) } else { None }; }
    if let Some(u) = unit { a.unit = u; }
    let _ = self.inner.store.patch_account(
      &a.id,
      a.max_concurrent,
      a.quota.unwrap_or(0.0),
      match a.unit { BalanceUnit::Credits => "Credits", BalanceUnit::Tokens => "Tokens" },
    );
  }
}
```

and in `store.rs`:

```rust
pub fn patch_account(&self, id: &str, max_concurrent: u32, monthly_quota: f64, balance_unit: &str) -> anyhow::Result<()> {
  self.conn.lock().unwrap().execute(
    "UPDATE accounts SET max_concurrent=?1, monthly_quota=?2, balance_unit=?3 WHERE id=?4",
    params![max_concurrent as i64, monthly_quota, balance_unit, id])?;
  Ok(())
}
```

plus a shared helper in `config.rs`:

```rust
/// 1st of the current month, unix seconds (UTC+8 zone — the CN Token Plan cycle).
pub fn month_start_unix() -> i64 {
  let off = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
  let local = off.from_utc_datetime(&chrono::Utc::now());
  chrono::NaiveDate::from((local.year(), local.month(), 1))
    .and_hms_opt(0, 0, 0).unwrap()
    .and_utc().timestamp()
}
```

**Step 1: Write the failing unit tests**

```rust
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
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test proxy_unit_test`
Expected: FAIL.

**Step 3: Implement src/proxy.rs**

```rust
// src/proxy.rs
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, Request, Response, StatusCode};
use axum::response::Json;
use axum::routing::{any, get};
use axum::Router;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::balance;
use crate::state::Runtime;
use crate::store::UsageEvent;
use crate::usage::{parse_anthropic_message, parse_openai_usage, SseTap, UsageTokens};

pub struct AppState {
  pub runtime: Arc<Runtime>,
  pub client: reqwest::Client,
  pub queue_timeout: Duration,
}

const MAX_BODY: usize = 10 * 1024 * 1024;

pub fn router(state: AppState) -> Router {
  Router::new()
    .route("/healthz", get(healthz))
    .route("/", get(crate::web::static_index))
    .route("/app.js", get(crate::web::static_app_js))
    .route("/styles.css", get(crate::web::static_styles))
    .route("/api/whoami", get(crate::web::whoami))
    .route("/api/accounts/health", get(crate::web::accounts_health))
    .route("/api/me/usage", get(crate::web::me_usage))
    .route("/api/admin/accounts", get(crate::web::admin_accounts))
    .route("/api/admin/users", get(crate::web::admin_users).post(crate::web::admin_create_user))
    .route("/api/admin/users/{key}/revoke", axum::routing::post(crate::web::admin_revoke_user))
    .route("/api/admin/accounts/{id}", axum::routing::patch(crate::web::admin_patch_account))
    .route("/api/admin/accounts/{id}/reconcile", axum::routing::post(crate::web::admin_reconcile))
    .route("/api/admin/accounts/{id}/clear-exhausted", axum::routing::post(crate::web::admin_clear_exhausted))
    .route("/api/admin/analytics", get(crate::web::admin_analytics))
    .route("/{*rest}", any(proxy_handler))
    .with_state(state)
}

fn json_err(status: StatusCode, code: &str, message: &str, retry_after: Option<u32>) -> Response {
  let body = json!({"error": {"message": message, "type": "invalid_request_error", "code": code}}).to_string();
  let mut builder = Response::builder().status(status).header(header::CONTENT_TYPE, "application/json");
  if let Some(ra) = retry_after {
    builder = builder.header(header::RETRY_AFTER, ra);
  }
  builder.body(Body::from(body)).unwrap()
}

fn unauth(code: &str, message: &str) -> Response {
  json_err(StatusCode::UNAUTHORIZED, code, message, None)
}

fn bearer(headers: &HeaderMap) -> Option<String> {
  headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
    .and_then(|v| v.strip_prefix("Bearer ")).map(|s| s.trim().to_string())
}

fn authenticate(st: &AppState, headers: &HeaderMap) -> Result<String, Response> {
  let key = bearer(headers).ok_or_else(|| unauth("authentication_error", "Missing Authorization: Bearer <key>"))?;
  if key == st.runtime.admin_key() {
    return Ok("admin".to_string());
  }
  match st.runtime.store().find_live_user_by_key(&key) {
    Ok(Some(u)) => Ok(u.id),
    _ => Err(unauth("invalid_api_key", "Unknown or revoked proxy key")),
  }
}

/// Extract model; inject stream_options.include_usage for OpenAI streams; report stream flag.
pub fn prepare_body(proto: crate::usage::Protocol, body: &Bytes) -> (String, Bytes, bool) {
  if body.is_empty() {
    return (String::from("unknown"), body.clone(), false);
  }
  match serde_json::from_slice::<Value>(body) {
    Ok(mut v) => {
      let model = v.get("model").and_then(|m| m.as_str()).unwrap_or("unknown").to_string();
      let is_stream = v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
      if proto == crate::usage::Protocol::OpenAi && is_stream {
        match v.get_mut("stream_options") {
          Some(Value::Object(o)) => { o.insert("include_usage".into(), Value::Bool(true)); }
          _ => { v["stream_options"] = json!({"include_usage": true}); }
        }
        if let Ok(b) = serde_json::to_vec(&v) {
          return (model, b, true);
        }
      }
      (model, body.clone(), is_stream)
    }
    Err(_) => (String::from("unknown"), body.clone(), false),
  }
}

fn record_usage_for(proto: crate::usage::Protocol, path: &str) -> bool {
  match proto {
    crate::usage::Protocol::OpenAi => path.ends_with("/chat/completions") || path.ends_with("/responses"),
    crate::usage::Protocol::Anthropic => path.ends_with("/messages"),
  }
}

async fn healthz(State(st): State<AppState>) -> Json<Value> {
  let mut accounts = Vec::new();
  for a in st.runtime.iter_accounts() {
    let (rem, pct) = st.runtime.remaining_of(&a);
    accounts.push(json!({
      "id": a.id, "label": a.label, "unit": a.unit, "quota": a.quota,
      "remaining": rem, "remaining_pct": pct,
      "in_flight": a.in_flight.load(Ordering::Acquire),
      "max_concurrent": a.max_concurrent,
      "disabled": a.disabled.load(Ordering::Acquire),
      "exhausted": a.exhausted.load(Ordering::Acquire),
    }));
  }
  Json(json!({"status": "ok", "accounts": accounts}))
}

async fn proxy_handler(State(st): State<AppState>, req: Request) -> Result<Response, Response> {
  let (parts, body) = req.into_parts();
  let path = parts.uri.path().trim_end_matches('/').to_string();

  let proto = if path.starts_with("/apps/anthropic") {
    crate::usage::Protocol::Anthropic
  } else if path.starts_with("/v1") {
    crate::usage::Protocol::OpenAi
  } else {
    return Err(json_err(StatusCode::NOT_FOUND, "not_found",
      "Unknown route. Use /v1/* (OpenAI-compatible) or /apps/anthropic/* (Anthropic-compatible).", None));
  };

  let user_id = authenticate(&st, &parts.headers)?;
  let body_bytes = axum::body::to_bytes(body, MAX_BODY).await
    .map_err(|_| json_err(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large", "Request body exceeds 10 MB.", None))?;

  let (model, fwd_body, is_stream) = prepare_body(proto, &body_bytes);

  // ---- balance + queue ----
  let deadline = Instant::now() + st.queue_timeout;
  let (slot, acct) = loop {
    let snaps = st.runtime.build_snapshots().await;
    match balance::select(&snaps, &mut rand::thread_rng()) {
      balance::Selection::Chosen(i) => {
        if let Some(a) = st.runtime.account_by_idx(i) {
          if let Some(s) = st.runtime.try_acquire(&a.id) {
            break (s, a);
          }
        }
        // lost the slot to a race — retry immediately
      }
      balance::Selection::NoneAvailable => {
        return Err(json_err(StatusCode::SERVICE_UNAVAILABLE, "no_available_accounts",
          "All upstream accounts are disabled or quota-exhausted. Ask the admin to reconcile or enable an account.", None));
      }
      balance::Selection::AllSlotsFull => {
        if Instant::now() >= deadline {
          return Err(json_err(StatusCode::SERVICE_UNAVAILABLE, "all_accounts_busy",
            "All upstream accounts are at their concurrency cap. Try again shortly.",
            Some(st.queue_timeout.as_secs().min(u32::MAX as u64) as u32)));
        }
        tokio::sleep(Duration::from_millis(200)).await;
      }
    }
  };

  let started = Instant::now();
  let (base, rest) = match proto {
    crate::usage::Protocol::OpenAi => (&acct.base_url_openai, path.strip_prefix("/v1").unwrap_or(&path)),
    crate::usage::Protocol::Anthropic => (&acct.base_url_anthropic, path.strip_prefix("/apps/anthropic").unwrap_or(&path)),
  };
  let mut url = if rest.starts_with('/') { format!("{base}{rest}") } else { format!("{base}/{rest}") };
  if let Some(q) = parts.uri.query() {
    url.push('?');
    url.push_str(q);
  }

  let headers = crate::forward::build_upstream_headers(&parts.headers, &acct.api_key);
  let upstream = crate::forward::open_stream(&st.client, &parts.method, &url, &headers, Some(fwd_body)).await
    .map_err(|e| json_err(StatusCode::BAD_GATEWAY, "upstream_error", &format!("Upstream unreachable: {e}"), None))?;
  let status = upstream.status().as_u16();

  if status >= 400 || !is_stream {
    // ---- plain pass-through (incl. error bodies on streaming requests) ----
    let plain_body = upstream.bytes().await
      .map_err(|e| json_err(StatusCode::BAD_GATEWAY, "upstream_error", &format!("Upstream read failed: {e}"), None))?;
    if crate::forward::is_quota_exhausted(status, &plain_body) {
      st.runtime.mark_exhausted(&acct.id).await;
    }
    if status < 400 && record_usage_for(proto, &path) {
      let u = match proto {
        crate::usage::Protocol::OpenAi => parse_openai_usage(std::str::from_utf8(&plain_body).unwrap_or("")).unwrap_or(UsageTokens::missing()),
        crate::usage::Protocol::Anthropic => parse_anthropic_message(std::str::from_utf8(&plain_body).unwrap_or("")).unwrap_or(UsageTokens::missing()),
      };
      let ev = UsageEvent {
        ts: chrono::Utc::now(), user_id: user_id.clone(), account_id: acct.id.clone(),
        model: model.clone(), input: u.input, cached: u.cached, output: u.output,
        credits: 0.0, latency_ms: started.elapsed().as_millis() as u64,
        status, stream: false, parse_error: u.parse_error,
      };
      st.runtime.record_event(&ev).await;
    }
    drop(slot);
    let content_type = upstream.headers().get(header::CONTENT_TYPE)
      .and_then(|v| v.to_str().ok()).unwrap_or("application/octet-stream").to_string();
    return Ok(Response::builder()
      .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
      .header(header::CONTENT_TYPE, content_type)
      .body(Body::from(plain_body))
      .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "response_build", "Failed to build response.", None))?);
  }

  // ---- stream relay ----
  let tap_stream = TapStream {
    inner: upstream.bytes_stream(),
    tap: SseTap::new(proto),
    usage: None,
    finalized: false,
    slot: Some(slot),
    runtime: st.runtime.clone(),
    user_id,
    acct_id: acct.id.clone(),
    model,
    status,
    started,
  };
  let content_type = upstream.headers().get(header::CONTENT_TYPE)
    .and_then(|v| v.to_str().ok()).unwrap_or("text/event-stream").to_string();
  Ok(Response::builder()
    .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    .header(header::CONTENT_TYPE, content_type)
    .body(Body::from_stream(tap_stream))
    .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "response_build", "Failed to build response.", None))?)
}

/// Relays the upstream byte stream and captures usage exactly once.
struct TapStream {
  inner: reqwest::ByteStream,
  tap: SseTap,
  usage: Option<UsageTokens>,
  finalized: bool,
  slot: Option<crate::state::Slot>, // released when the stream ends (or is dropped)
  runtime: Arc<Runtime>,
  user_id: String,
  acct_id: String,
  model: String,
  status: u16,
  started: Instant,
}

impl Stream for TapStream {
  type Item = Result<Bytes, reqwest::Error>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    match self.inner.poll_next(cx) {
      Poll::Ready(Some(Ok(chunk))) => {
        if self.usage.is_none() {
          self.usage = self.tap.feed(&chunk);
        }
        Poll::Ready(Some(Ok(chunk)))
      }
      Poll::Ready(x @ Some(Err(_))) => { self.finalize(); Poll::Ready(x) }
      Poll::Ready(None) => { self.finalize(); Poll::Ready(None) }
      Poll::Pending => Poll::Pending,
    }
  }
}

impl Drop for TapStream {
  fn drop(&mut self) { self.finalize(); }
}

impl TapStream {
  fn finalize(&mut self) {
    if self.finalized { return; }
    self.finalized = true;
    let usage = self.usage.take().unwrap_or(UsageTokens::missing());
    let runtime = self.runtime.clone();
    let user_id = self.user_id.clone();
    let acct_id = self.acct_id.clone();
    let model = self.model.clone();
    let status = self.status;
    let latency_ms = self.started.elapsed().as_millis() as u64;
    let _slot = self.slot.take(); // releases the in-flight slot
    tokio::spawn(async move {
      let ev = UsageEvent {
        ts: chrono::Utc::now(), user_id, account_id: acct_id, model,
        input: usage.input, cached: usage.cached, output: usage.output,
        credits: 0.0, latency_ms, status, stream: true, parse_error: usage.parse_error,
      };
      runtime.record_event(&ev).await;
      drop(_slot);
    });
  }
}
```

Also add `parse_anthropic_message` to `src/usage.rs` (non-stream Anthropic message JSON):

```rust
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
```

NOTE: `web.rs` does not exist yet — until Task 10 the `crate::web::*` route
registrations above must compile. If you implement Task 9 before Task 10, create
`src/web.rs` stubs first:

```rust
// src/web.rs (stub, replaced in Task 10)
use axum::response::IntoResponse;
pub async fn static_index() -> impl IntoResponse { "TokenBalancer" }
pub async fn static_app_js() -> impl IntoResponse { "" }
pub async fn static_styles() -> impl IntoResponse { "" }
pub async fn whoami() -> &'static str { "{}" }
pub async fn accounts_health() -> &'static str { "{}" }
pub async fn me_usage() -> &'static str { "{}" }
pub async fn admin_accounts() -> &'static str { "{}" }
pub async fn admin_users() -> &'static str { "{}" }
pub async fn admin_create_user() -> &'static str { "{}" }
pub async fn admin_revoke_user() -> &'static str { "{}" }
pub async fn admin_patch_account() -> &'static str { "{}" }
pub async fn admin_reconcile() -> &'static str { "{}" }
pub async fn admin_clear_exhausted() -> &'static str { "{}" }
pub async fn admin_analytics() -> &'static str { "{}" }
```

(Stubs returning plain &str compile fine with axum; real handlers land in Task 10.
If a stub signature mismatches a route method (get/post/patch), axum will
complain — the route list in this task is the source of truth for signatures.)

**Step 4: Run tests**

Run: `cargo test --test proxy_unit_test`
Expected: PASS (4 tests).

**Step 5: Commit**

```bash
git add src/proxy.rs src/state.rs src/store.rs src/config.rs src/usage.rs src/web.rs tests/proxy_unit_test.rs
git commit -m "feat: proxy routing, auth, balancing with queue, stream relay (T9)"
```

---

### Task 10: Web JSON API (auth, admin, user endpoints)

**Files:**
- Modify: `src/web.rs` (replace stubs)
- Modify: `src/store.rs` (add scoped aggregate queries)
- Create: `web/index.html`, `web/app.js`, `web/styles.css` (stubs for now — real UI in Task 11)
- Test: `tests/web_api_test.rs`

**Step 1: Add store queries (src/store.rs)**

```rust
  /// (key, input, cached, output, credits, count) grouped by a whitelisted column, since unix ts.
  pub fn group_since(&self, col: &str, since_unix: i64) -> anyhow::Result<Vec<(String, u64, u64, u64, f64, u64)>> {
    let col = match col { "user_id" | "account_id" | "model" => col, _ => return Err(anyhow::anyhow!("bad column")) };
    let since = DateTime::<Utc>::from_timestamp(since_unix, 0).unwrap_or_default().to_rfc3339();
    let c = self.conn.lock().unwrap();
    let sql = format!("SELECT {col}, COALESCE(SUM(input),0), COALESCE(SUM(cached),0), COALESCE(SUM(output),0), COALESCE(SUM(credits),0.0), COUNT(*) FROM usage_events WHERE ts>=?1 GROUP BY {col} ORDER BY 6 DESC");
    let mut st = c.prepare(&sql)?;
    let rows = st.query_map(params![since], |r| {
      Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? as u64,
         r.get::<_, i64>(3)? as u64, r.get::<_, f64>(4)?, r.get::<_, i64>(5)? as u64))
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
  }

  /// Totals for one user since unix ts: (input, cached, output, credits, count).
  pub fn totals_for(&self, user_id: &str, since_unix: i64) -> anyhow::Result<(u64, u64, u64, f64, u64)> {
    let since = DateTime::<Utc>::from_timestamp(since_unix, 0).unwrap_or_default().to_rfc3339();
    let c = self.conn.lock().unwrap();
    let (i, ca, o, cr, n) = c.query_row(
      "SELECT COALESCE(SUM(input),0), COALESCE(SUM(cached),0), COALESCE(SUM(output),0), COALESCE(SUM(credits),0.0), COUNT(*) FROM usage_events WHERE user_id=?1 AND ts>=?2",
      params![user_id, since],
      |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? as u64, r.get::<_, f64>(3)?, r.get::<_, i64>(4)? as u64)))?;
    Ok((i, ca, o, cr, n))
  }

  /// Daily rows for one user: (date, events, input, output, credits).
  pub fn daily_for(&self, user_id: &str, from_unix: i64, to_unix: i64) -> anyhow::Result<Vec<(String, u64, u64, u64, f64)>> {
    let f = DateTime::<Utc>::from_timestamp(from_unix, 0).unwrap_or_default().to_rfc3339();
    let t = DateTime::<Utc>::from_timestamp(to_unix, 0).unwrap_or_default().to_rfc3339();
    let c = self.conn.lock().unwrap();
    let mut st = c.prepare("SELECT substr(ts,1,10) d, COUNT(*), COALESCE(SUM(input),0), COALESCE(SUM(output),0), COALESCE(SUM(credits),0.0) FROM usage_events WHERE user_id=?1 AND ts>=?2 AND ts<?3 GROUP BY d ORDER BY d")?;
    let rows = st.query_map(params![user_id, f, t], |r| {
      Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? as u64, r.get::<_, i64>(3)? as u64, r.get::<_, f64>(4)?))
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
  }

  /// Top models for one user: (model, events, input+output, credits).
  pub fn top_models_for(&self, user_id: &str, since_unix: i64, limit: u32) -> anyhow::Result<Vec<(String, u64, u64, f64)>> {
    let since = DateTime::<Utc>::from_timestamp(since_unix, 0).unwrap_or_default().to_rfc3339();
    let c = self.conn.lock().unwrap();
    let mut st = c.prepare("SELECT model, COUNT(*), COALESCE(SUM(input + output),0), COALESCE(SUM(credits),0.0) FROM usage_events WHERE user_id=?1 AND ts>=?2 GROUP BY model ORDER BY 3 DESC LIMIT ?3")?;
    let rows = st.query_map(params![user_id, since, limit as i64], |r| {
      Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64, r.get::<_, i64>(2)? as u64, r.get::<_, f64>(3)?))
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
  }
```

**Step 2: Write the failing test**

```rust
// tests/web_api_test.rs
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use tower::ServiceExt; // oneshot

fn app() -> axum::Router {
  let cfg = tokenbalancer::config::Config {
    server: tokenbalancer::config::ServerConf { listen: "127.0.0.1:0".into(), admin_key: "tba_admin".into(), db_path: ":memory:".into(), queue_timeout_secs: 1 },
    defaults: tokenbalancer::config::DefaultsConf { region: tokenbalancer::config::Region::Cn, balance_unit: tokenbalancer::config::BalanceUnit::Credits, max_concurrent: 2 },
    accounts: vec![tokenbalancer::config::AccountConf { id: "a1".into(), label: None, api_key: "sk-sp-a".into(), region: None, base_url_openai: None, base_url_anthropic: None, seat_tier: Some(tokenbalancer::config::SeatTier::Pro), balance_unit: None, monthly_quota: None, cycle_start: None, max_concurrent: None, disabled: None }],
    credit_rates: Default::default(),
    users: vec![tokenbalancer::config::UserConf { key: "tbu_alice".into(), name: "alice".into() }],
  };
  // build the Router synchronously (Runtime::load is async — use block_on in test)
  tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    .block_on(async move {
      let store = tokenbalancer::store::Store::open(":memory:").unwrap();
      let runtime = tokenbalancer::state::Runtime::load(&cfg, std::sync::Arc::new(store)).await.unwrap();
      let client = reqwest::Client::new();
      let state = tokenbalancer::proxy::AppState { runtime, client, queue_timeout: std::time::Duration::from_secs(5) };
      tokenbalancer::proxy::router(state)
    })
}

fn get(path: &str, key: Option<&str>) -> Request<Body> {
  let mut b = Request::builder().uri(path);
  if let Some(k) = key { b = b.header(header::AUTHORIZATION, format!("Bearer {k}")); }
  b.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn whoami_roles() {
  let r = app().oneshot(get("/api/whoami", Some("tba_admin"))).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
  let r = app().oneshot(get("/api/whoami", Some("tbu_alice"))).await.unwrap();
  let body = axum::body::to_bytes(r.into_body(), 10_000).await.unwrap();
  let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
  assert_eq!(v["role"], "user");
  assert_eq!(v["user"]["name"], "alice");
  let r = app().oneshot(get("/api/whoami", Some("tbu_bad"))).await.unwrap();
  assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_guard() {
  // user key must NOT reach admin endpoints
  let r = app().oneshot(get("/api/admin/accounts", Some("tbu_alice"))).await.unwrap();
  assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
  // admin key works
  let r = app().oneshot(get("/api/admin/accounts", Some("tba_admin"))).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
}

#[tokio::test]
async fn create_and_revoke_user() {
  // one app instance for the whole flow (Router is Clone; every app() call
  // would otherwise be a fresh in-memory DB)
  let app = app();
  let r = app.clone().oneshot(Request::builder().uri("/api/admin/users")
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
  let r = app.clone().oneshot(Request::builder().uri(format!("/api/admin/users/{key}/revoke"))
    .header(header::AUTHORIZATION, "Bearer tba_admin")
    .body(Body::empty()).unwrap()).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
  // revoked key no longer authenticates
  let r = app.clone().oneshot(get("/api/whoami", Some(&key))).await.unwrap();
  assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn reconcile_reflects_in_accounts() {
  let r = app().oneshot(Request::builder().uri("/api/admin/accounts/a1/reconcile")
    .header(header::AUTHORIZATION, "Bearer tba_admin")
    .header(header::CONTENT_TYPE, "application/json")
    .body(Body::from(r#"{"remaining": 42000}"#)).unwrap()).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
}

#[tokio::test]
async fn static_ui_served() {
  let r = app().oneshot(get("/", None)).await.unwrap();
  assert_eq!(r.status(), StatusCode::OK);
  assert!(r.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap().contains("text/html"));
}
```

NOTE: `create_and_revoke_user` reuses one app instance (axum Router is Clone);
every other `app()` call builds a fresh in-memory DB.

**Step 3: Run test to verify it fails**

Run: `cargo test --test web_api_test`
Expected: FAIL (web stubs return "{}").

**Step 4: Implement src/web.rs**

```rust
// src/web.rs
use std::sync::atomic::Ordering;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::proxy::AppState;

const INDEX_HTML: &str = include_bytes!("../web/index.html");
const APP_JS: &str = include_bytes!("../web/app.js");
const STYLES: &str = include_bytes!("../web/styles.css");

pub async fn static_index() -> impl IntoResponse {
  ([("content-type", "text/html; charset=utf-8")], INDEX_HTML)
}
pub async fn static_app_js() -> impl IntoResponse {
  ([("content-type", "text/javascript; charset=utf-8")], APP_JS)
}
pub async fn static_styles() -> impl IntoResponse {
  ([("content-type", "text/css; charset=utf-8")], STYLES)
}

fn bearer(headers: &HeaderMap) -> Option<String> {
  headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
    .and_then(|v| v.strip_prefix("Bearer ")).map(|s| s.trim().to_string())
}

fn unauthorized() -> Response {
  let body = json!({"error": {"message": "Unauthorized", "code": "unauthorized"}}).to_string();
  Response::builder().status(StatusCode::UNAUTHORIZED)
    .header(header::CONTENT_TYPE, "application/json").body(axum::body::Body::from(body)).unwrap()
}

fn require_admin(st: &AppState, headers: &HeaderMap) -> Result<(), Response> {
  match bearer(headers) {
    Some(k) if k == st.runtime.admin_key() => Ok(()),
    _ => Err(unauthorized()),
  }
}

fn require_user(st: &AppState, headers: &HeaderMap) -> Result<(String, String), Response> {
  let k = bearer(headers).ok_or_else(unauthorized)?;
  if k == st.runtime.admin_key() { return Ok(("admin".into(), "admin".into())); }
  st.runtime.store().find_live_user_by_key(&k)
    .map_err(|_| unauthorized())?
    .map(|u| (u.id, u.name))
    .ok_or_else(unauthorized)
}

pub fn gen_user_key() -> String {
  const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let mut rng = rand::thread_rng();
  let s: String = (0..16).map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char).collect();
  format!("tbu_{s}")
}

pub async fn whoami(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  match bearer(&headers) {
    Some(k) if k == st.runtime.admin_key() => Ok(Json(json!({"role": "admin"}))),
    Some(k) => st.runtime.store().find_live_user_by_key(&k).map_err(|_| unauthorized())?
      .map(|u| Json(json!({"role": "user", "user": {"id": u.id, "name": u.name}})))
      .ok_or_else(unauthorized),
    None => Err(unauthorized()),
  }
}

pub async fn accounts_health(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_user(&st, &headers)?;
  let mut accounts = Vec::new();
  for a in st.runtime.iter_accounts() {
    let (_, pct) = st.runtime.remaining_of(&a);
    accounts.push(json!({
      "id": a.id, "label": a.label, "remaining_pct": pct,
      "in_flight": a.in_flight.load(Ordering::Acquire), "max_concurrent": a.max_concurrent,
      "disabled": a.disabled.load(Ordering::Acquire), "exhausted": a.exhausted.load(Ordering::Acquire),
    }));
  }
  Ok(Json(json!({"accounts": accounts})))
}

pub async fn me_usage(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  let (uid, _name) = require_user(&st, &headers)?;
  let now = Utc::now().timestamp();
  let since = now - 14 * 86_400;
  let (i, ca, o, cr, n) = st.runtime.store().totals_for(&uid, since).unwrap_or((0, 0, 0, 0.0, 0));
  let daily: Vec<Value> = st.runtime.store().daily_for(&uid, since, now + 86_400).unwrap_or_default()
    .into_iter().map(|(d, ev, di, do_, c)| json!({"date": d, "events": ev, "input": di, "output": do_, "tokens": di + do_, "credits": c})).collect();
  let models: Vec<Value> = st.runtime.store().top_models_for(&uid, since, 5).unwrap_or_default()
    .into_iter().map(|(m, ev, tok, c)| json!({"model": m, "events": ev, "tokens": tok, "credits": c})).collect();
  Ok(Json(json!({
    "totals": {"events": n, "input": i, "cached": ca, "output": o, "tokens": i + o, "credits": cr},
    "daily": daily, "top_models": models, "window_days": 14
  })))
}

pub async fn admin_accounts(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  let rows = st.runtime.store().list_accounts().unwrap_or_default();
  let mut accounts = Vec::new();
  for a in st.runtime.iter_accounts() {
    let row = rows.iter().find(|r| r.id == a.id);
    let (rem, pct) = st.runtime.remaining_of(&a);
    accounts.push(json!({
      "id": a.id, "label": a.label,
      "region": row.as_ref().map(|r| r.region.clone()).unwrap_or_default(),
      "unit": a.unit, "quota": a.quota,
      "remaining": rem, "remaining_pct": pct,
      "in_flight": a.in_flight.load(Ordering::Acquire), "max_concurrent": a.max_concurrent,
      "disabled": a.disabled.load(Ordering::Acquire), "exhausted": a.exhausted.load(Ordering::Acquire),
      "reconciled_at": row.and_then(|r| r.reconciled_at.clone()),
    }));
  }
  Ok(Json(json!({"accounts": accounts})))
}

pub async fn admin_users(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  let users = st.runtime.store().list_users().unwrap_or_default();
  let since = crate::config::month_start_unix();
  let by_user = st.runtime.store().group_since("user_id", since).unwrap_or_default();
  let mut out = Vec::new();
  for u in users {
    let (i, _ca, o, cr, n) = by_user.iter()
      .find(|(k, ..)| k == &u.id)
      .map(|(_, a, b, c, d, e)| (*a, *b, *c, *d, *e))
      .unwrap_or((0, 0, 0, 0.0, 0));
    out.push(json!({
      "id": u.id, "name": u.name, "key": u.key, "created_at": u.created_at, "revoked": u.revoked,
      "month_events": n, "month_tokens": i + o, "month_credits": cr,
    }));
  }
  Ok(Json(json!({"users": out, "window": "month-to-date"})))
}
```



**Step 5: Implement the remaining admin handlers (same file)**

```rust
#[derive(Deserialize)]
struct CreateUser { name: String }

pub async fn admin_create_user(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<CreateUser>) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  let key = gen_user_key();
  st.runtime.store().create_user(&key, &body.name).map_err(|e| {
    let mut r = unauthorized(); let _ = e; r
  })?;
  Ok(Json(json!({"key": key, "name": body.name})))
}

#[derive(Deserialize)]
struct PatchAccount {
  max_concurrent: Option<u32>,
  monthly_quota: Option<f64>,
  balance_unit: Option<String>,
  disabled: Option<bool>,
}

pub async fn admin_patch_account(Path(id): Path<String>, State(st): State<AppState>, headers: HeaderMap, Json(body): Json<PatchAccount>) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  if st.runtime.account(&id).is_none() {
    let body = json!({"error": {"message": "Unknown account", "code": "not_found"}}).to_string();
    return Err(Response::builder().status(StatusCode::NOT_FOUND)
      .header(header::CONTENT_TYPE, "application/json").body(axum::body::Body::from(body)).unwrap());
  }
  if let Some(d) = body.disabled { st.runtime.set_disabled(&id, d).await; }
  let unit = match body.balance_unit.as_deref() {
    Some("tokens") => Some(crate::config::BalanceUnit::Tokens),
    Some("credits") => Some(crate::config::BalanceUnit::Credits),
    _ => None,
  };
  st.runtime.patch(&id, body.max_concurrent, body.monthly_quota, unit);
  Ok(Json(json!({"ok": true, "id": id})))
}

#[derive(Deserialize)]
struct Reconcile { remaining: f64 }

pub async fn admin_reconcile(Path(id): Path<String>, State(st): State<AppState>, headers: HeaderMap, Json(body): Json<Reconcile>) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  if st.runtime.account(&id).is_none() {
    return Err(Response::builder().status(StatusCode::NOT_FOUND)
      .header(header::CONTENT_TYPE, "application/json")
      .body(axum::body::Body::from(json!({"error": {"message": "Unknown account", "code": "not_found"}}).to_string())).unwrap());
  }
  st.runtime.reconcile(&id, body.remaining).await;
  Ok(Json(json!({"ok": true, "id": id, "remaining": body.remaining})))
}

pub async fn admin_clear_exhausted(Path(id): Path<String>, State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  st.runtime.clear_exhausted(&id).await;
  Ok(Json(json!({"ok": true, "id": id})))
}

pub async fn admin_revoke_user(Path(key): Path<String>, State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  st.runtime.store().revoke_user(&key).map_err(|_| unauthorized())?;
  Ok(Json(json!({"ok": true, "revoked": key})))
}

#[derive(Deserialize)]
struct AnalyticsQuery { from: Option<String>, to: Option<String> } // "YYYY-MM-DD"

pub async fn admin_analytics(Query(q): Query<AnalyticsQuery>, State(st): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, Response> {
  require_admin(&st, &headers)?;
  let now = Utc::now().timestamp();
  let to = q.to.as_deref().and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
    .map(|d| d.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp())
    .unwrap_or(now);
  let from = q.from.as_deref().and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
    .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
    .unwrap_or(now - 29 * 86_400);
  let daily: Vec<Value> = st.runtime.store().daily_stats(from, to + 1).unwrap_or_default()
    .into_iter().map(|(d, ev, i, c, o)| json!({"date": d, "events": ev, "input": i, "cached": c, "output": o, "tokens": i + o})).collect();
  let since = from;
  let to_v = |rows: Vec<(String, u64, u64, u64, f64, u64)>| -> Vec<Value> {
    rows.into_iter().map(|(k, i, _ca, o, cr, n)| json!({"key": k, "input": i, "output": o, "tokens": i + o, "credits": cr, "events": n})).collect()
  };
  Ok(Json(json!({
    "from": from, "to": to,
    "daily": daily,
    "by_user": to_v(st.runtime.store().group_since("user_id", since).unwrap_or_default()),
    "by_account": to_v(st.runtime.store().group_since("account_id", since).unwrap_or_default()),
    "by_model": to_v(st.runtime.store().group_since("model", since).unwrap_or_default()),
  })))
}
```

Note: `Runtime::load` (Task 6) already seeds `cfg.users` into the store
(idempotent — skips existing keys), so both these tests and main.rs rely on it.

**Step 6: Create stub static files (real UI in Task 11)**

```html
<!-- web/index.html (stub) -->
<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>TokenBalancer</title>
<link rel="stylesheet" href="/styles.css"></head><body><h1>TokenBalancer</h1>
<script src="/app.js"></script></body></html>
```

```js
// web/app.js (stub)
console.log("tokenbalancer ui stub");
```

```css
/* web/styles.css (stub) */
body { font-family: system-ui, sans-serif; }
```

**Step 7: Run tests**

Run: `cargo test --test web_api_test`
Expected: PASS (5 tests).

**Step 8: Commit**

```bash
git add src/web.rs src/store.rs src/state.rs web/ tests/web_api_test.rs
git commit -m "feat: web JSON API (whoami, usage, admin accounts/users/analytics) (T10)"
```

---

### Task 11: Web UI (vanilla JS/CSS, embedded)

**Files:**
- Create (replace stubs): `web/index.html`, `web/app.js`, `web/styles.css`
- Test: extend `tests/web_api_test.rs` (content checks)

The UI is a single page. Paste a key (proxy key or admin key) →
`GET /api/whoami` decides the role → render admin or user view. Auto-refresh
every 30 s. No framework, no build step.

**Step 1: Write web/index.html**

```html
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TokenBalancer · Qwen Token Plan 团队版平衡代理</title>
<link rel="stylesheet" href="/styles.css">
</head>
<body>
<header>
  <div class="brand">⚖️ TokenBalancer</div>
  <div class="login">
    <input id="key" type="password" placeholder="粘贴代理 Key 或 admin Key" autocomplete="off">
    <button id="connect">连接</button>
    <span id="who" class="muted"></span>
  </div>
</header>
<main id="view"><div class="muted">请先在上方粘贴 Key 并连接。</div></main>
<footer class="muted">数据每 30 秒自动刷新 · 剩余量基于本地用量记账，可用“对账”校准</footer>
<script src="/app.js"></script>
</body>
</html>
```

**Step 2: Write web/app.js**

```js
"use strict";
const $ = (s) => document.querySelector(s);
const state = { key: "", role: null, user: null, timer: null };

async function api(path, opts = {}) {
  const r = await fetch(path, {
    method: opts.method || "GET",
    headers: Object.assign(
      { "Authorization": "Bearer " + state.key },
      opts.body ? { "Content-Type": "application/json" } : {}
    ),
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (r.status === 401) { logout(); throw new Error("Key 无效或已吊销"); }
  if (!r.ok) { const t = await r.text(); throw new Error(t || r.status); }
  return r.json();
}

function logout() {
  state.role = null; state.user = null;
  clearInterval(state.timer); state.timer = null;
  $("#who").textContent = "";
}

async function connect() {
  state.key = $("#key").value.trim();
  if (!state.key) return;
  try {
    const w = await api("/api/whoami");
    state.role = w.role; state.user = w.user || null;
    $("#who").textContent = state.role === "admin" ? "管理员" : "用户: " + (state.user ? state.user.name : state.user.id);
    refresh();
    clearInterval(state.timer);
    state.timer = setInterval(refresh, 30000);
  } catch (e) { $("#who").textContent = "连接失败: " + e.message; }
}

function refresh() { if (state.role === "admin") renderAdmin(); else renderUser(); }

// ---------- helpers ----------
function fmtNum(n) {
  if (n >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (n >= 1e4) return (n / 1e3).toFixed(1) + "k";
  return String(Math.round(n));
}
function bar(pct, cls) {
  const p = Math.max(0, Math.min(100, (pct || 0) * 100));
  return '<div class="bar ' + (cls || "") + '"><div style="width:' + p + '%"></div></div>';
}
function dailyBars(daily, key) {
  const days = [];
  const now = new Date();
  for (let i = 13; i >= 0; i--) {
    const d = new Date(now); d.setDate(d.getDate() - i);
    days.push(d.toISOString().slice(0, 10));
  }
  const map = {}; (daily || []).forEach(d => { map[d.date] = d; });
  const max = Math.max(1, ...days.map(dt => (map[dt] || {})[key] || 0));
  return days.map(dt => {
    const v = (map[dt] || {})[key] || 0;
    const h = Math.round((v / max) * 100);
    return '<div class="day" title="' + dt + ": " + fmtNum(v) + '"><div class="daybar" style="height:' + Math.max(2, h) + '%"></div><span>' + dt.slice(5) + "</span></div>";
  }).join("");
}
function acctCard(a, withActions) {
  const unitLabel = a.unit === "Credits" ? "credits" : "tokens";
  const quotaText = a.quota ? " · 剩余 " + fmtNum(a.remaining) + " / " + fmtNum(a.quota) : " · 剩余 " + fmtNum(a.remaining);
  return (
    '<div class="acct ' + (a.disabled ? "disabled" : "") + ' ' + (a.exhausted ? "exhausted" : "") + '">' +
    '<div class="acct-head"><span>' + (a.label || a.id) + ' <span class="muted">(' + (a.region || "cn") + " · " + unitLabel + ")</span></span>" +
    '<span class="muted">' + a.in_flight + "/" + a.max_concurrent + " · " + Math.round(a.remaining_pct * 100) + "%</span></div>" +
    bar(a.remaining_pct, a.exhausted ? "bad" : (a.disabled ? "off" : "ok")) +
    '<div class="tags">' +
      (a.exhausted ? '<span class="tag bad">额度耗尽</span>' : "") +
      (a.disabled ? '<span class="tag off">已禁用</span>' : "") +
      (a.reconciled_at ? '<span class="tag ok">已对账 ' + String(a.reconciled_at).slice(0, 10) + "</span>" : "") +
    "</div>" +
    (withActions ? (
      '<div class="actions">' +
      '<button data-act="toggle" data-id="' + a.id + '">' + (a.disabled ? "启用" : "禁用") + "</button>" +
      '<button data-act="reconcile" data-id="' + a.id + '">对账…</button>' +
      '<button data-act="clear" data-id="' + a.id + '">清除耗尽</button>' +
      '<button data-act="patch" data-id="' + a.id + '">并发/额度…</button>' +
      "</div>"
    ) : "") +
    "</div>"
  );
}

// ---------- user view ----------
async function renderUser() {
  let usage, health;
  try {
    [usage, health] = await Promise.all([api("/api/me/usage"), api("/api/accounts/health")]);
  } catch (e) { $("#view").innerHTML = '<div class="muted">' + e.message + "</div>"; return; }
  const t = usage.totals;
  const cards =
    '<div class="cards">' +
    '<div class="card"><div class="card-label">近14天 请求数</div><div class="card-value">' + t.events + "</div></div>" +
    '<div class="card"><div class="card-label">近14天 Tokens</div><div class="card-value">' + fmtNum(t.tokens) + "</div></div>" +
    '<div class="card"><div class="card-label">近14天 Credits(估算)</div><div class="card-value">' + fmtNum(t.credits) + "</div></div>" +
    "</div>";
  const top = (usage.top_models || []).map(m =>
    "<tr><td>" + m.model + "</td><td>" + m.events + "</td><td>" + fmtNum(m.tokens) + "</td><td>" + fmtNum(m.credits) + "</td></tr>").join("");
  const accounts = health.accounts.map(a => acctCard(a, false)).join("");
  $("#view").innerHTML =
    cards +
    "<h2>我的用量（近14天）</h2>" +
    '<div class="chart">' + dailyBars(usage.daily, "tokens") + "</div>" +
    "<h2>我的常用模型</h2>" +
    '<table><thead><tr><th>模型</th><th>请求</th><th>Tokens</th><th>Credits(估算)</th></tr></thead><tbody>' +
    (top || '<tr><td colspan="4" class="muted">暂无数据</td></tr>') + "</tbody></table>" +
    "<h2>团队帐号状态（只读）</h2>" +
    '<div class="accts">' + accounts + "</div>";
}

// ---------- admin view ----------
async function renderAdmin() {
  let accts, users, ana;
  try {
    [accts, users, ana] = await Promise.all([api("/api/admin/accounts"), api("/api/admin/users"), api("/api/admin/analytics")]);
  } catch (e) { $("#view").innerHTML = '<div class="muted">' + e.message + "</div>"; return; }
  const accounts = accts.accounts.map(a => acctCard(a, true)).join("");
  const usersRows = users.users.map(u =>
    '<tr class="' + (u.revoked ? "revoked" : "") + '">' +
    "<td>" + u.name + '</td><td class="mono">' + u.key + "</td><td>" + u.created_at.slice(0, 10) + "</td>" +
    "<td>" + u.month_events + "</td><td>" + fmtNum(u.month_tokens) + "</td><td>" + fmtNum(u.month_credits) + "</td>" +
    "<td>" + (u.revoked ? "已吊销" : '<button data-revoke="' + u.key + '">吊销</button>') + "</td></tr>").join("");
  const row = (r) => "<tr><td>" + r.key + "</td><td>" + r.events + "</td><td>" + fmtNum(r.tokens) + "</td><td>" + fmtNum(r.credits) + "</td></tr>";
  const byUser = (ana.by_user || []).slice(0, 10).map(row).join("");
  const byModel = (ana.by_model || []).slice(0, 10).map(row).join("");
  const fromDay = ana.from ? new Date(ana.from * 1000).toISOString().slice(0, 10) : "";
  $("#view").innerHTML =
    "<h2>帐号（" + accts.accounts.length + "）</h2>" +
    '<div class="accts">' + accounts + "</div>" +
    "<h2>团队用量（" + fromDay + " 起）</h2>" +
    '<div class="chart tall">' + dailyBars(ana.daily, "tokens") + "</div>" +
    '<div class="cols"><div><h3>按成员</h3><table><thead><tr><th>成员</th><th>请求</th><th>Tokens</th><th>Credits</th></tr></thead><tbody>' +
    (byUser || '<tr><td colspan="4" class="muted">暂无</td></tr>') + "</tbody></table></div>" +
    '<div><h3>按模型</h3><table><thead><tr><th>模型</th><th>请求</th><th>Tokens</th><th>Credits</th></tr></thead><tbody>' +
    (byModel || '<tr><td colspan="4" class="muted">暂无</td></tr>') + "</tbody></table></div></div>" +
    "<h2>成员 Key（本月用量）</h2>" +
    '<table><thead><tr><th>名称</th><th>Key</th><th>创建</th><th>请求</th><th>Tokens</th><th>Credits</th><th></th></tr></thead><tbody>' +
    (usersRows || '<tr><td colspan="7" class="muted">暂无成员 — 先创建</td></tr>') + "</tbody></table>" +
    '<div class="create"><input id="newname" placeholder="新成员名称"><button id="newuser">创建代理 Key</button><span id="newkey" class="mono"></span></div>';
  bindAdmin(accts);
}

function bindAdmin(accts) {
  document.querySelectorAll("[data-act]").forEach(b => { b.onclick = async () => {
    const id = b.dataset.id;
    if (b.dataset.act === "toggle") {
      const a = accts.accounts.find(x => x.id === id);
      await api("/api/admin/accounts/" + id, { method: "PATCH", body: { disabled: !a.disabled } });
      refresh();
    } else if (b.dataset.act === "clear") {
      await api("/api/admin/accounts/" + id + "/clear-exhausted", { method: "POST" }); refresh();
    } else if (b.dataset.act === "reconcile") {
      const v = prompt("该帐号当前真实剩余量（从 Token Plan 控制台或 CLI 读取，单位与帐号一致）：");
      if (v === null) return;
      const remaining = parseFloat(v);
      if (isNaN(remaining) || remaining < 0) return alert("数字无效");
      await api("/api/admin/accounts/" + id + "/reconcile", { method: "POST", body: { remaining } });
      refresh();
    } else if (b.dataset.act === "patch") {
      const a = accts.accounts.find(x => x.id === id);
      const mc = prompt("最大并发数（当前 " + a.max_concurrent + "）：", String(a.max_concurrent));
      if (mc === null) return;
      const body = { max_concurrent: parseInt(mc, 10) };
      const q = prompt("月度额度（当前 " + (a.quota ?? "未知") + "，留空不改）：", "");
      if (q !== null && q !== "") body.monthly_quota = parseFloat(q);
      await api("/api/admin/accounts/" + id, { method: "PATCH", body });
      refresh();
    }
  }; });
  document.querySelectorAll("[data-revoke]").forEach(b => { b.onclick = async () => {
    if (!confirm("吊销该 Key？该成员将立即失去访问。")) return;
    await api("/api/admin/users/" + b.dataset.revoke + "/revoke", { method: "POST" });
    refresh();
  }; });
  $("#newuser").onclick = async () => {
    const name = $("#newname").value.trim() || "member";
    const r = await api("/api/admin/users", { method: "POST", body: { name } });
    $("#newkey").textContent = "新 Key: " + r.key + "（仅此次显示）";
    refresh();
  };
}

$("#connect").onclick = connect;
$("#key").addEventListener("keydown", e => { if (e.key === "Enter") connect(); });
try {
  const k = localStorage.getItem("tb_key");
  if (k) { $("#key").value = k; state.key = k; connect(); }
} catch (e) {}
```

**Step 3: Write web/styles.css**

```css
:root { --bg:#0f1420; --panel:#1a2233; --panel2:#212c42; --text:#e8edf7; --muted:#8b96ad;
  --ok:#22c55e; --bad:#ef4444; --off:#6b7280; --accent:#6366f1; }
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--text); font:14px/1.5 system-ui,"PingFang SC","Microsoft YaHei",sans-serif; }
header { display:flex; justify-content:space-between; align-items:center; padding:12px 20px; background:var(--panel); position:sticky; top:0; z-index:5; flex-wrap:wrap; gap:8px; }
.brand { font-weight:700; font-size:16px; }
.login { display:flex; gap:8px; align-items:center; }
#key { width:320px; max-width:60vw; padding:8px 10px; border-radius:8px; border:1px solid #334; background:var(--panel2); color:var(--text); }
button { padding:8px 14px; border-radius:8px; border:1px solid #334; background:var(--panel2); color:var(--text); cursor:pointer; }
button:hover { border-color:var(--accent); }
main { padding:20px; max-width:1100px; margin:0 auto; }
h2 { margin:26px 0 10px; font-size:16px; } h3 { margin:14px 0 8px; font-size:14px; color:var(--muted); }
.muted { color:var(--muted); } .mono { font-family:ui-monospace,monospace; font-size:12px; }
footer { text-align:center; padding:16px; color:var(--muted); font-size:12px; }
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin-bottom:8px; }
.card { background:var(--panel); border-radius:12px; padding:14px; }
.card-label { color:var(--muted); font-size:12px; } .card-value { font-size:24px; font-weight:700; margin-top:4px; }
table { width:100%; border-collapse:collapse; background:var(--panel); border-radius:12px; overflow:hidden; }
th,td { text-align:left; padding:8px 12px; border-bottom:1px solid #2a3550; }
th { color:var(--muted); font-weight:500; font-size:12px; }
tr.revoked td { opacity:.45; }
.chart { display:flex; align-items:flex-end; gap:4px; height:120px; padding:10px 0; }
.chart.tall { height:160px; }
.day { flex:1; display:flex; flex-direction:column; justify-content:flex-end; align-items:center; gap:4px; height:100%; }
.day .daybar { width:100%; max-width:26px; background:var(--accent); border-radius:4px 4px 0 0; }
.day span { font-size:10px; color:var(--muted); }
.accts { display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:12px; }
.acct { background:var(--panel); border-radius:12px; padding:12px; }
.acct.disabled { opacity:.55; }
.acct-head { display:flex; justify-content:space-between; gap:8px; margin-bottom:6px; font-weight:600; }
.bar { height:8px; background:var(--panel2); border-radius:4px; overflow:hidden; margin:6px 0; }
.bar div { height:100%; background:var(--ok); }
.bar.bad div { background:var(--bad); } .bar.off div { background:var(--off); }
.tags { display:flex; gap:6px; flex-wrap:wrap; min-height:0; }
.tag { font-size:11px; padding:2px 8px; border-radius:10px; }
.tag.bad { background:rgba(239,68,68,.15); color:var(--bad); }
.tag.off { background:rgba(107,114,128,.2); color:var(--off); }
.tag.ok { background:rgba(34,197,94,.15); color:var(--ok); }
.actions { display:flex; gap:6px; margin-top:8px; flex-wrap:wrap; }
.actions button { padding:4px 10px; font-size:12px; }
.cols { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
@media (max-width:800px){ .cols{grid-template-columns:1fr;} }
.create { display:flex; gap:8px; align-items:center; margin-top:10px; }
.create input { width:220px; padding:8px 10px; border-radius:8px; border:1px solid #334; background:var(--panel2); color:var(--text); }
```

**Step 4: Add content tests to tests/web_api_test.rs**

```rust
#[tokio::test]
async fn ui_assets_contain_expected_content() {
  let r = app().oneshot(get("/", None)).await.unwrap();
  let body = axum::body::to_bytes(r.into_body(), 100_000).await.unwrap();
  let html = String::from_utf8_lossy(&body);
  assert!(html.contains("TokenBalancer"));
  assert!(html.contains("/app.js"));

  let r = app().oneshot(get("/app.js", None)).await.unwrap();
  let body = axum::body::to_bytes(r.into_body(), 200_000).await.unwrap();
  let js = String::from_utf8_lossy(&body);
  assert!(js.contains("/api/whoami"));
  assert!(js.contains("/api/admin/accounts"));

  let r = app().oneshot(get("/styles.css", None)).await.unwrap();
  assert!(r.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap().contains("text/css"));
}
```

**Step 5: Run tests**

Run: `cargo test --test web_api_test`
Expected: PASS (6 tests).

**Step 6: Manual verification (required before commit)**

```bash
cargo build
TOKENBALANCER_CONFIG=config.example.toml ./target/debug/tokenbalancer serve &
curl -s localhost:8787/healthz | head -c 300
curl -s localhost:8787/ | head -c 200
curl -s -H "Authorization: Bearer tba_replace_me" localhost:8787/api/whoami
```
Open http://127.0.0.1:8787/ in a browser; paste the admin key; verify the admin
dashboard renders (empty accounts OK). Kill the server.

**Step 7: Commit**

```bash
git add web/ tests/web_api_test.rs
git commit -m "feat: embedded web UI (admin dashboard + user usage view) (T11)"
```

---

### Task 12: main.rs wiring, end-to-end test, README, final verification

**Files:**
- Modify: `src/main.rs` (full serve wiring)
- Create: `tests/e2e_proxy_test.rs`
- Modify: `README.md`
- Modify: `config.example.toml` (final pass)

**Step 1: Implement src/main.rs**

```rust
// src/main.rs
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt()
    .with_env_filter(
      EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    )
    .init();

  let args: Vec<String> = std::env::args().collect();
  let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("serve");
  match cmd {
    "serve" => {
      let path = args.get(2).cloned().unwrap_or_else(|| "config.toml".into());
      let cfg = tokenbalancer::config::load(&path)?;
      let store = tokenbalancer::store::Store::open(&cfg.server.db_path)?;
      let runtime = tokenbalancer::state::Runtime::load(&cfg, store).await?;
      let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()?;
      let state = tokenbalancer::proxy::AppState {
        runtime,
        client,
        queue_timeout: Duration::from_secs(cfg.server.queue_timeout_secs),
      };
      let app = tokenbalancer::proxy::router(state)
        .layer(tower_http::cors::CorsLayer::permissive());
      let listener = tokio::net::TcpListener::bind(&cfg.server.listen).await?;
      let port = cfg.server.listen.rsplit(':').next().unwrap_or("8787");
      println!("TokenBalancer 已启动  http://{port}");
      println!("  代理 (OpenAI):   http://<host>:{port}/v1");
      println!("  代理 (Anthropic): http://<host>:{port}/apps/anthropic");
      println!("  管理页面:       http://<host>:{port}/");
      axum::serve(listener, app).await?;
      Ok(())
    }
    "key" => {
      // generate a proxy key to paste into config [[users]] or the web UI
      println!("{}", tokenbalancer::web::gen_user_key());
      Ok(())
    }
    _ => {
      eprintln!("用法: tokenbalancer serve [config.toml] | tokenbalancer key new");
      Ok(())
    }
  }
}
```

**Step 2: Write the end-to-end test**

```rust
// tests/e2e_proxy_test.rs
use std::sync::Arc;
use std::time::Duration;
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use serde_json::{json, Value};

// ---------- mock upstream (single server; accounts distinguished by Bearer key) ----------
const SSE_FINAL: &str = concat!(
  "data: {\\\"id\\\":\\\"x\\\",\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"hi\\\"}}]}\\n\\n",
  "data: {\\\"id\\\":\\\"x\\\",\\\"choices\\\":[],\\\"usage\\\":{\\\"prompt_tokens\\\":300,\\\"completion_tokens\\\":150,\\\"total_tokens\\\":450}}\\n\\n",
  "data: [DONE]\\n\\n"
);

#[derive(Clone)]
struct Mock {
  calls: Arc<std::sync::Mutex<Vec<(String, String)>>>, // (auth, path)
  mode: Arc<std::sync::Mutex<String>>,                // "ok" | "quota-A" | "slow"
}

async fn mock_handler(
  axum::extract::State(m): axum::extract::State<Mock>,
  req: Request<Body>,
) -> axum::response::Response {
  let auth = req.headers().get(header::AUTHORIZATION)
    .and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
  let path = req.uri().path().to_string();
  m.calls.lock().unwrap().push((auth.clone(), path.clone()));
  let body = axum::body::to_bytes(req.into_body(), 1_000_000).await.unwrap();
  let bv: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
  let want_stream = bv.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
  let mode = m.mode.lock().unwrap().clone();

  if mode == "quota-A" && auth == "Bearer sk-sp-A" {
    return axum::response::Response::builder().status(StatusCode::TOO_MANY_REQUESTS)
      .header(header::CONTENT_TYPE, "application/json")
      .body(Body::from(r#"{"error":{"message":"Throttling.AllocationQuota: insufficient_quota","code":"AllocationQuota"}}"#))
      .unwrap();
  }
  if mode == "slow" {
    tokio::sleep(Duration::from_millis(300)).await;
  }
  if want_stream {
    return axum::response::Response::builder().status(StatusCode::OK)
      .header(header::CONTENT_TYPE, "text/event-stream")
      .body(Body::from(SSE_FINAL)).unwrap();
  }
  axum::response::Response::builder().status(StatusCode::OK)
    .header(header::CONTENT_TYPE, "application/json")
    .body(Body::from(
      r#"{"id":"x","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":300,"completion_tokens":150,"total_tokens":450}}"#))
    .unwrap()
}

// ---------- harness ----------
#[derive(Clone)]
struct Harness { port: u16, mock: Mock, store: Arc<tokenbalancer::store::Store> }

fn account_conf(id: &str, key: &str, base: &str, quota: Option<f64>, maxc: u32) -> tokenbalancer::config::AccountConf {
  tokenbalancer::config::AccountConf {
    id: id.into(), label: Some(id.into()), api_key: key.into(), region: None,
    base_url_openai: Some(base.into()), base_url_anthropic: None,
    seat_tier: None, balance_unit: None, monthly_quota: quota, cycle_start: None,
    max_concurrent: Some(maxc), disabled: None,
  }
}

async fn start(a_quota: Option<f64>, a_max: u32, b_quota: Option<f64>, b_max: u32) -> Harness {
  let mock = Mock { calls: Arc::new(std::sync::Mutex::new(Vec::new())), mode: Arc::new(std::sync::Mutex::new("ok".into())) };
  let mock_app = Router::new().route("/{*rest}", post(mock_handler)).with_state(mock.clone());
  let ml = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let mport = ml.local_addr().unwrap().port();
  tokio::spawn(axum::serve(ml, mock_app));
  tokio::sleep(Duration::from_millis(100)).await;
  let base = format!("http://127.0.0.1:{mport}");

  let store = Arc::new(tokenbalancer::store::Store::open(":memory:").unwrap());
  let cfg = tokenbalancer::config::Config {
    server: tokenbalancer::config::ServerConf { listen: "127.0.0.1:0".into(), admin_key: "tba_e2e".into(), db_path: ":memory:".into(), queue_timeout_secs: 5 },
    defaults: tokenbalancer::config::DefaultsConf { region: tokenbalancer::config::Region::Cn, balance_unit: tokenbalancer::config::BalanceUnit::Tokens, max_concurrent: 2 },
    accounts: vec![
      account_conf("A", "sk-sp-A", &base, a_quota, a_max),
      account_conf("B", "sk-sp-B", &base, b_quota, b_max),
    ],
    credit_rates: Default::default(),
    users: vec![tokenbalancer::config::UserConf { key: "tbu_e2e".into(), name: "tester".into() }],
  };
  let runtime = tokenbalancer::state::Runtime::load(&cfg, store.clone()).await.unwrap();
  let state = tokenbalancer::proxy::AppState { runtime, client: reqwest::Client::new(), queue_timeout: Duration::from_secs(5) };
  let app = tokenbalancer::proxy::router(state);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = listener.local_addr().unwrap().port();
  tokio::spawn(axum::serve(listener, app));
  tokio::sleep(Duration::from_millis(100)).await;
  Harness { port, mock, store }
}


async fn call(h: &Harness, path: &str, key: &str, body: Value) -> (StatusCode, String, Vec<(String, String)>) {
  let url = format!("http://127.0.0.1:{}{}", h.port, path);
  let r = reqwest::Client::new()
    .post(&url)
    .header(header::AUTHORIZATION, format!("Bearer {key}"))
    .json(&body)
    .send().await.unwrap();
  let status = r.status();
  let text = r.text().await.unwrap();
  let calls = h.mock.calls.lock().unwrap().clone();
  (status, text, calls)
}

// Default balance unit is Credits, so seed events must carry credits —
// token fields do not move the credits window.
fn seed_usage(store: &Arc<tokenbalancer::store::Store>, account: &str, credits: f64) {
  let ev = tokenbalancer::store::UsageEvent {
    ts: chrono::Utc::now(), user_id: "seed".into(), account_id: account.into(),
    model: "seed".into(), input: 0, cached: 0, output: 0,
    credits, latency_ms: 0, status: 200, stream: false, parse_error: false,
  };
  store.insert_event(&ev).unwrap();
}

fn chat(stream: bool) -> Value {
  json!({"model":"qwen3.7-max","stream":stream,"messages":[{"role":"user","content":"hi"}]})
}
```

**Tests**

```rust
#[tokio::test]
async fn bad_key_401() {
  let h = start(None, 2, None, 2).await; // (None,None) quotas
  let (status, _, _) = call(&h, "/v1/chat/completions", "tbu_wrong", chat(false)).await;
  assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn routes_to_highest_remaining_and_records_usage() {
  let h = start(Some(1_000_000.0), 2, Some(1_000_000.0), 2).await;
  // pre-consume B so A is clearly higher
  seed_usage(&h.store, "B", 900_000.0); // B pct 0.1 -> A strictly preferred
  let (status, text, calls) = call(&h, "/v1/chat/completions", "tbu_e2e", chat(false)).await;
  assert_eq!(status, StatusCode::OK);
  let v: Value = serde_json::from_str(&text).unwrap();
  assert_eq!(v["usage"]["prompt_tokens"], 300);
  assert_eq!(calls.len(), 1);
  assert_eq!(calls[0].0, "Bearer sk-sp-A");
  // usage recorded for the user
  let (i, _c, o, _cr, n) = h.store.totals_for("tbu_e2e", 0).unwrap();
  assert_eq!((i, o, n), (300, 150, 1));
}

#[tokio::test]
async fn balancing_shifts_to_other_account_after_consumption() {
  let h = start(Some(5.0), 2, Some(1_000_000.0), 2).await;
  seed_usage(&h.store, "B", 50_000.0); // B pct 0.95 vs A 1.0
  // each request costs 2.1 credits (300/500 in + 150/100 out); A falls below B after req1; B serves req2/req3
  for _ in 0..3 {
    let (status, _, _) = call(&h, "/v1/chat/completions", "tbu_e2e", chat(false)).await;
    assert_eq!(status, StatusCode::OK);
  }
  let calls = h.mock.calls.lock().unwrap().clone();
  assert_eq!(calls[0].0, "Bearer sk-sp-A");
  assert_eq!(calls[1].0, "Bearer sk-sp-B"); // A remaining fraction (0.58) < B (0.95)
  assert_eq!(calls[2].0, "Bearer sk-sp-B");
}

#[tokio::test]
async fn concurrency_cap_queues_then_proceeds() {
  let h = start_single().await; // one account, max_concurrent=1, slow mock
  h.mock.mode.lock().unwrap().replace("slow".into());
  let (a, b) = (h.clone(), h.clone());
  let r1 = tokio::spawn(async move { call(&a, "/v1/chat/completions", "tbu_e2e", chat(false)).await });
  tokio::sleep(Duration::from_millis(50)).await; // first request in flight
  let r2 = tokio::spawn(async move { call(&b, "/v1/chat/completions", "tbu_e2e", chat(false)).await });
  let (s1, _, _) = r1.await.unwrap();
  let (s2, _, calls) = r2.await.unwrap();
  assert_eq!(s1, StatusCode::OK);
  assert_eq!(s2, StatusCode::OK); // queued ~300ms then served (queue_timeout 5s)
  assert_eq!(calls.len(), 2);
  assert!(calls.iter().all(|(k, _)| k == "Bearer sk-sp-A"));
}

async fn start_single() -> Harness {
  // single-account variant: A max_concurrent=1 quota 10_000
  let mock = Mock { calls: Arc::new(std::sync::Mutex::new(Vec::new())), mode: Arc::new(std::sync::Mutex::new("ok".into())) };
  let mock_app = Router::new().route("/{*rest}", post(mock_handler)).with_state(mock.clone());
  let ml = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let mport = ml.local_addr().unwrap().port();
  tokio::spawn(axum::serve(ml, mock_app));
  tokio::sleep(Duration::from_millis(100)).await;
  let base = format!("http://127.0.0.1:{mport}");
  let store = Arc::new(tokenbalancer::store::Store::open(":memory:").unwrap());
  let cfg = tokenbalancer::config::Config {
    server: tokenbalancer::config::ServerConf { listen: "127.0.0.1:0".into(), admin_key: "tba".into(), db_path: ":memory:".into(), queue_timeout_secs: 5 },
    defaults: tokenbalancer::config::DefaultsConf { region: tokenbalancer::config::Region::Cn, balance_unit: tokenbalancer::config::BalanceUnit::Tokens, max_concurrent: 2 },
    accounts: vec![account_conf("A", "sk-sp-A", &base, Some(10_000.0), 1)],
    credit_rates: Default::default(),
    users: vec![tokenbalancer::config::UserConf { key: "tbu_e2e".into(), name: "t".into() }],
  };
  let runtime = tokenbalancer::state::Runtime::load(&cfg, store.clone()).await.unwrap();
  let state = tokenbalancer::proxy::AppState { runtime, client: reqwest::Client::new(), queue_timeout: Duration::from_secs(5) };
  let app = tokenbalancer::proxy::router(state);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = listener.local_addr().unwrap().port();
  tokio::spawn(axum::serve(listener, app));
  tokio::sleep(Duration::from_millis(100)).await;
  Harness { port, mock, store }
}

#[tokio::test]
async fn quota_429_marks_exhausted_and_fails_over() {
  let h = start(Some(1_000_000.0), 2, Some(1_000_000.0), 2).await;
  seed_usage(&h.store, "B", 500_000.0); // A pct 1.0 vs B 0.5
  h.mock.mode.lock().unwrap().replace("quota-A".into());
  let (s1, t1, _) = call(&h, "/v1/chat/completions", "tbu_e2e", chat(false)).await;
  assert_eq!(s1, StatusCode::TOO_MANY_REQUESTS);
  assert!(t1.contains("AllocationQuota"));
  let (s2, _, calls) = call(&h, "/v1/chat/completions", "tbu_e2e", chat(false)).await;
  assert_eq!(s2, StatusCode::OK); // failed over to B
  assert_eq!(calls.last().unwrap().0, "Bearer sk-sp-B");
  // healthz reflects exhaustion
  let r = reqwest::get(format!("http://127.0.0.1:{}/healthz", h.port)).await.unwrap();
  let v: Value = r.json().await.unwrap();
  let a = v["accounts"][0];
  assert_eq!(a["id"], "A");
  assert_eq!(a["exhausted"], true);
}

#[tokio::test]
async fn sse_stream_relay_and_usage_recorded() {
  let h = start(None, 2, None, 2).await;
  let url = format!("http://127.0.0.1:{}/v1/chat/completions", h.port);
  let r = reqwest::Client::new().post(&url)
    .header(header::AUTHORIZATION, "Bearer tbu_e2e")
    .json(&chat(true)).send().await.unwrap();
  assert!(r.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap().contains("text/event-stream"));
  let text = r.text().await.unwrap();
  assert!(text.contains("data:"));
  assert!(text.contains("usage"));
  // wait for the record to land (spawned task)
  tokio::sleep(Duration::from_millis(300)).await;
  let (i, _c, o, _cr, n) = h.store.totals_for("tbu_e2e", 0).unwrap();
  assert_eq!((i, o, n), (300, 150, 1));
}
```


**Step 3: Run the full test suite**

Run: `cargo test`
Expected: ALL PASS (unit + store + forward + usage + balance + state + proxy_unit + web_api + e2e).

**Step 4: Write README.md**

```markdown
# TokenBalancer

Qwen Token Plan 团队版 多帐号自动平衡代理。

团队成员把 AI 工具的 Base URL 指向本程序、使用管理员分配的**代理 Key** 作为 API Key；
程序按各上游帐号的**剩余额度**自动路由（余额多者优先），每帐号并发上限默认 2（可配置），
并内置 Web 管理页面（成员看自己用量，管理员看全部帐号 + 团队分析 + 对账）。

## 快速开始

```bash
cargo build --release
# 1) 复制并编辑配置
cp config.example.toml config.toml   # 填入各席位的 sk-sp- key、admin_key、listen
# 2) 启动
./target/release/tokenbalancer serve config.toml
```

启动后：
- 代理（OpenAI 兼容）：`http://<host>:8787/v1`
- 代理（Anthropic 兼容）：`http://<host>:8787/apps/anthropic`
- 管理页面：`http://<host>:8787/`
- 健康检查：`http://<host>:8787/healthz`

## 获取 Token Plan 团队版 Key 与 Base URL

1. 在 Token Plan 管理平台购买/管理团队版订阅：
   - 国内千问云：https://tokenplan-enterprise.qianwenai.com
   - 国际 QwenCloud：https://tokenplan-enterprise.qwencloud.com
2. 成员管理 → 分配席位 → 生成专属 API Key（格式 `sk-sp-xxxxx`，只显示一次）。
3. API Key 页面查看套餐专属 Base URL（国内 `token-plan.cn-beijing.maas.aliyuncs.com`，
   国际 `token-plan.ap-southeast-1.maas.aliyuncs.com`；程序默认已内置，一般无需改）。

鉴权方式：`Authorization: Bearer <sk-sp-...>`（不是 x-api-key）。

## 团队成员接入

管理员在 Web 页面「成员 Key」中创建代理 Key（或在配置 `[[users]]` 中预置），
成员在工具中配置：

| 工具类型 | Base URL | API Key |
|---|---|---|
| OpenAI 兼容（Cursor / Qwen Code / Codex / OpenCode / Cherry Studio…） | `http://<host>:8787/v1` | 代理 Key |
| Anthropic 兼容（Claude Code 等） | `http://<host>:8787/apps/anthropic` | 代理 Key |

## 管理页面

- **成员视图**（代理 Key 登录）：近 14 天用量、按天趋势、常用模型、团队帐号状态（只读）。
- **管理员视图**（admin_key 登录）：
  - 每个帐号：剩余量条、在途并发、禁用/启用、**对账**（粘贴控制台/CLI 读到的真实剩余量）、清除耗尽、调整并发/额度；
  - 团队分析：按天用量趋势、按成员/模型聚合；
  - 成员 Key 管理：创建（Key 只显示一次）、吊销。

## 对账（校准剩余量）

程序通过代理自身流量记账估算剩余量。要校准：在 Token Plan 控制台（Organization
Usage）或官方 CLI（`qianwen usage summary --format json` → `token_plan.remainingCredits`）
读取该帐号真实剩余 Credits，在管理页点击「对账…」填入。此后该帐号剩余量 =
对账值 − 对账后的新消耗。

## 平衡策略

- 每个请求选择「剩余比例（remaining/quota）最高」且有并发空闲的帐号；
- 全部满并发 → 排队（默认 5s，`queue_timeout_secs`），超时 503 + Retry-After；
- 帐号被上游 429（AllocationQuota / insufficient_quota）判定耗尽 → 自动跳过直至
  对账/周期开始/手动清除；
- 余额单位可选 `tokens`（默认，来自响应 usage，自包含）或 `credits`
  （按模型费率估算，见 config `[credit_rates]`，可用对账校准）。

## 安全

- `config.toml` 含全部 sk-sp key 与 admin_key：**不要提交到 git**（.gitignore 已排除
  config.local.toml；建议文件名用 config.local.toml 并 `chmod 600`）。
- SQLite 文件（`data/`）也含 key，同样注意权限。
- 生产部署建议置于 TLS 反向代理（Caddy/nginx）之后。

## 合规提示

Token Plan 条款要求专属 Key 用于交互式 AI 工具及其发起的调用，禁止应用后端/
批量任务等用法。本程序是团队成员**交互式工具流量**的透明转发层（不产生额外调用），
请团队自行评估是否符合其订阅条款。

## 已知限制（v1）

- credits 费率表是近似值（官方未公开精确费率），对账可校准；
- 流式响应中途的 4xx 不做额度耗尽检测（仅非流式错误体检测）；
- 单实例部署（无多节点/HA）；用量统计为本地窗口（默认当月）聚合。

## 开发

```bash
cargo test            # 全量测试（含端到端 mock 上游）
cargo clippy          # 静态检查
```
```

**Step 5: Final verification**

Run:
```bash
cargo test --release
cargo build --release
# smoke: start with example config, hit endpoints, stop
./target/release/tokenbalancer serve config.example.toml &
sleep 1
curl -s localhost:8787/healthz
curl -s -o /dev/null -w "%{http_code}" localhost:8787/
kill %1
```
Expected: all tests green; healthz returns the two example accounts; UI returns 200.

**Step 6: Commit**

```bash
git add src/main.rs tests/e2e_proxy_test.rs README.md config.example.toml
git commit -m "feat: main wiring, e2e tests, README — TokenBalancer v0.1 complete (T12)"
```

---

## Execution notes

- Total: 12 tasks. Pure-logic tasks (1–6, 8) are fast; 9–12 carry the integration.
- Every `cargo test` should stay green at commit time; if a fix touches an
  earlier task's code, extend that test rather than deleting coverage.
- After Task 12, verify the feature end-to-end with the user: point a real tool
  (e.g. Qwen Code) at the proxy with a real seat key (if the user has one), watch
  /healthz and the admin UI update live.
- Branch: feature/tokenbalancer. Finishing options (merge / PR / cleanup) are the
  user's call after verification.

