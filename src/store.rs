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

  pub fn patch_account(&self, id: &str, max_concurrent: u32, monthly_quota: f64, balance_unit: &str) -> anyhow::Result<()> {
    self.conn.lock().unwrap().execute(
      "UPDATE accounts SET max_concurrent=?1, monthly_quota=?2, balance_unit=?3 WHERE id=?4",
      params![max_concurrent as i64, monthly_quota, balance_unit, id])?;
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
    // plan deviation: plan text used `.transpose().ok()` (type error)
    Ok(it.next().transpose()?)
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

  /// (key, input, cached, output, credits, count) grouped by a whitelisted
  /// column within [since_unix, to_unix).
  pub fn group_since(&self, col: &str, since_unix: i64, to_unix: i64) -> anyhow::Result<Vec<(String, u64, u64, u64, f64, u64)>> {
    let col = match col { "user_id" | "account_id" | "model" => col, _ => return Err(anyhow::anyhow!("bad column")) };
    let since = DateTime::<Utc>::from_timestamp(since_unix, 0).unwrap_or_default().to_rfc3339();
    let to = DateTime::<Utc>::from_timestamp(to_unix, 0).unwrap_or_default().to_rfc3339();
    let c = self.conn.lock().unwrap();
    let sql = format!("SELECT {col}, COALESCE(SUM(input),0), COALESCE(SUM(cached),0), COALESCE(SUM(output),0), COALESCE(SUM(credits),0.0), COUNT(*) FROM usage_events WHERE ts>=?1 AND ts<?2 GROUP BY {col} ORDER BY 6 DESC");
    let mut st = c.prepare(&sql)?;
    let rows = st.query_map(params![since, to], |r| {
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
}
