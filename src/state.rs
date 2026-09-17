// src/state.rs
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use chrono::{Datelike, DateTime, TimeZone, Utc};

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
  pub unit: Mutex<BalanceUnit>,
  pub quota: Mutex<Option<f64>>,      // in unit terms; None = unknown -> neutral pct 0.5
  pub region: Region,                // billing zone for cycle math (UTC+8 Cn, UTC Intl)
  pub cycle_day: u32,                // day-of-month anchor of the billing cycle (1..31; 1 when unset)
  pub max_concurrent: AtomicU32,
  pub disabled: AtomicBool,
  pub exhausted: AtomicBool,
  pub in_flight: AtomicU32,
  /// (at_unix, remaining in unit terms) — admin reconciliation baseline.
  reconciled: Mutex<Option<(i64, f64)>>,
}

pub struct Slot {
  acct: Arc<RuntimeAccount>,
}

impl Drop for Slot {
  fn drop(&mut self) {
    self.acct.in_flight.fetch_sub(1, Ordering::Release);
  }
}

impl std::fmt::Debug for Runtime {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Runtime").field("accounts", &self.inner.accounts.len()).finish()
  }
}

/// Days in a calendar month (1..31).
fn days_in_month(year: i32, month: u32) -> u32 {
  let first = chrono::NaiveDate::from_ymd_opt(year, month, 1).unwrap();
  let next_first = if month == 12 {
    chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
  } else {
    chrono::NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
  };
  (next_first - first).num_days() as u32
}

/// (year, month) of the month preceding (year, month).
fn prev_month(year: i32, month: u32) -> (i32, u32) {
  if month == 1 { (year - 1, 12) } else { (year, month - 1) }
}

/// 00:00 local (region's zone: UTC+8 for Cn, UTC otherwise) unix seconds of
/// the MOST RECENT occurrence of the cycle anchor day: the anchor day of the
/// current month once it has passed locally, else the previous month's
/// occurrence. An anchor day that does not exist in a month (e.g. day 31 in
/// February) clamps to that month's last day.
pub fn cycle_window_start(day: u32, region: Region) -> i64 {
  let off = match region {
    Region::Cn => chrono::FixedOffset::east_opt(8 * 3600).unwrap(),
    _ => chrono::FixedOffset::east_opt(0).unwrap(),
  };
  let local = Utc::now().with_timezone(&off);
  let anchor = day.max(1);
  let (year, month) = (local.year(), local.month());
  let current_d = anchor.min(days_in_month(year, month));
  if local.day() >= current_d {
    let naive = chrono::NaiveDate::from_ymd_opt(year, month, current_d)
      .unwrap()
      .and_hms_opt(0, 0, 0)
      .unwrap();
    off.from_local_datetime(&naive).unwrap().timestamp()
  } else {
    let (py, pm) = prev_month(year, month);
    let d = anchor.min(days_in_month(py, pm));
    let naive = chrono::NaiveDate::from_ymd_opt(py, pm, d)
      .unwrap()
      .and_hms_opt(0, 0, 0)
      .unwrap();
    off.from_local_datetime(&naive).unwrap().timestamp()
  }
}

/// Day-of-month anchor from a "YYYY-MM-DD" cycle_start string; 1 when absent
/// or unparseable (only the day of the anchor matters, not the year/month).
fn parse_cycle_day(s: &str) -> u32 {
  chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
    .map(|d| d.day())
    .unwrap_or(1)
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
        region: format!("{region:?}"),
        base_url_openai: a.base_url_openai.clone().unwrap_or_else(|| cfg_mod::base_url_openai(region).into()),
        base_url_anthropic: a.base_url_anthropic.clone().unwrap_or_else(|| cfg_mod::base_url_anthropic(region).into()),
        seat_tier: a.seat_tier.map(|t| format!("{t:?}")).unwrap_or_default(),
        balance_unit: format!("{unit:?}"),
        monthly_quota: quota,
        cycle_start,
        max_concurrent: maxc,
        // Only applied on the INSERT path of the upsert: an account that
        // already exists keeps its runtime disabled/exhausted state.
        disabled: a.disabled.unwrap_or(false),
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
        unit: Mutex::new(unit),
        quota: Mutex::new(if r.monthly_quota > 0.0 { Some(r.monthly_quota) } else { None }),
        region,
        cycle_day: parse_cycle_day(&r.cycle_start),
        max_concurrent: AtomicU32::new(r.max_concurrent),
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

  /// All accounts in stable index order.
  pub fn iter_accounts(&self) -> Vec<std::sync::Arc<RuntimeAccount>> {
    self.inner.accounts.iter().cloned().collect()
  }

  /// Admin field patch (persists to store, updates in-memory account).
  pub fn patch(&self, id: &str, max_concurrent: Option<u32>, monthly_quota: Option<f64>, unit: Option<BalanceUnit>) {
    if let Some(a) = self.account(id) {
      if let Some(m) = max_concurrent { a.max_concurrent.store(m.max(1), Ordering::Release); }
      if let Some(q) = monthly_quota { *a.quota.lock().unwrap() = if q > 0.0 { Some(q) } else { None }; }
      if let Some(u) = unit { *a.unit.lock().unwrap() = u; }
      let maxc = a.max_concurrent.load(Ordering::Acquire);
      let quota = *a.quota.lock().unwrap();
      let u = *a.unit.lock().unwrap();
      let _ = self.inner.store.patch_account(
        &a.id,
        maxc,
        quota.unwrap_or(0.0),
        match u { BalanceUnit::Credits => "Credits", BalanceUnit::Tokens => "Tokens" },
      );
    }
  }

  /// Try to take a concurrency slot for the account; None when at cap.
  pub fn try_acquire(&self, id: &str) -> Option<Slot> {
    let idx = *self.inner.id_to_idx.get(id)?;
    let acct = &self.inner.accounts[idx];
    loop {
      let cur = acct.in_flight.load(Ordering::Acquire);
      if cur >= acct.max_concurrent.load(Ordering::Acquire) { return None; }
      match acct.in_flight.compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => return Some(Slot { acct: Arc::clone(acct) }),
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
        max_concurrent: a.max_concurrent.load(Ordering::Acquire),
        disabled: a.disabled.load(Ordering::Acquire),
        exhausted: a.exhausted.load(Ordering::Acquire),
      }
    }).collect()
  }

  /// (remaining in unit terms, remaining fraction 0..1) for one account.
  pub fn remaining_of(&self, a: &RuntimeAccount) -> (f64, f64) {
    if a.exhausted.load(Ordering::Acquire) { return (0.0, 0.0); }
    // The cycle window start moves with the calendar: recompute it fresh on
    // every call instead of caching a load-time value.
    let ws = cycle_window_start(a.cycle_day, a.region);
    let window_sum = self.inner.store
      .sum_since(&a.id, ws)
      .unwrap_or((0, 0, 0, 0.0, 0));
    let unit = *a.unit.lock().unwrap();
    let consumed = unit_consumption(&unit, &window_sum);
    let quota = *a.quota.lock().unwrap();
    // Copy the baseline out of the reconciled mutex before issuing any further
    // store call, so no mutex guard is held across a blocking SQLite query.
    let reconciled = *a.reconciled.lock().unwrap();
    let rem = match reconciled {
      Some((at_unix, rem0)) if at_unix >= ws => {
        // reconciliation baseline: true remaining at `at_unix`, minus what flowed after
        let post = self.inner.store.sum_since(&a.id, at_unix).unwrap_or((0, 0, 0, 0.0, 0));
        (rem0 - unit_consumption(&unit, &post)).max(0.0)
      }
      _ => (quota.unwrap_or(0.0) - consumed).max(0.0),
    };
    let pct = match quota {
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
    let _ = self.inner.store.insert_event(&ev);
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
      // keep the DB consistent with the in-memory reset, so a restart does
      // not resurrect a cleared reconciliation baseline
      let _ = self.inner.store.clear_reconciled(&a.id);
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
  let local = Utc::now().with_timezone(&off);
  chrono::NaiveDate::from_ymd_opt(local.year(), local.month(), 1).unwrap().to_string()
}
