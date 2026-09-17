// tests/state_test.rs
use std::sync::Arc;
use chrono::{Datelike, Utc};
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
/// Event with an explicit timestamp and exact credits (credits-mode accounts
/// count the per-event credits sum; record_event would recompute credits).
fn ev_at(account: &str, ts: chrono::DateTime<Utc>, credits: f64) -> tokenbalancer::store::UsageEvent {
  tokenbalancer::store::UsageEvent {
    ts, user_id: "tester".into(), account_id: account.into(), model: "seed".into(),
    input: 0, cached: 0, output: 0, credits, latency_ms: 0, status: 200, stream: false, parse_error: false,
  }
}

#[tokio::test]
async fn cycle_window_ignores_previous_month_usage() {
  // day-1 anchor (explicit "2026-07-01" -> day 1, same as the default):
  // usage from a previous calendar month must not count, even though it was
  // recorded while the process was running.
  let c = AccountConf { id: "a".into(), label: None, api_key: "k".into(), region: None,
    base_url_openai: None, base_url_anthropic: None, seat_tier: Some(SeatTier::Pro),
    balance_unit: None, monthly_quota: None, cycle_start: Some("2026-07-01".into()),
    max_concurrent: None, disabled: None };
  let store = Arc::new(Store::open(":memory:").unwrap());
  let rt = Runtime::load(&cfg(vec![c]), store.clone()).await.unwrap();
  // 35 days ago is always in a previous calendar month (months have <= 31 days),
  // so it must fall before the current window start on any day of the month.
  let old_ts = Utc::now() - chrono::Duration::days(35);
  store.insert_event(&ev_at("a", old_ts, 10_000.0)).unwrap();
  assert_eq!(rt.remaining("a"), 100_000.0, "previous-month usage must not count");
  // the same amount inside the current window does count
  store.insert_event(&ev_at("a", Utc::now(), 10_000.0)).unwrap();
  assert_eq!(rt.remaining("a"), 90_000.0, "in-window usage must count");
}

#[tokio::test]
async fn cycle_window_mid_month_anchor() {
  // Anchor day = 3 days before today in the account's zone (day 1..3 of the
  // month fall back to a fixed day 3). The window start is read back from
  // the same helper the runtime uses, so the assertions hold on any date.
  let off = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
  let local = Utc::now().with_timezone(&off);
  let today = local.day();
  let anchor_day = if today >= 4 { today - 3 } else { 3 };
  let c = AccountConf { id: "a".into(), label: None, api_key: "k".into(), region: None,
    base_url_openai: None, base_url_anthropic: None, seat_tier: Some(SeatTier::Pro),
    balance_unit: None, monthly_quota: None, cycle_start: Some(format!("{:04}-{:02}-{:02}", local.year(), local.month(), anchor_day)),
    max_concurrent: None, disabled: None };
  let store = Arc::new(Store::open(":memory:").unwrap());
  let rt = Runtime::load(&cfg(vec![c]), store.clone()).await.unwrap();
  let ws = tokenbalancer::state::cycle_window_start(anchor_day, Region::Cn);
  // an event just before the window start is not counted
  let old_ts = chrono::DateTime::<Utc>::from_timestamp(ws - 3600, 0).unwrap();
  store.insert_event(&ev_at("a", old_ts, 10_000.0)).unwrap();
  assert_eq!(rt.remaining("a"), 100_000.0, "event before the mid-month anchor must not count");
  // an event now is counted
  store.insert_event(&ev_at("a", Utc::now(), 10_000.0)).unwrap();
  assert_eq!(rt.remaining("a"), 90_000.0, "event after the mid-month anchor must count");
}
