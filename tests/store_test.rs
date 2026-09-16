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
  assert_eq!(rows.len(), 1);
  assert_eq!(rows[0].id, "a1");
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
  // future since -> nothing
  let (i2, _, _, _, n2) = s.sum_since("a1", Utc::now().timestamp() + 3600).unwrap();
  assert_eq!((i2, n2), (0, 0));
}
