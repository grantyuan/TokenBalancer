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
