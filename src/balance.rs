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
