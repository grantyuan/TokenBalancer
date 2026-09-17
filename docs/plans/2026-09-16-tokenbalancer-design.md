# TokenBalancer Design — Qwen Token Plan 团队版 multi-account auto-balancing proxy

Date: 2026-09-16
Status: approved (user confirmed all open decisions)

## 1. Goal

A single Rust service that sits between team members' AI tools (Cursor, Claude Code,
Qwen Code, OpenCode, ...) and several Qwen "Token Plan 团队版" (Team Edition) accounts.
Each team member gets a **proxy key** from the admin; they point their tool at the proxy's
HTTP address and use the proxy key as their API key. The proxy picks which upstream
Token Plan account serves each request so that all accounts' remaining quota stays
roughly balanced, and enforces a per-account concurrency cap (default 2 concurrent
users per account, configurable).

## 2. Research findings (Qwen Token Plan 团队版)

### 2.1 Real endpoints (per-region Base URLs)

| Region | OpenAI-compatible | Anthropic-compatible |
|---|---|---|
| CN 千问云 (default) | https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1 | https://token-plan.cn-beijing.maas.aliyuncs.com/apps/anthropic |
| Intl QwenCloud | https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1 | https://token-plan.ap-southeast-1.maas.aliyuncs.com/apps/anthropic |

Sources: https://platform.qianwenai.com/docs/token-plan/team/token-plan-team-quickstart.md
and https://docs.qwencloud.com/token-plan/team/token-plan-team-quickstart.md
Per-account overrides are possible (config field); the plan-specific Base URL shown on
the console's API Keys page is authoritative.

### 2.2 Keys & auth

- Token Plan seat key: sk-sp-xxxxx, one per seat; shown only once at creation.
- Auth header must be Authorization: Bearer <key> (NOT x-api-key).
- Must not be mixed with pay-as-you-go sk- keys or their base URLs.

### 2.3 Quota model

- Seats: Standard 25,000 Credits/mo, Pro 100,000, Max 250,000 (Intl pricing; CN 团队版
  tiers are the same shape). Over seat quota → org-shared Credit Pack (Intl: 625,000
  Credits/pack) → when everything is exhausted, requests hard-fail
  (429 Throttling.AllocationQuota / insufficient_quota) until the next billing cycle.
- Credits reset at the start of each subscription month; no rollover.
- Credits are deducted per request from input / cached / output tokens with
  model-dependent rates (community data, approximate): e.g. qwen3.6-plus ~5,000
  input-tokens/credit, ~25,000 cached-tokens/credit, ~830 output-tokens/credit;
  headline figures range ~100 (long-context/flagship) to ~1,000 (light models)
  tokens/credit overall. Rates are not a stable public API → treat as configurable
  approximation, with manual reconciliation as the correction mechanism.

### 2.4 How to read remaining quota (the key constraint)

There is **no documented HTTP API that returns remaining credits for a sk-sp key**.
Remaining quota is only visible via:

1. the Token Plan management console (Organization Usage: subscription status,
   remaining Credits, usage trend, member usage) — needs account login;
2. the official CLI (qianwen usage summary --format json →
   token_plan: { totalCredits, remainingCredits, usedPct, resetDate }) —
   account-level OAuth login, not sk-sp keys.

**Consequence:** the proxy must account for usage itself. Every proxied response
carries usage (OpenAI: body.usage or the final SSE chunk when
stream_options.include_usage=true; Anthropic: message_start usage + message_delta
output_tokens). The proxy accumulates per-account and per-user consumption and
computes remaining = quota − consumed (with optional admin reconciliation).

## 3. Architecture

Single Rust binary, one listening port (default 8787) serving both:

- the LLM proxy (OpenAI-compatible /v1/*, Anthropic-compatible /apps/anthropic/*)
- the web UI + JSON API

Components (src/):

- config.rs — parse config.toml (serde): server, defaults, accounts[], credit rates.
- store.rs — SQLite (rusqlite, bundled): accounts, users, usage_events + aggregates;
  source of truth for mutable state (user keys, toggles, reconciliation, usage).
  config.toml seeds static fields; DB wins on restart for mutable ones.
- credit.rs — pure token→credit conversion (per-model, per-modality rates,
  conservative defaults for unknown models).
- usage.rs — pure parsers: OpenAI JSON body, OpenAI SSE (final-chunk usage),
  Anthropic SSE (message_start + message_delta). Returns UsageTokens
  { input, cached, output, parse_error }.
- balance.rs — pure selection: given account snapshots (remaining_pct, in_flight,
  max_concurrent, disabled, exhausted), pick the account with max remaining_pct that
  has a free slot; define the queue-then-503 rule. remaining_pct = remaining/quota,
  which makes tokens- and credits-mixable accounts comparable (the balancing goal is
  relative spread, not absolute equality).
- forward.rs — rewrite Authorization to the selected account's sk-sp key, forward
  bytes (including SSE streams) with reqwest, pass through status/body; detect
  upstream 429-quota → mark account exhausted; on SSE capture usage from the final
  chunks (usage.rs) while relaying.
- proxy.rs — axum routes for the proxy: Bearer auth (user key or admin key),
  path matching, account selection (balance.rs) + in-flight slot acquisition,
  forwarding, usage recording, OpenAI-style error JSON.
- web.rs — static UI (include_bytes!) + JSON API: /api/whoami, /api/me/usage,
  /api/admin/* (accounts, users, analytics, reconcile, account edits, user key
  management).
- state.rs — runtime: load accounts (config ∪ DB), in-flight counters (AtomicU32 +
  RAII guard), consumed-since-cycle-start running totals, exhaustion flags, cycle
  rollover.
- main.rs — CLI (serve, key new, init), tracing, startup.

Web UI (no build step, vanilla JS + CSS, embedded via include_bytes!):
- key entry → /api/whoami → role: user or admin
- user view: my usage (today/month), daily trend, top models, team account health (read-only)
- admin view: accounts table (remaining bar, in-flight, status; actions:
  enable/disable, clear exhaustion, reconcile actual remaining, edit quota/
  concurrency/balance unit), users table (create/revoke proxy keys, per-user usage),
  team analysis (daily usage per account/user/model)

## 4. Routing & balancing rules (normative)

For each incoming LLM request:
1. Auth: Bearer must equal a live user proxy key (or admin key, attributed to "admin").
   Otherwise 401 OpenAI-style error JSON.
2. Candidates = accounts with !disabled && !exhausted && in_flight < max_concurrent.
3. If candidates non-empty: pick max remaining_pct (ties: random among ties);
   acquire its in-flight slot (atomic, checked again under lock to avoid races).
4. If candidates empty but some account is merely slot-full: wait up to
   queue_timeout_secs (default 5) polling every 200 ms; then 503 + Retry-After: 5.
5. If all accounts disabled/exhausted: 503 JSON error (no Retry-After needed).
6. Forward with the account's key. On success: record usage event (tokens, model,
   latency, stream flag, status) attributed to both account and user.
7. On upstream 429 whose body mentions quota (insufficient_quota /
   AllocationQuota / quota): mark account exhausted until cycle rollover or admin
   action; do NOT record the request's usage (it consumed nothing).

Account remaining (per its balance_unit):
- tokens: monthly_quota_tokens − consumed_tokens_since(cycle_start)
- credits: monthly_quota_credits − consumed_credits_since(cycle_start), where each
  event's credits = f(model rates, tokens)
- admin reconciliation: set baseline (reconciled_at, reconciled_remaining);
  remaining = reconciled_remaining − consumed_since(reconciled_at)
- exhausted accounts report remaining 0 for selection.

Cycle rollover: cycle_start per account (default: 1st of current month, UTC+8 for
CN accounts / UTC for intl — kept as a per-account config field). A background task
every 6h rolls accounts whose window has passed (resets running consumed total;
historical events stay for analytics).

## 5. Downstream key semantics

- Users: tbu_<16 url-safe chars>, created by admin in the UI (stored in DB, revoked
  flag). Used as the tool's API key; base URL = http://<host>:<port>/v1 (OpenAI) or
  http://<host>:<port>/apps/anthropic (Anthropic).
- Admin: admin_key in config.toml (tba_...); grants UI admin API + may also proxy
  (attribution "admin").
- Wrong/unknown key → 401. Revoked user key → 401.

## 6. Config file (config.toml) shape

[server] listen = "0.0.0.0:8787", admin_key, queue_timeout_secs = 5
[defaults] region = "cn", balance_unit = "tokens", max_concurrent = 2,
            monthly_quota_credits / monthly_quota_tokens by seat tier
[[accounts]] id, label, api_key = "sk-sp-...", region = "cn"|"intl",
             base_url_openai / base_url_anthropic (optional override),
             seat_tier = "standard"|"pro"|"max", balance_unit, monthly_quota (in unit),
             cycle_start (optional date), max_concurrent, disabled
[credit_rates] per-model tokens-per-credit { input, cached, output } + default fallback
[[users]] (optional seed) key, name

## 7. Error handling & edge cases

- Upstream timeouts: reqwest connect 10s, total stream inactivity 300s; on failure
  release the slot and 502 (or the upstream error passed through on 4xx/5xx from
  upstream — pass through status + body, only 429-quota has special handling).
- Client disconnect mid-stream: stop forwarding, release slot, still record partial
  usage captured so far (best-effort; parse_error flag if usage not seen).
- Body size: forward as-is, no cap beyond stream (config max_request_mb default 10).
- CORS: permissive on /v1/* and /apps/anthropic/* (browser-based agents).
- Concurrency correctness: selection is done under a single std::sync::Mutex over
  the account table (fast, in-memory), slots are AtomicU32 with a guard that
  fetch_sub on drop.
- Key material: config.toml holds sk-sp keys — .gitignore + docs warn; DB stores
  them too (SQLite file, 0600).

## 8. Testing strategy

- Unit (cargo test): config parsing; credit conversion; usage parsers (fixtures for
  OpenAI JSON/SSE, Anthropic SSE incl. cache read/creation); balancer selection
  (remaining_pct ordering, tie-break, slot-full queue rule, exhaustion skip);
  key generation uniqueness/format.
- Integration (tests/, tokio + ephemeral axum mock upstream that counts in-flight and
  returns canned OpenAI/Anthropic payloads incl. 429-quota and SSE streams):
  routing to highest-remaining; concurrency cap enforcement (parallel requests
  blocked/queued); exhaustion on 429; usage attributed to account+user; 401 on bad
  key; admin API (whoami, reconcile, create/revoke user, analytics).
- Web UI: curl-based smoke tests for JSON API; UI manually verified.

## 9. Non-goals (v1)

- No multi-node / HA, no TLS termination (put behind Caddy/nginx), no per-user
  billing or spend caps beyond account quota, no image/video generation routing
  (text chat endpoints only; those APIs need separate async task flows),
  no SSO, no i18n (UI in Chinese with English labels where natural).
