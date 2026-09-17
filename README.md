# TokenBalancer

An auto-balancing HTTP proxy for **Qwen Token Plan Team Edition** multi-account usage.

Point your AI tools at this proxy and use an admin-issued **proxy key** as your API key.
The proxy routes every request to the upstream account with the most remaining quota
(higher remaining/quota ratio wins), enforces a per-account concurrency cap (default 2,
configurable), and ships a built-in web admin page (members see their own usage; the
admin sees all accounts, team analytics, and quota reconciliation).

## Quick start

```bash
cargo build --release
# 1) Copy and edit the config
cp config.example.toml config.toml   # fill in the sk-sp- keys, admin_key, listen address
# 2) Start
./target/release/tokenbalancer serve config.toml
```

Once running:
- Proxy (OpenAI-compatible): `http://<host>:8787/v1`
- Proxy (Anthropic-compatible): `http://<host>:8787/apps/anthropic`
- Admin page: `http://<host>:8787/`
- Health check: `http://<host>:8787/healthz` (liveness only, returns `{"status":"ok"}`;
  account detail lives in the authenticated admin API / admin page)

## Getting Token Plan team-edition keys and base URLs

1. Purchase/manage your team subscription on the Token Plan platform:
   - CN (Qwen Cloud): https://tokenplan-enterprise.qianwenai.com
   - International (QwenCloud): https://tokenplan-enterprise.qwencloud.com
2. Member management → assign a seat → generate a dedicated API key (format
   `sk-sp-xxxxx`, shown only once).
3. Check the plan-specific base URL on the API key page (CN:
   `token-plan.cn-beijing.maas.aliyuncs.com`, Intl:
   `token-plan.ap-southeast-1.maas.aliyuncs.com`; built into the proxy by default,
   usually no change needed).

Auth scheme: `Authorization: Bearer <sk-sp-...>` (not x-api-key).

## Onboarding team members

The admin creates proxy keys on the web page under "Member Keys" (or pre-seeds them in
the config `[[users]]`). Members configure their tools with:

| Tool type | Base URL | API Key |
|---|---|---|
| OpenAI-compatible (Cursor / Qwen Code / Codex / OpenCode / Cherry Studio...) | `http://<host>:8787/v1` | proxy key |
| Anthropic-compatible (Claude Code, etc.) | `http://<host>:8787/apps/anthropic` | proxy key |

## Admin page

The UI supports Chinese and English (toggle in the header; the choice is remembered in
localStorage and defaults to your browser language).

- **Member view** (log in with a proxy key): last-14-days usage, daily trend, top
  models, team account status (read-only).
- **Admin view** (log in with the admin key):
  - Per account: remaining-amount bar, in-flight concurrency, disable/enable,
    **reconcile** (enter the real remaining amount read from the console/CLI),
    clear-exhausted, adjust concurrency/quota;
  - Team analytics: daily usage trend, aggregates by member/model;
  - Member key management: create (key shown only once), revoke.

## Reconciliation (calibrating remaining quota)

The proxy estimates remaining quota by self-accounting the traffic that passes through
it. To calibrate: read the account's real remaining credits from the Token Plan
console (Organization Usage) or the official CLI
(`qianwen usage summary --format json` → `token_plan.remainingCredits`), then click
"Reconcile..." on the admin page and enter the value. From that moment on, the
account's remaining amount = reconciled value − new consumption since reconciliation.

## Balancing strategy

- Each request picks the account with the highest remaining ratio
  (remaining/quota) that still has a free concurrency slot;
- All slots busy → queue (default 5s, `queue_timeout_secs`), then 503 + Retry-After;
- An account hit by an upstream 429 (AllocationQuota / insufficient_quota) is marked
  exhausted and skipped until reconciliation / cycle start / manual clear;
- Balance unit per account: `tokens` (default, taken directly from response usage)
  or `credits` (estimated via the per-model rate table in config `[credit_rates]`,
  calibratable through reconciliation).

## Compliance note

Token Plan terms require dedicated keys to be used with interactive AI tools and the
calls they initiate; application backends/batch workloads are not permitted. This
program is a transparent forwarding layer for team members' **interactive tool
traffic** (it generates no extra calls). Teams should evaluate compliance with their
subscription terms themselves.

## Known limitations (v1)

- The credits rate table is an approximation (official exact rates are not public);
  reconciliation calibrates it;
- Mid-stream 4xx responses are not inspected for quota exhaustion (only non-streaming
  error bodies are);
- Single-instance deployment (no multi-node/HA); usage stats aggregate over the local
  billing window (default: current month).

## Development

```bash
cargo test            # full test suite (includes end-to-end tests against a mock upstream)
cargo clippy          # static checks
```
