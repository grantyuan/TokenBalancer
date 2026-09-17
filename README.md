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
- 健康检查：`http://<host>:8787/healthz`（仅存活探测，返回 `{"status":"ok"}`；帐号详情见鉴权后的 admin API / 管理页面）

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
