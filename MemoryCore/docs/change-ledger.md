# 统一变更账（memory_events）持久性与恢复

`memory_events` 是记忆变更的统一账本：extraction（L1 抽取）、api_mutation（管理面增删改 / clear）、
review（revert）三类事件都写入这里，供 `/memory/diff`、`/memory/history`、`/memory/review/inbox`
和 revert 使用。`memory_audit` 仍是独立的 API 访问日志，不受影响。

## 事件身份 `event_id`

- 每条事件在逻辑写入点生成一次 `event_id`：`evt-` + 32 hex（共 36 字符）。
  见 `src/core/store/memory-event-id.ts`。
- 同一个 `event_id` 贯穿 JSONL outbox 与所有 store，重复写入是幂等的：
  - SQLite：`memory_events.event_id` 上的 partial unique index（`event_id != ''`），
    `INSERT ... ON CONFLICT DO NOTHING`。
  - MongoDB：`event_id` 上的 unique partial index，重复键（11000）视为已写入。
  - TCVDB：文档主键 `id = event_id`，upsert 同 id 即覆盖同一事件。
    不再拼 `record_id` 等业务字段，因此主键长度恒为 36，远低于 TCVDB 128 字符上限，
    也不会因同毫秒同记录的不同事件而互相覆盖。
- 历史数据的 `event_id` 为空（SQLite `''`、Mongo/TCVDB 缺字段），查询返回 `undefined`；
  调用方未传 `event_id` 时由 store 自动补生成。

## JSONL outbox

- 路径：`events/YYYY-MM-DD.jsonl`（按 `event_ts` 的 UTC 日期分片），经 `StorageAdapter.appendFile`
  写入，本地文件系统与 COS 后端均适用。每行一条完整 `MemoryEvent`（含 `event_id`、`snapshot_json`），
  足以重建 `memory_events`。
- 写入顺序（`src/core/record/event-ledger.ts` 的 `appendLedgerEvent`）：
  1. 分配 `event_id`；
  2. 追加 JSONL outbox；
  3. 追加到当前 store；
  4. 任一步失败只记 warn 与健康度计数，**不阻塞主写路径**。
- 接入点：L1 writer（created / superseded / updated / merged）、管理面 `recordAudit` 镜像、
  chat_memory clear 的 L1/L2/L3 deleted、`/memory/diff/revert` 的 reverted。
- 未配置 storage 时只写 store（行为与之前一致，但无法回放）。

## 健康度与降级提示

- 按 store 对象（进程内）统计 `store_failures` / `jsonl_failures` / `last_failure_at` / `last_error`，
  以及 `pending_store_events`：已进 outbox、但尚未写入 store 的事件数。计数是进程级、重启清零，
  多实例部署下各实例独立。
- `degraded` 只在 `pending_store_events > 0` 时为真。backfill 成功回放后对应事件出队，
  补齐完成即自动解除降级。store 与 outbox 同时失败（或待补事件超过 10000 条上限）的事件
  无法回放，只能重启清零。仅 outbox 失败时 store 中的数据完整，不算降级，只计入 `jsonl_failures`。
- `POST /v3/memory/ledger/status` 返回 `{ supported, jsonl_outbox, health }`。
- 出现过追加失败时，`/memory/diff` 与 `/memory/review/inbox` 响应附带
  `ledger: { degraded: true, store_failures, jsonl_failures, pending_store_events, last_failure_at }`，
  MemoryPanel 审阅页据此显示“变更账降级”提示。
- TCVDB 的 `appendMemoryEvent` 在 upsert 失败时会抛出（此前只打 warn），以便计入健康度；
  既有调用方都经 `appendLedgerEvent` 或 try/catch，不影响主写路径。

## 回放 / 补齐（runbook）

适用：store 故障恢复后、节点重建、后端迁移（如 SQLite → TCVDB）。

```bash
curl -X POST "$GATEWAY/v3/memory/ledger/backfill" \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -H "x-tdai-team-id: $TEAM" -H "x-tdai-agent-id: $AGENT" -H "x-tdai-user-id: $USER" \
  -d '{"since":"2026-09-01T00:00:00.000Z"}'
# => { files, scanned, replayed, skipped, malformed, failed }
```

- 回放范围限定在请求 isolation 的 team/agent；`since` 可选，按文件日期与 `event_ts` 过滤。
- 依赖 `event_id` 幂等，可安全重复执行；无 `event_id` 的畸形行计入 `malformed` 并跳过。
- `failed > 0` 说明 store 仍不可用，恢复后重跑即可。
- `replayed` 统计的是向 store 发起追加的次数，已存在的 `event_id` 在 store 侧为 no-op，
  因此重复执行时 `replayed` 不会变成 0。
- 单机 standalone 模式下 outbox 位于根存储（`<baseDir>/events/`），多个 service id 共用一份；
  回放按 team/agent 过滤。service 模式下 outbox 位于各实例自己的存储中。

## 顺序与时钟假设

- 同一 session 的事件按 `event_ts` 排序，同一时间戳内按各后端的插入序（SQLite rowid、
  Mongo ObjectId）作为稳定次序；TCVDB 同毫秒事件无插入序保证。
- `event_ts` 取写入实例的本机时钟；多实例部署需 NTP 同步，时钟漂移会影响跨实例事件的相对顺序
  与 `since` 过滤，但不会导致事件丢失或重复（身份由 `event_id` 决定）。
- 回放写入的事件保留原 `event_ts`，diff/history 中的顺序与原始写入一致。

## conversation/add 幂等

- 通过 `Idempotency-Key` 请求头或 body `idempotency_key`（body 优先，≤256 字符）传入。
- 作用域：`(service, team, user, agent, session, key)`。
- 同进程内 24h 内的重试直接返回首次的 `accepted_ids`，跳过 quota 检查、L0 写入、pipeline 通知、
  JSONL 镜像与 quota 上报。
- 缓存未命中（跨实例 / 进程重启）时 L0 id 由作用域与消息序号确定性派生，并走 upsert 路径，
  L0 不会重复；但 pipeline 通知与 quota 上报可能再发生一次。需要跨实例严格幂等时，
  应在网关前置层按 key 做粘性路由或外部去重。

## 保留期

outbox 分片目前不自动清理。可按日期删除早于保留期的 `events/*.jsonl`；删除后对应时间段的事件
将无法再回放，但不影响 store 中已存在的事件。
