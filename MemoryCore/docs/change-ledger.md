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

- 路径：按 `event_ts` 的 UTC 日期、按写入进程（writer）分片，经 `StorageAdapter.appendFile` 写入，本地文件系统与 COS 后端均适用：
  - `events/YYYY-MM-DD.<writerId>.jsonl`：本 writer 追加的活动分片；
  - `events/YYYY-MM-DD[.<writerId>]~<gen>.jsonl`：擦除改写生成的封存分片（内容已脱敏，此后不再追加，再次改写时换新 `<gen>`）；
  - `events/YYYY-MM-DD.jsonl`：旧版无后缀分片（本改动之前写入），继续可回放、可改写。
- `writerId` = `<hostname>-<8 hex>`，首次启动时生成并持久化到 `<dataDir>/.metadata/ledger_writer_id`，
  同一数据目录重启后沿用（因此仍“拥有”并能改写重启前的分片）；每个数据目录只应有一个写入进程。
  未经 TdaiCore 初始化（如单测、脚本）时使用进程级随机 id。
- 回放、TTL 删除、`pruneLedgerOutbox` 都按日期前缀枚举 `events/` 下全部 `.jsonl`，与 writer 后缀无关。每行一条完整 `MemoryEvent`（含 `event_id`、`snapshot_json`），
  足以重建 `memory_events`。
- 写入顺序（`src/core/record/event-ledger.ts` 的 `appendLedgerEvent`）：
  1. 分配 `event_id`；
  2. 追加 JSONL outbox（分片锁内复查已知擦除标记，被覆盖的事件直接以骨架行落盘）；
  3. 追加到当前 store；若写入期间有新的擦除标记注册，落库后按该标记的 filter 补一次
     定向擦除（`store.redactMemoryEvents`），失败记 `pending_redactions`；
  4. 任一步失败只记 warn 与健康度计数，**不阻塞主写路径**。
- 已知擦除标记集合（进程内）：本进程接受过的所有 clear/TTL filter + 回放时在 outbox
  扫到的标记。被任一标记覆盖的追加永不携带明文——无论 append 与 redact 的相对时序如何，
  outbox 与 store 两条腿上都不会复活明文（跨进程时依赖对方节点回放后收敛）。
- 接入点：L1 writer（created / superseded / updated / merged）、管理面 `recordAudit` 镜像、
  chat_memory clear 的 L1/L2/L3 deleted、`/memory/diff/revert` 的 reverted。
- 未配置 storage 时只写 store（行为与之前一致，但无法回放）。

## 健康度与降级提示

- 按逻辑 store（StorePool 建店时绑定 `backend:instanceId`，对象被 LRU 驱逐重建后记账延续）
  × 租户（team/agent，进程内）统计 `store_failures` / `jsonl_failures` / `last_failure_at`
  （不对外暴露后端原始错误文本），
  以及 `pending_store_events`：已进 outbox、但尚未写入 store 的事件数。计数是进程级、重启清零，
  多实例部署下各实例独立。
- `degraded` 在 `pending_store_events > 0`、`pending_redactions > 0` 或出现不可恢复失败时为真。
  backfill 成功回放后对应事件出队，补齐完成即自动解除降级。store 与 outbox 同时失败
  （或待补事件超过 10000 条上限）的事件无法回放，只能重启清零或 `reset`。
  仅 outbox 失败时 store 中的数据完整，不算降级，只计入 `jsonl_failures`。
- `POST /v3/memory/ledger/status` 返回 `{ supported, jsonl_outbox, backfill_enabled, health }`，
  `health` 含 `pending_redactions` / `pending_outbox_rewrites`。`{"reset": true}` 清零失败计数
  （运维手段，需 `TDAI_LEDGER_BACKFILL_ENABLED`）；**pending 擦除是未落地的工作项，不在 reset
  范围内**——只能由 backfill 真正落地或进程重启清除。
- 出现过追加失败时，`/memory/diff`、`/memory/history` 与 `/memory/review/inbox` 响应附带
  `ledger: { degraded: true, store_failures, jsonl_failures, pending_store_events, pending_redactions, last_failure_at }`，
  MemoryPanel 审阅页据此显示“变更账降级”提示。
- SQLite / TCVDB 的 `appendMemoryEvent` 在后端降级或写入失败时抛出，由 `appendLedgerEvent` 记为 pending，
  不影响主写路径。审阅路径（diff/history/inbox/revert）的查询失败一律 fail-closed，返回 503。

## 撤销守卫（revert）

默认 fail-closed，以下情况返回 409：
- 目标写入之后同 team/agent 有 `deleted`（clear/archive，`scope=agent`）或记录已不存在（含 TTL 清理）；
- 提取写入之后有 `source=api_mutation` 的人工编辑：先按 `event_id` 撤销该人工编辑层，或 `force:true` 覆盖；
- 被恢复的旧记录还有其它存活后继（并发 session 分叉）；
- 被恢复的旧记录没有可用快照（写入缺口或已被 clear/TTL 擦除）：默认 409，`force:true` 接受只删不恢复（响应带 `missing`）；
- 该记录的 `reverted` 事件仍在 pending（未进 store）时返回 503，补齐后再判断。

管理面 update 事件带修改前的 `snapshot_json`，可按 `event_id` 逐层回退。`reviewer_id` 只取
`x-tdai-reviewer-id` 请求头（MemoryPanel 以 `panelMeta.userId` 填入），忽略 body。

## clear / TTL 擦除

`redactLedgerEvents(filter)` 覆盖 `event_ts <= until` 且 team/agent/user（设置时）匹配的事件，依次做三件事：

1. **擦除标记**：向本 writer 活动分片追加 `{"redact": filter, "marker_ts": ...}`。标记行永不修改、永不删除，
   只随所在分片按日期整体删除。
2. **outbox 行改写**：对本 writer 拥有的分片（本 writer 后缀，含其封存分片；以及旧版无后缀分片）中日期 ≤ `until` 的分片，
   将匹配事件行的 `content` 置空、删除 `snapshot_json`；`event_id`、`op`、`event_ts`、record/scope 元数据原样保留，
   与 `store.redactMemoryEvents` 的骨架语义一致。标记行、畸形行、不匹配的行逐字节保留。
   封存分片通过 `appendObject` 写到全新的 `~<gen>` key（单次 append 落完整内容），随后才删除原分片——
   `events/` 下所有对象保持 append-created 的单一访问模式，COS `APPENDABLE_KEY_PREFIXES` 前缀守卫不会拒绝；
   删除原分片严格发生在封存 append 完成之后，所以封存分片在写完前永远不是唯一副本，
   读到半途分片的回放不会丢事件（重复按 `event_id` 幂等去重，截断尾行计 malformed）。
3. **store 擦除**：`content` / `snapshot_json` 置空，保留元数据骨架。

入口先把 filter 登记进本进程的已知标记集（fail-closed：即使三步全失败也生效）——
本进程内被覆盖的追加写骨架（分片锁内复查保证时序无关），store 落库后按标记补定向擦除。
并发的两次擦除由全局改写锁串行，后到者对"已被封存的分片"按 (日期, writer) 族键重新列举，
不会对着已消失的文件名报成功。

- chat_memory clear 的 filter 为 `{ team_id, agent_id, until }`，并写 `scope=agent` 的 deleted 事件。
- TTL 清理（L1 实际执行时）写一条 `source=retention`、`scope=retention`、`until=cutoff`、`record_id=retention-l1-<cutoff>`
  的批次 deleted 事件，然后走同一路径，filter 为不带租户的 `{ until: cutoff }`，覆盖所有租户的匹配行；
  过期的 outbox 分片按日期整体删除。批次事件不含逐条 record_id（`deleteL1Expired` 只返回数量），revert 依靠目标行不存在来拒绝。

### 明文何时真正消失

store 擦除成功，且所有含匹配行的分片都已改写之后：

- 本 writer 的分片与旧版无后缀分片：`redactLedgerEvents` 返回时（改写成功的前提下）即已消失。
- 其它 writer（其它节点 / 数据目录）的分片：本进程**不会**改写——改写是读-改-写，不能与对方的追加竞争。
  对方下一次 backfill 读到标记后，会改写自己分片中被覆盖的行；在此之前明文仍留在对方分片里，最迟随分片 TTL 删除。
  writer 已永久下线（如 pod 重建且数据目录未持久化）时，其分片没有 owner，只能靠 TTL 删除。
- 旧版无后缀分片视为任何进程都可改写的共享文件，属尽力而为：若仍有旧版进程向其追加，改写可能丢失并发追加的行。
- 同一进程内，同一分片的追加与改写经 per-shard 队列串行，改写期间到达的追加不会丢失。

### 失败处理

- 三步各自失败都不影响 clear/TTL 本身（不抛错）。失败部分记入 `pending_redactions`，
  其中标记追加或分片改写未完成的另计 `pending_outbox_rewrites`；`degraded` 保持为真，直到 store 擦除与 outbox 改写都已完成。
- backfill 在回放事件**之前**重试未完成的标记追加与分片改写。
- 回放时擦除标记（以及仍 pending 的 filter）总是先于事件应用，因此即使改写始终失败，被擦除内容也不会被回放进 store。

## 回放 / 补齐（runbook）

适用：store 故障恢复后、节点重建、后端迁移（如 SQLite → TCVDB）。

```bash
curl -X POST "$GATEWAY/v3/memory/ledger/backfill" \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -H "x-tdai-team-id: $TEAM" -H "x-tdai-agent-id: $AGENT" -H "x-tdai-user-id: $USER" \
  -d '{"since":"2026-09-01T00:00:00.000Z"}'
# => { files, scanned, replayed, skipped, malformed, failed, redacted, redactions_applied, outbox_redacted, outbox_failed }
```

- 运维操作：需部署设置 `TDAI_LEDGER_BACKFILL_ENABLED=1`（否则 403）；`since` 必填（否则 400）。
  MemoryPanel 降级横幅提供“从 outbox 补齐”按钮（经面板代理 `/memory/ledger/backfill`）。
- 回放范围限定在请求 isolation 的 team/agent，按文件日期与 `event_ts` 过滤。
- 依赖 `event_id` 幂等，可安全重复执行；非 JSON、非对象（如 `null`、数组）或缺 `event_id` 的畸形行计入 `malformed` 并跳过，不影响后续行与分片。
- store 拒绝写入（含降级状态）时回放计入 `failed`，事件保持 pending，直到真正写入成功。
- 未知 `op`、非字符串 `event_id`/`record_id`、无法解析的 `event_ts` 同样计入 `malformed`（而非 `failed`），`failed` 只反映 store 不可用。
- 回放先重试 pending 的标记追加 / 分片改写，再用扫描到的全部标记（含其它 writer 写的）改写本 writer 拥有的分片
  （计入 `outbox_redacted`；失败计入 `outbox_failed` 并保持 pending）。`redacted` 统计被标记覆盖、以骨架形式回放的事件数。
- 回放会先把扫描到的 clear/TTL 擦除标记重新应用到 store（收窄到请求的 team/agent，幂等，计入 `redactions_applied`）。
  store 擦除失败时健康度记 `pending_redactions` 并置 `degraded`，直到覆盖该标记所在分片的 backfill 成功；
  TTL 标记不带租户：按租户 backfill 只擦该租户，并只清除该租户视角下的 `pending_redactions`。
- TCVDB 在 init 失败后保持降级直到进程重启，期间 backfill 只会计入 `failed`，不会误清 pending。
- `failed > 0` 说明 store 仍不可用，恢复后重跑即可。
- `replayed` 统计的是向 store 发起追加的次数，已存在的 `event_id` 在 store 侧为 no-op，
  因此重复执行时 `replayed` 不会变成 0。
- 单机 standalone 模式下 outbox 位于根存储（`<baseDir>/events/`），多个 service id 共用一份；
  回放按 team/agent 过滤。service 模式下 outbox 位于各实例自己的存储中。

## 顺序与时钟假设

- 事件按 `event_ts` 排序（SQLite 为 `ORDER BY event_ts, seq`），同一时间戳内按各后端的插入序（SQLite rowid、
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

## 进程内状态与已知限制

- 健康度与 `pending_redactions` 是进程内状态：重启后清零，未完成的改写不再自动重试，
  直到下一次 backfill 用 outbox 中的标记重新扫一遍本 writer 分片；期间标记保证不会回放进 store。
- 多实例各自统计，状态接口只反映收到请求的那个实例。
- 每次 clear 会读取本 writer 所有日期 ≤ `until` 的分片，开销与 outbox 大小成正比。
- 回放一次性把 `since` 之后的分片读入内存，超大 outbox 需按 `since` 分段执行。
- 回放只使用扫描范围内（`since` 之后分片里）的标记；早于 `since` 的标记不参与回放，其覆盖的事件在原 store 中已擦除，
  但若用于重建新 store，需让 `since` 覆盖相应标记所在分片。
- 封存分片与原分片短暂并存时（崩溃于 rename 与删除之间）可能留下含明文的原分片；它仍属本 writer，下次 backfill 会改写。
- 擦除改写只扫日期 ≤ `until` 的分片：极端情况下一条迟到的带旧 `event_ts` 的事件若落进更晚日期的
  分片，那行明文不会被本次改写扫到（标记仍保证其不会进 store）。
- `resetLedgerHealth` / `status {"reset":true}` 只清失败计数，不清 pending 擦除——后者是未完成的
  工作，丢弃它会让失败的 store 擦除永远不重试而账本却报健康。
- 已知标记集是进程内状态且上限 10000 条（FIFO 淘汰）；另一进程的 in-flight 明文追加收敛于
  该进程的下次 backfill，与本节“其它 writer 分片”语义一致。

## 保留期

memory-cleaner 按保留期删除过期 `events/*.jsonl`：分片按 **UTC** 日期命名，清理也按 UTC 日界
（`pruneLedgerOutbox`，经 StorageAdapter 与分片锁，rowfs/COS 部署下走同一 adapter）。
删除后对应时间段的事件无法再回放，但不影响 store 中已存在的事件。
