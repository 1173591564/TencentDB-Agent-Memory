import { createHash } from "node:crypto";
import type { IMemoryStore, L1RecordRow, MemoryEvent, MemoryEventFilter, MaybePromise } from "./types.js";
import type { IsolationFilter } from "./isolation.js";
import { newMemoryEventId, healIsoId, canonIsoTs } from "./memory-event-id.js";
import { normalizeReviewStatus, type ReviewStatus } from "./visibility.js";

const MAX_NODES = 50_000;
const MAX_EVENTS = 500_000;
const PAGE = 1000;
const BATCH = 50;
const bytes = new WeakMap<MemoryEvent[], number>();
function appendPage(target: MemoryEvent[], page: MemoryEvent[]): void {
  const size = (bytes.get(target) ?? 0) + page.reduce((n, e) => n + JSON.stringify(e).length * 2, 0);
  if (size > 64 * 1024 * 1024 || target.length + page.length > MAX_EVENTS) throw new Error("Review history memory budget exceeded");
  bytes.set(target, size);
  target.push(...page);
}
type Operation = { operation_id?: string; request_id?: string; reviewer_id?: string; reason?: string; persist_no_op?: boolean };
type Node = { id: string; scope: { team_id: string; user_id: string; agent_id: string }; row?: L1RecordRow; events: MemoryEvent[]; loaded: boolean };

export function validReview(value: unknown): value is NonNullable<MemoryEvent["review"]> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const v = value as Record<string, unknown>;
  return v.protocol === 1 && Object.keys(v).every((k) => ["protocol", "observed", "sources", "operation_id", "request_hash", "no_op", "content_hash", "fence_hash", "guard_at"].includes(k)) &&
    (v.guard_at === undefined || (typeof v.guard_at === "string" && canonIsoTs(v.guard_at) === v.guard_at)) &&
    (v.fence_hash === undefined || (typeof v.fence_hash === "string" && /^[a-f0-9]{64}$/.test(v.fence_hash))) &&
    (v.content_hash === undefined || (typeof v.content_hash === "string" && /^[a-f0-9]{64}$/.test(v.content_hash))) &&
    (v.no_op === undefined || typeof v.no_op === "boolean") &&
    (v.operation_id === undefined || (typeof v.operation_id === "string" && /^rop-[a-f0-9]{64}$/.test(v.operation_id))) &&
    (v.request_hash === undefined || (typeof v.request_hash === "string" && /^[a-f0-9]{64}$/.test(v.request_hash))) &&
    [v.observed, v.sources].every((a) => a === undefined || (Array.isArray(a) && a.length <= MAX_NODES && a.every((s) => typeof s === "string" && s.length > 0 && s.length <= 1024)));
}

export function decodeReview(json: unknown): MemoryEvent["review"] {
  if (json === undefined || json === null || json === "") return undefined;
  let value: unknown;
  try { value = typeof json === "string" ? JSON.parse(json) as unknown : json; }
  catch { throw new Error("Invalid review protocol payload"); }
  if (!validReview(value)) throw new Error("Invalid review protocol payload");
  return value;
}

const scopeOf = (r: { team_id?: string; user_id?: string; agent_id?: string }) => ({
  team_id: healIsoId(r.team_id ?? "")!, user_id: healIsoId(r.user_id ?? "")!, agent_id: healIsoId(r.agent_id ?? "")!,
});
const key = (scope: Node["scope"], id: string) => JSON.stringify([scope.team_id, scope.user_id, scope.agent_id, id]);
const tokenOf = (e: MemoryEvent) => e.review?.operation_id ?? e.event_id ?? `legacy-event-${createHash("sha256").update(JSON.stringify([e.record_id, e.event_ts, e.op, e.session_key, e.session_id])).digest("hex")}`;

class ReviewGraph {
  readonly nodes = new Map<string, Node>();
  private eventCount = 0;
  private eventBytes = 0;
  readonly clears = new Map<string, MemoryEvent | undefined>();

  constructor(private readonly roots: L1RecordRow[]) {
    for (const row of roots) this.add(scopeOf(row), row.record_id).row = row;
  }

  private add(scope: Node["scope"], id: string): Node {
    const k = key(scope, id);
    let n = this.nodes.get(k);
    if (!n) {
      if (this.nodes.size >= MAX_NODES) throw new Error("Review lineage node budget exceeded");
      n = { id, scope, events: [], loaded: false };
      this.nodes.set(k, n);
    }
    return n;
  }

  next(): Node[] {
    const first = [...this.nodes.values()].find((n) => !n.loaded);
    if (!first) return [];
    return [...this.nodes.values()].filter((n) => !n.loaded && key(n.scope, "") === key(first.scope, "")).slice(0, BATCH);
  }

  accept(nodes: Node[], rows: L1RecordRow[], events: MemoryEvent[]): void {
    this.eventCount += events.length;
    this.eventBytes += bytes.get(events) ?? events.reduce((n, e) => n + JSON.stringify(e).length * 2, 0);
    if (this.eventBytes > 64 * 1024 * 1024) throw new Error("Review graph memory budget exceeded");
    if (this.eventCount > MAX_EVENTS) throw new Error("Review history event budget exceeded");
    for (const n of nodes) {
      n.loaded = true;
      n.row = rows.find((r) => r.record_id === n.id && key(scopeOf(r), "") === key(n.scope, "")) ?? n.row;
      n.events = events.filter((e) => e.record_id === n.id && key(scopeOf(e), "") === key(n.scope, ""));
      for (const source of this.sources(n)) if (source !== n.id) this.add(n.scope, source);
    }
  }

  private sources(n: Node): string[] {
    const result = new Set<string>();
    if (n.row?.review_sources_json) {
      let parsed: unknown;
      try { parsed = JSON.parse(n.row.review_sources_json) as unknown; }
      catch { throw new Error("Invalid review lineage"); }
      if (!Array.isArray(parsed) || parsed.length > MAX_NODES || !parsed.every((s) => typeof s === "string" && s.length > 0 && s.length <= 1024)) throw new Error("Invalid review lineage");
      for (const s of parsed) if (s && s !== n.id) result.add(s);
    }
    for (const e of n.events) {
      if ((e.layer ?? "l1") === "l1" && ["created", "updated", "merged"].includes(e.op) && (e.source === undefined || e.source === "extraction")) {
        for (const s of e.review?.sources ?? e.supersedes ?? []) if (s !== n.id) result.add(s);
      }
    }
    return [...result];
  }

  result(): L1RecordRow[] {
    const own = new Map<string, Set<string>>();
    const removed = new Map<string, Set<string>>();
    const parents = new Map<string, string[]>();
    const values = new Map<string, Set<string>>();
    const incomplete = new Map<string, boolean>();
    for (const [k, n] of this.nodes) {
      const tokens = new Set<string>();
      const cancelled = new Set<string>();
      const clear = this.clears.get(JSON.stringify([n.scope.team_id, n.scope.agent_id]));
      const guard = n.row?.review_guard_at || n.row?.created_time || n.events.filter((e) => ["created", "updated", "merged"].includes(e.op)).map((e) => e.review?.guard_at ?? e.event_ts).sort()[0];
      if (guard && canonIsoTs(guard) !== guard) throw new Error("Invalid generation guard");
      if (clear && (!guard || guard <= clear.event_ts)) tokens.add(`clear:${tokenOf(clear)}`);
      const hashes = new Map<string, Set<string>>();
      for (const e of n.events) if ((e.layer ?? "l1") === "l1" && e.review?.operation_id) {
        const group = hashes.get(e.review.operation_id) ?? new Set<string>();
        group.add(e.review.request_hash ?? "");
        hashes.set(e.review.operation_id, group);
      }
      const observations = new Map<string, Set<string>>();
      for (const e of n.events) if ((e.layer ?? "l1") === "l1" && e.op === "restored" && e.review?.operation_id && e.review.protocol === 1) {
        const observed = new Set(e.review.no_op ? [] : e.review.observed ?? []);
        const prior = observations.get(e.review.operation_id);
        observations.set(e.review.operation_id, prior ? new Set([...prior].filter((t) => observed.has(t))) : observed);
      }
      const conflicts = new Set([...hashes].filter(([, h]) => h.size > 1).map(([id]) => id));
      for (const id of conflicts) tokens.add(`conflict:${createHash("sha256").update(JSON.stringify([id, [...hashes.get(id)!].sort()])).digest("hex")}`);
      const legacy = `legacy-status:${createHash("sha256").update(k).digest("hex")}`;
      if ((n.row?.review_tokens === undefined && normalizeReviewStatus(n.row?.review_status) === "quarantined") || (!n.row && !n.events.length)) tokens.add(legacy);
      for (const e of n.events) {
        if ((e.layer ?? "l1") !== "l1") continue;
        if (e.review !== undefined && !validReview(e.review)) throw new Error("Invalid review event");
        if (e.review?.no_op) continue;
        if (e.op === "retracted") tokens.add(tokenOf(e));
        if (e.op === "restored") {
          if (e.review?.operation_id && conflicts.has(e.review.operation_id)) continue;
          if (e.review?.protocol === 1) {
            if (!e.review.observed) throw new Error("Restore missing observed retractions");
            for (const t of e.review.operation_id ? observations.get(e.review.operation_id)! : e.review.observed) cancelled.add(t);
          } else {
            for (const t of tokens) cancelled.add(t);
            cancelled.add(legacy);
          }
        }
      }
      own.set(k, tokens);
      removed.set(k, cancelled);
      parents.set(k, this.sources(n).map((s) => key(n.scope, s)));
      values.set(k, new Set());
      incomplete.set(k, !n.row && !n.events.length);
    }
    const children = new Map<string, Set<string>>();
    for (const [child, sources] of parents) for (const source of sources) {
      if (!children.has(source)) children.set(source, new Set());
      children.get(source)!.add(child);
    }
    const queue = [...this.nodes.keys()];
    const queued = new Set(queue);
    let propagated = 0;
    for (let index = 0; index < queue.length; index++) {
      if (index > 2_000_000) throw new Error("Review lineage propagation budget exceeded");
      const k = queue[index]!;
      queued.delete(k);
      const tokens = new Set(own.get(k));
      for (const p of parents.get(k)!) for (const t of values.get(p) ?? []) tokens.add(t);
      for (const t of removed.get(k)!) if (!t.startsWith("clear:")) tokens.delete(t);
      const before = values.get(k)!;
      const missing = incomplete.get(k)! || parents.get(k)!.some((p) => incomplete.get(p));
      const missingChanged = missing !== incomplete.get(k);
      incomplete.set(k, missing);
      if (!missingChanged && tokens.size === before.size && [...tokens].every((t) => before.has(t))) continue;
      propagated += tokens.size - before.size;
      if (propagated > 1_000_000) throw new Error("Review lineage token budget exceeded");
      values.set(k, tokens);
      for (const child of children.get(k) ?? []) if (!queued.has(child)) { queue.push(child); queued.add(child); }
    }
    return this.roots.map((row) => {
      const tokens = [...values.get(key(scopeOf(row), row.record_id))!].sort();
      const invalid = row.review_status !== undefined && !["", "active", "quarantined"].includes(row.review_status);
      return { ...row, review_status: tokens.length ? "quarantined" : "active", review_tokens: tokens, ...(incomplete.get(key(scopeOf(row), row.record_id)) ? { review_incomplete: true } : {}), ...(invalid ? { review_invalid: true } : {}) };
    });
  }
}

const clearKey = (scope: { team_id?: string; agent_id?: string }) => JSON.stringify([healIsoId(scope.team_id ?? ""), healIsoId(scope.agent_id ?? "")]);
const clearFilter = (scope: { team_id?: string; agent_id?: string }): MemoryEventFilter => ({ ...scope, op: "deleted", source: "api_mutation", scope: "agent", layer: "l1", order: "desc", limit: 1, metadata_only: true });

export function assertClearGuardSync(store: IMemoryStore, record: { teamId?: string; agentId?: string; review_guard_at?: string }): void {
  if (!record.review_guard_at) return;
  if (canonIsoTs(record.review_guard_at) !== record.review_guard_at || !store.queryMemoryEvents) throw new Error("Unverifiable generation guard");
  const marker = sync(store.queryMemoryEvents(clearFilter({ team_id: record.teamId || "default", agent_id: record.agentId || "default" })))[0];
  if (marker && marker.event_ts >= record.review_guard_at) throw new ReviewConflictError("Generation invalidated by agent clear");
}

export async function assertClearGuard(store: IMemoryStore, record: { teamId?: string; agentId?: string; review_guard_at?: string }): Promise<void> {
  if (!record.review_guard_at) return;
  if (canonIsoTs(record.review_guard_at) !== record.review_guard_at || !store.queryMemoryEvents) throw new Error("Unverifiable generation guard");
  const marker = (await store.queryMemoryEvents(clearFilter({ team_id: record.teamId || "default", agent_id: record.agentId || "default" })))[0];
  if (marker && marker.event_ts >= record.review_guard_at) throw new ReviewConflictError("Generation invalidated by agent clear");
}

function filters(nodes: Node[]): { rows: Parameters<IMemoryStore["queryL1Records"]>[0]; events: MemoryEventFilter } {
  const scope = nodes[0]!.scope;
  const ids = nodes.map((n) => n.id);
  return {
    rows: { recordIds: ids, teamId: scope.team_id, userId: scope.user_id, agentId: scope.agent_id, visibility: "all" },
    events: { ...scope, record_ids: ids, limit: PAGE, metadata_only: true },
  };
}

const sync = <T>(value: MaybePromise<T>): T => {
  if (value instanceof Promise) throw new Error("Async store used in synchronous review resolver");
  return value;
};

export function resolveReviewRowsSync(store: IMemoryStore, rows: L1RecordRow[]): L1RecordRow[] {
  if (!rows.length) return rows;
  if (!store.queryMemoryEvents) throw new Error("Review ledger unavailable");
  const graph = new ReviewGraph(rows);
  for (let nodes = graph.next(); nodes.length; nodes = graph.next()) {
    const f = filters(nodes);
    const raw = sync(store.queryL1Records(f.rows, { strict: true, review: false, metadataOnly: true }));
    const events: MemoryEvent[] = [];
    for (let offset = 0; ; offset += PAGE) {
      if (offset >= MAX_EVENTS) throw new Error("Review history scan budget exceeded");
      const page = sync(store.queryMemoryEvents({ ...f.events, offset }));
      appendPage(events, page);
      if (page.length < PAGE) break;
    }
    const ck = clearKey(nodes[0]!.scope);
    if (!graph.clears.has(ck)) graph.clears.set(ck, sync(store.queryMemoryEvents(clearFilter({ team_id: nodes[0]!.scope.team_id, agent_id: nodes[0]!.scope.agent_id })))[0]);
    graph.accept(nodes, raw, events);
  }
  return graph.result();
}

export async function resolveReviewRows(store: IMemoryStore, rows: L1RecordRow[]): Promise<L1RecordRow[]> {
  if (!rows.length) return rows;
  if (!store.queryMemoryEvents) throw new Error("Review ledger unavailable");
  const graph = new ReviewGraph(rows);
  for (let nodes = graph.next(); nodes.length; nodes = graph.next()) {
    const f = filters(nodes);
    const raw = await store.queryL1Records(f.rows, { strict: true, review: false, metadataOnly: true });
    const events: MemoryEvent[] = [];
    for (let offset = 0; ; offset += PAGE) {
      if (offset >= MAX_EVENTS) throw new Error("Review history scan budget exceeded");
      const page = await store.queryMemoryEvents({ ...f.events, offset });
      appendPage(events, page);
      if (page.length < PAGE) break;
    }
    const ck = clearKey(nodes[0]!.scope);
    if (!graph.clears.has(ck)) graph.clears.set(ck, (await store.queryMemoryEvents(clearFilter({ team_id: nodes[0]!.scope.team_id, agent_id: nodes[0]!.scope.agent_id })))[0]);
    graph.accept(nodes, raw, events);
  }
  return graph.result();
}

function change(row: L1RecordRow, status: ReviewStatus, operation: Operation): { changed: boolean; previous: ReviewStatus; event?: MemoryEvent } {
  const previous = normalizeReviewStatus(row.review_status);
  if (status === "active" && row.review_tokens?.some((t) => t.startsWith("clear:"))) throw new ReviewConflictError("Generation invalidated by clear cannot be restored");
  if (previous === status && !operation.persist_no_op) return { changed: false, previous };
  const event: MemoryEvent = {
    event_id: newMemoryEventId(), event_ts: new Date().toISOString(),
    ...scopeOf(row), task_id: row.task_id || undefined, session_id: row.session_id, session_key: row.session_key,
    record_id: row.record_id, content: "", memory_type: row.type, version: row.version, source: "review", layer: "l1",
    op: status === "active" ? "restored" : "retracted", reason: operation.reason,
    reviewer_id: operation.reviewer_id, request_id: operation.request_id,
    review: { protocol: 1, request_hash: requestHash(row.record_id, status, operation),
      ...(operation.operation_id ? { operation_id: operation.operation_id } : {}),
      ...(previous === status ? { no_op: true } : {}),
      ...(status === "active" ? { observed: row.review_tokens ?? [] } : {}) },
  };
  if (!validReview(event.review)) throw new Error("Review operation exceeds protocol payload budget");
  return { changed: previous !== status, previous, event };
}

export class ReviewConflictError extends Error {}

const requestHash = (id: string, status: ReviewStatus, op: Operation) => createHash("sha256")
  .update(JSON.stringify([id, status, op.reason ?? "", op.reviewer_id ?? ""])).digest("hex");

function previousResult(e: MemoryEvent, id: string, status: ReviewStatus, op: Operation) {
  if (e.review?.request_hash !== requestHash(id, status, op)) throw new ReviewConflictError("Review operation identity reused with different input");
  return { changed: !e.review.no_op, previous: (e.review.no_op ? status : status === "active" ? "quarantined" : "active") as ReviewStatus, event: e };
}

export const isDerivedReviewPath = (path: string): boolean => path === "persona.md" || path === ".metadata/scene_index.json" || (path.startsWith("scene_blocks/") && path.endsWith(".md"));

export async function profileReviewFence(store: IMemoryStore, isolation?: { teamId?: string; userId?: string; agentId?: string }): Promise<string[]> {
  if (!store.queryMemoryEvents) throw new Error("Review ledger unavailable");
  const scope: { team_id?: string; agent_id?: string; user_id?: string } = isolation ? { team_id: isolation.teamId || "default", agent_id: isolation.agentId || "default", ...(isolation.teamId ? {} : { user_id: isolation.userId || "default" }) } : {};
  const events: MemoryEvent[] = [];
  for (let offset = 0; ; offset += PAGE) {
    if (offset >= MAX_EVENTS) throw new Error("Profile review event budget exceeded");
    const page = await store.queryMemoryEvents({ ...scope, layer: "l1", source: "review", limit: PAGE, offset, metadata_only: true });
    appendPage(events, page);
    if (page.length < PAGE) break;
  }
  const current = await store.queryL1Records({ teamId: scope.team_id, userId: scope.user_id, agentId: scope.agent_id, visibility: "quarantined" }, { strict: true, review: false, metadataOnly: true });
  const roots = new Map(historicalReviewRows(events).map((r) => [key(scopeOf(r), r.record_id), r]));
  for (const r of current) roots.set(key(scopeOf(r), r.record_id), { ...r, review_sources_json: "[]" });
  const graph = new ReviewGraph([...roots.values()]);
  graph.accept([...graph.nodes.values()], current.map((r) => ({ ...r, review_sources_json: "[]" })), events);
  const tokens = new Set(graph.result().flatMap((r) => r.review_tokens ?? []));
  const clear = (await store.queryMemoryEvents(clearFilter({ team_id: scope.team_id, agent_id: scope.agent_id })))[0];
  if (clear) tokens.add(`clear:${tokenOf(clear)}`);
  return [...tokens].sort();
}

export async function derivedProfileAllowed(store: IMemoryStore, path: string, content: string, fence: readonly string[], isolation?: { teamId?: string; userId?: string; agentId?: string }): Promise<boolean> {
  if (!fence.length) return true;
  if (!store.queryMemoryEvents) throw new Error("Review ledger unavailable");
  const hash = createHash("sha256").update(content).digest("hex");
  const events: MemoryEvent[] = [];
  for (let offset = 0; offset < MAX_EVENTS; offset += PAGE) {
    const page = await store.queryMemoryEvents({ record_id: path, layer: path === "persona.md" ? "l3" : "l2", source: "review", op: "updated", team_id: isolation?.teamId || "default", agent_id: isolation?.agentId || "default", limit: PAGE, offset });
    appendPage(events, page);
    if (page.length < PAGE) {
      const hashes = new Map<string, Set<string>>();
      for (const e of events) if (e.review?.operation_id) {
        const values = hashes.get(e.review.operation_id) ?? new Set<string>();
        values.add(e.review.request_hash ?? "");
        hashes.set(e.review.operation_id, values);
      }
      return events.some((e) => e.review?.content_hash === hash && (!e.review.operation_id || hashes.get(e.review.operation_id)!.size === 1) && e.review?.fence_hash === createHash("sha256").update(JSON.stringify([...fence].sort())).digest("hex"));
    }
  }
  throw new Error("Derived acknowledgement history budget exceeded");
}

function historyFilter(id: string, filter?: IsolationFilter): MemoryEventFilter {
  return { record_id: id, layer: "l1", team_id: filter?.teamId, user_id: filter?.userId, agent_id: filter?.agentId, limit: PAGE };
}

export function historicalReviewRows(events: MemoryEvent[]): L1RecordRow[] {
  const records = new Map<string, L1RecordRow>();
  for (const e of events) {
    if ((e.layer ?? "l1") !== "l1" || !["created", "updated", "merged", "superseded", "retracted", "restored"].includes(e.op)) continue;
    const scope = scopeOf(e);
    records.set(key(scope, e.record_id), {
      record_id: e.record_id, content: "", type: e.memory_type ?? "work_fact", priority: 0, scene_name: "",
      ...scope, task_id: e.task_id ?? "", session_key: e.origin_session_key ?? e.session_key,
      session_id: e.origin_session_id ?? e.session_id, version: e.version ?? 0, timestamp_str: "", timestamp_start: "", timestamp_end: "",
      created_time: "", updated_time: e.event_ts, metadata_json: "{}", review_status: "active",
    });
  }
  return [...records.values()];
}

async function historicalReviewRow(store: IMemoryStore, id: string, filter?: IsolationFilter): Promise<L1RecordRow | undefined> {
  if (!store.queryMemoryEvents) throw new Error("Review ledger unavailable");
  const events: MemoryEvent[] = [];
  for (let offset = 0; offset < MAX_EVENTS; offset += PAGE) {
    const page = await store.queryMemoryEvents({ ...historyFilter(id, filter), offset });
    appendPage(events, page);
    if (page.length < PAGE) {
      const row = historicalReviewRows(events)[0];
      return row && (filter?.taskId === undefined || row.task_id === filter.taskId) ? (await resolveReviewRows(store, [row]))[0] : undefined;
    }
  }
  throw new Error("Review history scan budget exceeded");
}

function historicalReviewRowSync(store: IMemoryStore, id: string, filter?: IsolationFilter): L1RecordRow | undefined {
  if (!store.queryMemoryEvents) throw new Error("Review ledger unavailable");
  const events: MemoryEvent[] = [];
  for (let offset = 0; offset < MAX_EVENTS; offset += PAGE) {
    const page = sync(store.queryMemoryEvents({ ...historyFilter(id, filter), offset }));
    appendPage(events, page);
    if (page.length < PAGE) {
      const row = historicalReviewRows(events)[0];
      return row && (filter?.taskId === undefined || row.task_id === filter.taskId) ? resolveReviewRowsSync(store, [row])[0] : undefined;
    }
  }
  throw new Error("Review history scan budget exceeded");
}

export function setReviewStatusSync(store: IMemoryStore, id: string, status: ReviewStatus, filter?: IsolationFilter, operation: Operation = {}) {
  if (operation.operation_id && store.queryMemoryEvents) {
    for (let offset = 0; offset < MAX_EVENTS; offset += PAGE) {
      const page = sync(store.queryMemoryEvents({ ...historyFilter(id, filter), offset }));
      const found = page.find((e) => e.review?.operation_id === operation.operation_id);
      if (found) return filter?.taskId !== undefined && (found.task_id ?? "") !== filter.taskId ? undefined : previousResult(found, id, status, operation);
      if (page.length < PAGE) break;
      if (offset + PAGE >= MAX_EVENTS) throw new Error("Review operation lookup budget exceeded");
    }
  }
  const row = sync(store.queryL1Records({ ...filter, recordIds: [id], visibility: "all" }, { strict: true }))[0] ?? historicalReviewRowSync(store, id, filter);
  if (!row) return undefined;
  const result = change(row, status, operation);
  if (result.event) {
    if (!store.appendMemoryEvent) throw new Error("Review ledger unavailable");
    sync(store.appendMemoryEvent(result.event));
  }
  return result;
}

export async function setReviewStatus(store: IMemoryStore, id: string, status: ReviewStatus, filter?: IsolationFilter, operation: Operation = {}) {
  if (operation.operation_id && store.queryMemoryEvents) {
    for (let offset = 0; offset < MAX_EVENTS; offset += PAGE) {
      const page = await store.queryMemoryEvents({ ...historyFilter(id, filter), offset });
      const found = page.find((e) => e.review?.operation_id === operation.operation_id);
      if (found) return filter?.taskId !== undefined && (found.task_id ?? "") !== filter.taskId ? undefined : previousResult(found, id, status, operation);
      if (page.length < PAGE) break;
      if (offset + PAGE >= MAX_EVENTS) throw new Error("Review operation lookup budget exceeded");
    }
  }
  const row = (await store.queryL1Records({ ...filter, recordIds: [id], visibility: "all" }, { strict: true }))[0] ?? await historicalReviewRow(store, id, filter);
  if (!row) return undefined;
  const result = change(row, status, operation);
  if (result.event) {
    if (!store.appendMemoryEvent) throw new Error("Review ledger unavailable");
    await store.appendMemoryEvent(result.event);
  }
  return result;
}
