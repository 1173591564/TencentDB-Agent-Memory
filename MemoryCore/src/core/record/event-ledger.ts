/**
 * Memory change-ledger writer with a JSONL outbox.
 *
 * Every memory event is appended to this writer's shard
 * `events/YYYY-MM-DD.<writerId>.jsonl` (`events/YYYY-MM-DD.jsonl` when no
 * writer id is configured) via the StorageAdapter, so local fs and COS behave
 * the same, before it is written
 * to the configured store. Both steps are best-effort and never throw, so the
 * write path keeps its availability semantics. Because each event carries a
 * stable `event_id` and every backend treats a repeated `event_id` as a no-op,
 * `replayLedgerEvents` can re-apply the outbox any number of times to backfill
 * events the store missed (backend outage, node rebuild, backend migration).
 *
 * Clear/archive/TTL append a redaction marker to the outbox, rewrite this
 * writer's shards so matching lines lose their content/snapshot, and blank the
 * same events in the store; replay applies the markers, so a backfill never
 * writes cleared content back. Shards are only ever rewritten by the writer
 * that appends to them (plus legacy unsuffixed shards), so a rewrite never
 * races another node's append; in-process appends and rewrites of a shard are
 * serialized.
 */
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { hostname } from "node:os";
import { join } from "node:path";
import type { StorageAdapter } from "../storage/adapter.js";
import { StoragePaths } from "../storage/types.js";
import type { IMemoryStore, MemoryEvent, MemoryEventRedactFilter } from "../store/types.js";
import { withMemoryEventId } from "../store/memory-event-id.js";
import type { Logger } from "../types.js";

/** The store capabilities the ledger needs; health is tracked per store object and tenant. */
export type LedgerStore = Pick<IMemoryStore, "appendMemoryEvent" | "redactMemoryEvents">;

const TAG = "[memory-tdai][event-ledger]";

export interface LedgerHealth {
  store_failures: number;
  jsonl_failures: number;
  last_failure_at?: string;
  /** Events that reached the outbox but not the store and have not been replayed yet. */
  pending_store_events: number;
  /**
   * Clear/TTL redactions not fully landed: the store wipe and/or the outbox
   * marker/shard rewrite failed (backfill retries both).
   */
  pending_redactions: number;
  /** Of `pending_redactions`, those whose outbox marker or shard rewrite is still outstanding. */
  pending_outbox_rewrites: number;
}

export interface LedgerAppendResult {
  event_id: string;
  jsonl: boolean;
  store: boolean;
}

export interface LedgerReplayResult {
  files: number;
  scanned: number;
  replayed: number;
  skipped: number;
  malformed: number;
  failed: number;
  /** Replayed events covered by a clear/TTL marker (replayed as content-less skeletons). */
  redacted: number;
  /** Clear/TTL markers re-applied to the store. */
  redactions_applied: number;
  /** Outbox lines blanked in place (pending rewrites retried + owned shards swept against markers). */
  outbox_redacted: number;
  /** Outbox marker appends / shard rewrites that failed again (kept pending). */
  outbox_failed: number;
}

/** Tenant scope used by health, replay and redaction (unset fields match anything). */
export interface LedgerScope {
  team_id?: string;
  agent_id?: string;
  user_id?: string;
  task_id?: string;
}

type LedgerLogger = Pick<Logger, "warn"> & Partial<Pick<Logger, "debug">>;

interface RedactionMarker {
  redact: MemoryEventRedactFilter;
  marker_ts: string;
}

const MAX_PENDING_TRACKED = 10_000;

interface TenantHealth {
  team_id: string;
  agent_id: string;
  store_failures: number;
  jsonl_failures: number;
  last_failure_at?: string;
  /** event_id → record_id for events written to the outbox but missing from the store. */
  pending: Map<string, string>;
  /** Store failures that cannot be cleared by replay (no outbox copy, or pending overflowed). */
  unrecoverable: number;
}

const healthByStore = new WeakMap<object, Map<string, TenantHealth>>();
interface PendingRedaction {
  filter: MemoryEventRedactFilter;
  /** Tenants a scoped backfill already redacted (for filters not pinned to one tenant). */
  cleared: Set<string>;
  /** The store wipe has not landed. */
  store: boolean;
  /** The outbox shard rewrite has not landed. */
  outbox: boolean;
  /** The outbox marker append failed; retried verbatim. */
  marker?: RedactionMarker;
}

function markPending(
  store: LedgerStore | undefined,
  filter: MemoryEventRedactFilter,
  part: { store?: true; outbox?: true; marker?: RedactionMarker },
): void {
  const m = pendingRedactions(store);
  const k = redactionKey(filter);
  let p = m.get(k);
  if (!p) {
    p = { filter, cleared: new Set(), store: false, outbox: false };
    m.set(k, p);
  }
  if (part.store) { p.store = true; p.cleared.clear(); }
  if (part.outbox) p.outbox = true;
  if (part.marker) p.marker = part.marker;
}

function settlePending(m: Map<string, PendingRedaction>, p: PendingRedaction): void {
  if (!p.store && !p.outbox && !p.marker) m.delete(redactionKey(p.filter));
}

/** Per store: failed store redactions keyed by their canonical filter. */
const pendingRedactionsByStore = new WeakMap<object, Map<string, PendingRedaction>>();

function redactionKey(f: MemoryEventRedactFilter): string {
  return JSON.stringify([f.team_id ?? null, f.agent_id ?? null, f.user_id ?? null, f.until]);
}

function pendingRedactions(store: LedgerStore | undefined): Map<string, PendingRedaction> {
  const key = store ?? UNBOUND_STORE;
  let m = pendingRedactionsByStore.get(key);
  if (!m) {
    m = new Map();
    pendingRedactionsByStore.set(key, m);
  }
  return m;
}

/** A redaction filter touches the scope when it is not pinned to a different team/agent. */
function redactionTouches(f: MemoryEventRedactFilter, scope?: LedgerScope): boolean {
  return (f.team_id === undefined || scope?.team_id === undefined || f.team_id === scope.team_id) &&
    (f.agent_id === undefined || scope?.agent_id === undefined || f.agent_id === scope.agent_id) &&
    (f.user_id === undefined || scope?.user_id === undefined || f.user_id === scope.user_id);
}

function scopeTenantKey(scope?: LedgerScope): string | undefined {
  return scope?.team_id !== undefined && scope.agent_id !== undefined ? tenantKey(scope.team_id, scope.agent_id) : undefined;
}

function storeRedactionPendingFor(p: PendingRedaction, scope?: LedgerScope): boolean {
  if (!p.store || !redactionTouches(p.filter, scope)) return false;
  const k = scopeTenantKey(scope);
  return k === undefined || !p.cleared.has(k);
}

function outboxRedactionPendingFor(p: PendingRedaction, scope?: LedgerScope): boolean {
  return (p.outbox || p.marker !== undefined) && redactionTouches(p.filter, scope);
}
const UNBOUND_STORE = {};

function tenantKey(team: string, agent: string): string {
  return `${team}\u0001${agent}`;
}

function tenantsFor(store: LedgerStore | undefined): Map<string, TenantHealth> {
  const key = store ?? UNBOUND_STORE;
  let m = healthByStore.get(key);
  if (!m) {
    m = new Map();
    healthByStore.set(key, m);
  }
  return m;
}

function tenantHealth(store: LedgerStore | undefined, event: Pick<MemoryEvent, "team_id" | "agent_id">): TenantHealth {
  const team = event.team_id ?? "";
  const agent = event.agent_id ?? "";
  const tenants = tenantsFor(store);
  const k = tenantKey(team, agent);
  let h = tenants.get(k);
  if (!h) {
    h = { team_id: team, agent_id: agent, store_failures: 0, jsonl_failures: 0, pending: new Map(), unrecoverable: 0 };
    tenants.set(k, h);
  }
  return h;
}

function matchingTenants(store: LedgerStore | undefined, scope?: LedgerScope): TenantHealth[] {
  return [...tenantsFor(store).values()].filter((h) =>
    (scope?.team_id === undefined || h.team_id === scope.team_id) &&
    (scope?.agent_id === undefined || h.agent_id === scope.agent_id),
  );
}

function recordFailure(
  store: LedgerStore | undefined,
  kind: "store" | "jsonl",
  event: MemoryEvent & { event_id: string },
  recoverable: boolean,
): void {
  const h = tenantHealth(store, event);
  if (kind === "store") {
    h.store_failures += 1;
    if (recoverable && h.pending.size < MAX_PENDING_TRACKED) h.pending.set(event.event_id, event.record_id);
    else h.unrecoverable += 1;
  } else {
    h.jsonl_failures += 1;
  }
  h.last_failure_at = new Date().toISOString();
}

/**
 * Snapshot of this process's ledger health for the given store, restricted to
 * the tenants matching `scope` (a tenant only sees its own failures). Raw
 * backend errors are logged, never returned. `degraded` stays set while
 * events are missing from the store: pending events clear once a backfill
 * replays them; failures without an outbox copy (or beyond the tracking cap)
 * clear only via `resetLedgerHealth` or a restart. Outbox write failures alone
 * do not degrade review data and are reported via `jsonl_failures` only.
 */
export function getLedgerHealth(
  store: LedgerStore | undefined,
  scope?: LedgerScope,
): LedgerHealth & { degraded: boolean } {
  let store_failures = 0;
  let jsonl_failures = 0;
  let pending = 0;
  let unrecoverable = 0;
  let last: string | undefined;
  for (const h of matchingTenants(store, scope)) {
    store_failures += h.store_failures;
    jsonl_failures += h.jsonl_failures;
    pending += h.pending.size;
    unrecoverable += h.unrecoverable;
    if (h.last_failure_at && (!last || h.last_failure_at > last)) last = h.last_failure_at;
  }
  const pendingR = [...pendingRedactions(store).values()];
  const redactions = pendingR.filter((p) => storeRedactionPendingFor(p, scope) || outboxRedactionPendingFor(p, scope)).length;
  const outboxRewrites = pendingR.filter((p) => outboxRedactionPendingFor(p, scope)).length;
  return {
    store_failures,
    jsonl_failures,
    ...(last ? { last_failure_at: last } : {}),
    pending_store_events: pending + unrecoverable,
    pending_redactions: redactions,
    pending_outbox_rewrites: outboxRewrites,
    degraded: pending > 0 || unrecoverable > 0 || redactions > 0,
  };
}

/**
 * Whether the store may be missing an event of `recordId` for this tenant
 * (pending replay, or an unrecoverable store failure). Review decisions that
 * depend on the full event history of the record must not proceed.
 */
export function hasPendingLedgerEvent(store: LedgerStore | undefined, recordId: string, scope?: LedgerScope): boolean {
  for (const h of matchingTenants(store, scope)) {
    if (h.unrecoverable > 0) return true;
    for (const rid of h.pending.values()) if (rid === recordId) return true;
  }
  return false;
}

/** Test/ops hook: clear the failure counters. */
export function resetLedgerHealth(store: LedgerStore | undefined): void {
  healthByStore.delete(store ?? UNBOUND_STORE);
  pendingRedactionsByStore.delete(store ?? UNBOUND_STORE);
}

function shardDateOf(ts: string): string {
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? new Date().toISOString().slice(0, 10) : d.toISOString().slice(0, 10);
}

// ── Outbox shards ──
//
//   events/YYYY-MM-DD.jsonl                  legacy / no writer id configured
//   events/YYYY-MM-DD.<writerId>.jsonl       live shard this writer appends to
//   events/YYYY-MM-DD[.<writerId>]~<gen>.jsonl  sealed shard produced by a rewrite
//
// A rewrite never overwrites an object in place (COS appendable objects cannot
// be overwritten): it creates a fresh sealed shard with the redacted content,
// then deletes the old one. Later appends recreate the live shard.

const WRITER_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
const SHARD_RE = /^(\d{4}-\d{2}-\d{2})(?:\.([A-Za-z0-9_-]{1,64}))?(?:~([a-z0-9]{1,32}))?\.jsonl$/;

let ledgerWriterId: string | undefined = newLedgerWriterId();

/**
 * Set this process's outbox writer id (stable per node/boot). New events go
 * to `events/YYYY-MM-DD.<writerId>.jsonl`; rewrites only touch shards with
 * this suffix (and legacy unsuffixed shards). `undefined` restores the legacy
 * single-writer naming.
 */
export function setLedgerWriterId(id: string | undefined): void {
  if (id !== undefined && !WRITER_ID_RE.test(id)) throw new Error(`invalid ledger writer id: ${JSON.stringify(id)}`);
  ledgerWriterId = id;
}

export function getLedgerWriterId(): string | undefined {
  return ledgerWriterId;
}

/** A fresh writer id: `<host>-<8 hex>`. */
export function newLedgerWriterId(): string {
  const host = hostname().replace(/[^A-Za-z0-9_-]/g, "").slice(0, 32) || "node";
  return `${host}-${randomBytes(4).toString("hex")}`;
}

const WRITER_ID_FILE = join(".metadata", "ledger_writer_id");

/**
 * Adopt the writer id persisted in `dataDir` (created on first use), so a
 * restarted process keeps owning — and can still rewrite — the shards it
 * wrote before. One writer process per data dir is assumed. Falls back to
 * the per-boot id already in effect when the file cannot be read or written.
 */
export function loadLedgerWriterId(dataDir: string, logger?: LedgerLogger): string | undefined {
  const file = join(dataDir, WRITER_ID_FILE);
  try {
    const existing = readFileSync(file, "utf-8").trim();
    if (WRITER_ID_RE.test(existing)) {
      ledgerWriterId = existing;
      return ledgerWriterId;
    }
  } catch {
    // not created yet
  }
  const id = newLedgerWriterId();
  try {
    mkdirSync(join(dataDir, ".metadata"), { recursive: true });
    writeFileSync(file, id, "utf-8");
    ledgerWriterId = id;
  } catch (err) {
    logger?.warn?.(`${TAG} cannot persist outbox writer id (using per-boot ${ledgerWriterId}): ${err instanceof Error ? err.message : String(err)}`);
  }
  return ledgerWriterId;
}

interface ShardName {
  name: string;
  date: string;
  writer?: string;
}

export function parseLedgerShardName(name: string): ShardName | undefined {
  const m = SHARD_RE.exec(name);
  return m ? { name, date: m[1]!, ...(m[2] ? { writer: m[2] } : {}) } : undefined;
}

/** Shards this process may rewrite: its own, plus legacy unsuffixed shards (best-effort shared). */
function ownsShard(s: ShardName): boolean {
  return s.writer === undefined || s.writer === ledgerWriterId;
}

async function listShards(storage: StorageAdapter): Promise<ShardName[]> {
  const shards: ShardName[] = [];
  let marker: string | undefined;
  do {
    const page = await storage.readdirPage(StoragePaths.eventsDir, { suffix: ".jsonl", maxKeys: 1000, marker });
    for (const entry of page.entries) {
      if (entry.isDirectory) continue;
      const name = entry.key.startsWith(StoragePaths.eventsDir) ? entry.key.slice(StoragePaths.eventsDir.length) : entry.key;
      const shard = parseLedgerShardName(name);
      if (shard) shards.push(shard);
    }
    marker = page.nextMarker;
  } while (marker !== undefined);
  return shards.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}

function sealedShardKey(s: ShardName): string {
  const gen = `${Date.now().toString(36)}${randomBytes(4).toString("hex")}`;
  return `${StoragePaths.eventsDir}${s.date}${s.writer ? `.${s.writer}` : ""}~${gen}.jsonl`;
}

/** Per-shard FIFO: appends and rewrites of the same key never interleave within this process. */
const shardQueues = new Map<string, Promise<void>>();

async function withShardLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const prev = shardQueues.get(key) ?? Promise.resolve();
  let release!: () => void;
  const mine = new Promise<void>((r) => { release = r; });
  const tail = prev.then(() => mine);
  shardQueues.set(key, tail);
  await prev;
  try {
    return await fn();
  } finally {
    release();
    if (shardQueues.get(key) === tail) shardQueues.delete(key);
  }
}

async function appendToLiveShard(storage: StorageAdapter, ts: string, line: string): Promise<void> {
  const key = StoragePaths.eventShard(shardDateOf(ts), ledgerWriterId);
  await withShardLock(key, () => storage.appendFile(key, line));
}

/**
 * Blank content/snapshot_json of event lines covered by any filter; marker
 * lines, malformed lines and uncovered events are kept byte-for-byte.
 */
function redactOutboxContent(content: string, filters: readonly MemoryEventRedactFilter[]): { content: string; lines: number } {
  let lines = 0;
  const out = content.split("\n").map((line) => {
    if (!line.trim()) return line;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      return line;
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) return line;
    const e = value as MemoryEvent & Partial<RedactionMarker>;
    if (e.redact !== undefined || typeof e.event_ts !== "string") return line;
    const hasPlaintext = (typeof e.content === "string" && e.content !== "") || (e.snapshot_json !== undefined && e.snapshot_json !== "");
    if (!hasPlaintext || !filters.some((f) => markerCovers(f, e))) return line;
    lines += 1;
    const { snapshot_json: _dropped, ...skeleton } = e;
    return JSON.stringify({ ...skeleton, content: "" });
  });
  return { content: out.join("\n"), lines };
}

/**
 * Rewrite the owned shards that may hold events covered by `filters`
 * (shard date ≤ the latest `until`). Each rewrite runs under the shard lock:
 * re-read, create a sealed copy atomically, delete the original. Throws after
 * trying every shard if any rewrite failed.
 */
async function rewriteOutboxShards(
  storage: StorageAdapter,
  filters: readonly MemoryEventRedactFilter[],
  only?: ReadonlySet<string>,
): Promise<number> {
  if (filters.length === 0) return 0;
  const maxDate = filters.map((f) => shardDateOf(f.until)).sort().at(-1)!;
  const shards = (await listShards(storage)).filter((s) => ownsShard(s) && s.date <= maxDate && (!only || only.has(s.name)));
  let lines = 0;
  let firstErr: unknown;
  for (const s of shards) {
    const key = `${StoragePaths.eventsDir}${s.name}`;
    try {
      lines += await withShardLock(key, async () => {
        const content = await storage.readFile(key);
        if (content === null) return 0;
        const r = redactOutboxContent(content, filters);
        if (r.lines === 0) return 0;
        await storage.createFileAtomic(sealedShardKey(s), r.content);
        await storage.unlink(key);
        return r.lines;
      });
    } catch (err) {
      firstErr ??= err;
    }
  }
  if (firstErr !== undefined) throw firstErr;
  return lines;
}

export async function appendLedgerEvent(params: {
  store: LedgerStore | undefined;
  storage?: StorageAdapter;
  event: MemoryEvent;
  logger?: LedgerLogger;
}): Promise<LedgerAppendResult> {
  const { store, storage, logger } = params;
  const event = withMemoryEventId(params.event);
  const result: LedgerAppendResult = { event_id: event.event_id, jsonl: false, store: false };

  if (storage) {
    try {
      await appendToLiveShard(storage, event.event_ts, JSON.stringify(event) + "\n");
      result.jsonl = true;
    } catch (err) {
      recordFailure(store, "jsonl", event, false);
      logger?.warn?.(
        `${TAG} outbox append failed (non-fatal) event_id=${event.event_id} record_id=${event.record_id}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  } else {
    logger?.debug?.(`${TAG} outbox skipped: no storage adapter (event_id=${event.event_id})`);
  }

  if (store?.appendMemoryEvent) {
    try {
      await store.appendMemoryEvent(event);
      result.store = true;
    } catch (err) {
      recordFailure(store, "store", event, result.jsonl);
      logger?.warn?.(
        `${TAG} store append failed (non-fatal${result.jsonl ? ", recoverable via backfill" : ""}) ` +
        `event_id=${event.event_id} record_id=${event.record_id} op=${event.op}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
  return result;
}

/**
 * Blank content/snapshot of the events matching `filter` (clear/archive/TTL):
 * appends a redaction marker to the outbox (so replay honours it), rewrites
 * the owned outbox shards so matching lines lose their plaintext, and redacts
 * the store. Best-effort, never throws: any part that fails is tracked in
 * `pending_redactions` (ledger degraded) and retried by backfill.
 */
export async function redactLedgerEvents(params: {
  store: LedgerStore | undefined;
  storage?: StorageAdapter;
  filter: MemoryEventRedactFilter;
  logger?: LedgerLogger;
}): Promise<{ jsonl: boolean; rewritten?: number; redacted?: number }> {
  const { store, storage, filter, logger } = params;
  const out: { jsonl: boolean; rewritten?: number; redacted?: number } = { jsonl: false };
  if (storage) {
    const marker: RedactionMarker = { redact: filter, marker_ts: new Date().toISOString() };
    try {
      await appendToLiveShard(storage, marker.marker_ts, JSON.stringify(marker) + "\n");
      out.jsonl = true;
    } catch (err) {
      markPending(store, filter, { marker });
      logger?.warn?.(`${TAG} redaction marker append failed (non-fatal, retried by backfill): ${err instanceof Error ? err.message : String(err)}`);
    }
    try {
      out.rewritten = await rewriteOutboxShards(storage, [filter]);
    } catch (err) {
      markPending(store, filter, { outbox: true });
      logger?.warn?.(`${TAG} outbox shard rewrite failed (non-fatal, retried by backfill): ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  if (store?.redactMemoryEvents) {
    try {
      out.redacted = await store.redactMemoryEvents(filter);
    } catch (err) {
      markPending(store, filter, { store: true });
      logger?.warn?.(
        `${TAG} store redaction failed (non-fatal${out.jsonl ? ", re-applied by backfill" : ""}): ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
  return out;
}

/** Retry outbox markers / shard rewrites that failed when the redaction ran. */
async function retryPendingOutbox(
  store: LedgerStore,
  storage: StorageAdapter,
  out: LedgerReplayResult,
  logger?: LedgerLogger,
): Promise<void> {
  const pendingR = pendingRedactions(store);
  for (const p of [...pendingR.values()]) {
    if (p.marker) {
      try {
        await appendToLiveShard(storage, p.marker.marker_ts, JSON.stringify(p.marker) + "\n");
        p.marker = undefined;
      } catch (err) {
        out.outbox_failed += 1;
        logger?.warn?.(`${TAG} replay: redaction marker retry failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    if (p.outbox) {
      try {
        out.outbox_redacted += await rewriteOutboxShards(storage, [p.filter]);
        p.outbox = false;
      } catch (err) {
        out.outbox_failed += 1;
        logger?.warn?.(`${TAG} replay: outbox shard rewrite retry failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    settlePending(pendingR, p);
  }
}

const MEMORY_EVENT_OPS: ReadonlySet<string> = new Set<MemoryEvent["op"]>(
  ["created", "updated", "merged", "superseded", "reverted", "deleted"],
);

function isReplayableEvent(e: Partial<Record<keyof MemoryEvent, unknown>>): e is MemoryEvent {
  return typeof e.event_id === "string" && e.event_id !== "" &&
    typeof e.record_id === "string" && e.record_id !== "" &&
    typeof e.op === "string" && MEMORY_EVENT_OPS.has(e.op) &&
    typeof e.event_ts === "string" && !Number.isNaN(Date.parse(e.event_ts));
}

function markerCovers(m: MemoryEventRedactFilter, e: MemoryEvent): boolean {
  return e.event_ts <= m.until &&
    (m.team_id === undefined || (e.team_id ?? "") === m.team_id) &&
    (m.agent_id === undefined || (e.agent_id ?? "") === m.agent_id) &&
    (m.user_id === undefined || (e.user_id ?? "") === m.user_id);
}

function inScope(scope: LedgerScope | undefined, e: MemoryEvent): boolean {
  return (scope?.team_id === undefined || (e.team_id ?? "") === scope.team_id) &&
    (scope?.agent_id === undefined || (e.agent_id ?? "") === scope.agent_id) &&
    (scope?.user_id === undefined || (e.user_id ?? "") === scope.user_id) &&
    (scope?.task_id === undefined || (e.task_id ?? "") === scope.task_id);
}

/**
 * Re-apply outbox events to the store. Idempotent: events already present
 * (same event_id) are no-ops on every backend. Pending outbox rewrites are
 * retried first; redaction markers found in the scanned shards (from every
 * writer) are then applied to the store and to this writer's own scanned
 * shards, so cleared content is never restored and converges out of the outbox.
 */
export async function replayLedgerEvents(params: {
  store: LedgerStore;
  storage: StorageAdapter;
  /** Only replay shards dated on/after this ISO timestamp (and events at/after it). */
  since?: string;
  scope?: LedgerScope;
  logger?: LedgerLogger;
}): Promise<LedgerReplayResult> {
  const { store, storage, since, scope, logger } = params;
  const out: LedgerReplayResult = {
    files: 0, scanned: 0, replayed: 0, skipped: 0, malformed: 0, failed: 0, redacted: 0, redactions_applied: 0,
    outbox_redacted: 0, outbox_failed: 0,
  };
  await retryPendingOutbox(store, storage, out, logger);
  if (!store.appendMemoryEvent) return out;

  const sinceDate = since ? new Date(since).toISOString().slice(0, 10) : undefined;
  const shards = (await listShards(storage)).filter((s) => !sinceDate || s.date >= sinceDate);

  const events: MemoryEvent[] = [];
  const markers: MemoryEventRedactFilter[] = [];
  const ownedContent = new Map<string, string>();
  for (const shard of shards) {
    const content = await storage.readFile(`${StoragePaths.eventsDir}${shard.name}`);
    if (content === null) continue;
    out.files += 1;
    if (ownsShard(shard)) ownedContent.set(shard.name, content);
    for (const line of content.split("\n")) {
      if (!line.trim()) continue;
      out.scanned += 1;
      let value: unknown;
      try {
        value = JSON.parse(line);
      } catch {
        out.malformed += 1;
        continue;
      }
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
        out.malformed += 1;
        continue;
      }
      const parsed = value as MemoryEvent & Partial<RedactionMarker>;
      if (parsed.redact && typeof parsed.redact.until === "string") {
        markers.push(parsed.redact);
        continue;
      }
      if (!isReplayableEvent(parsed)) {
        out.malformed += 1;
        continue;
      }
      events.push(parsed);
    }
  }

  // Re-apply clear/TTL redactions to the store first (idempotent): a redaction
  // the store missed when it ran must not leave cleared content behind.
  // Markers are narrowed to the requested scope so a tenant backfill never
  // touches other tenants' events.
  const pendingR = pendingRedactions(store);
  const markerKeys = new Set(markers.map(redactionKey));
  for (const p of pendingR.values()) {
    if (p.store && !markerKeys.has(redactionKey(p.filter))) {
      markers.push(p.filter);
      markerKeys.add(redactionKey(p.filter));
    }
  }

  // Sweep this writer's scanned shards against every marker seen (markers may
  // come from other writers), so their plaintext converges out of the outbox.
  const dirty = new Set([...ownedContent].filter(([, c]) => redactOutboxContent(c, markers).lines > 0).map(([n]) => n));
  if (dirty.size > 0) {
    try {
      out.outbox_redacted += await rewriteOutboxShards(storage, markers, dirty);
    } catch (err) {
      out.outbox_failed += 1;
      for (const filter of markers) markPending(store, filter, { outbox: true });
      logger?.warn?.(`${TAG} replay: outbox sweep rewrite failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  if (store.redactMemoryEvents) {
    for (const m of markers) {
      if (!redactionTouches(m, scope)) continue;
      const narrowed: MemoryEventRedactFilter = {
        ...m,
        ...(m.team_id === undefined && scope?.team_id !== undefined ? { team_id: scope.team_id } : {}),
        ...(m.agent_id === undefined && scope?.agent_id !== undefined ? { agent_id: scope.agent_id } : {}),
      };
      try {
        await store.redactMemoryEvents(narrowed);
        out.redactions_applied += 1;
        const pending = pendingR.get(redactionKey(m));
        const tenant = scopeTenantKey(scope);
        if (pending?.store && redactionKey(narrowed) === redactionKey(m)) {
          pending.store = false;
          settlePending(pendingR, pending);
        } else if (pending?.store && tenant !== undefined) pending.cleared.add(tenant);
      } catch (err) {
        out.failed += 1;
        logger?.warn?.(`${TAG} replay redaction failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }

  const pendingByTenant = new Map<string, TenantHealth>();
  for (const h of tenantsFor(store).values()) pendingByTenant.set(tenantKey(h.team_id, h.agent_id), h);

  // Rewrites move lines into sealed shards, so file order is not event order.
  events.sort((a, b) => (a.event_ts < b.event_ts ? -1 : a.event_ts > b.event_ts ? 1 : 0));
  for (const raw of events) {
    if ((since && raw.event_ts < since) || !inScope(scope, raw)) {
      out.skipped += 1;
      continue;
    }
    let event = raw;
    if (markers.some((m) => markerCovers(m, raw))) {
      event = { ...raw, content: "", snapshot_json: undefined };
      out.redacted += 1;
    }
    try {
      await store.appendMemoryEvent(event);
      out.replayed += 1;
      pendingByTenant.get(tenantKey(event.team_id ?? "", event.agent_id ?? ""))?.pending.delete(event.event_id!);
    } catch (err) {
      out.failed += 1;
      logger?.warn?.(
        `${TAG} replay failed event_id=${event.event_id}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
  return out;
}

/** Delete outbox shards dated strictly before `beforeDate` (YYYY-MM-DD). Returns deleted shard count. */
export async function pruneLedgerOutbox(storage: StorageAdapter, beforeDate: string): Promise<number> {
  const names = (await listShards(storage)).filter((s) => s.date < beforeDate).map((s) => s.name);
  for (const name of names) await storage.unlink(`${StoragePaths.eventsDir}${name}`);
  return names.length;
}
