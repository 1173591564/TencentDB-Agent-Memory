/**
 * Memory change-ledger writer with a JSONL outbox.
 *
 * Every memory event is appended to `events/YYYY-MM-DD.jsonl` (via the
 * StorageAdapter, so local fs and COS behave the same) before it is written
 * to the configured store. Both steps are best-effort and never throw, so the
 * write path keeps its availability semantics. Because each event carries a
 * stable `event_id` and every backend treats a repeated `event_id` as a no-op,
 * `replayLedgerEvents` can re-apply the outbox any number of times to backfill
 * events the store missed (backend outage, node rebuild, backend migration).
 *
 * Clear/archive/TTL append a redaction marker to the outbox and blank the
 * matching events' content/snapshot in the store; replay applies the markers,
 * so a backfill never writes cleared content back.
 */
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
  /** Clear/TTL redactions the store failed to apply (backfill re-applies them from the outbox). */
  pending_redactions: number;
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
  /** Replayed events whose content was blanked by a later clear/TTL marker. */
  redacted: number;
  /** Clear/TTL markers re-applied to the store. */
  redactions_applied: number;
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
    (f.agent_id === undefined || scope?.agent_id === undefined || f.agent_id === scope.agent_id);
}

function scopeTenantKey(scope?: LedgerScope): string | undefined {
  return scope?.team_id !== undefined && scope.agent_id !== undefined ? tenantKey(scope.team_id, scope.agent_id) : undefined;
}

function redactionPendingFor(p: PendingRedaction, scope?: LedgerScope): boolean {
  if (!redactionTouches(p.filter, scope)) return false;
  const k = scopeTenantKey(scope);
  return k === undefined || !p.cleared.has(k);
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
  const redactions = [...pendingRedactions(store).values()].filter((p) => redactionPendingFor(p, scope)).length;
  return {
    store_failures,
    jsonl_failures,
    ...(last ? { last_failure_at: last } : {}),
    pending_store_events: pending + unrecoverable,
    pending_redactions: redactions,
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
      await storage.appendFile(StoragePaths.event(shardDateOf(event.event_ts)), JSON.stringify(event) + "\n");
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
 * appends a redaction marker to the outbox (so replay honours it) and redacts
 * the store. Best-effort, never throws.
 */
export async function redactLedgerEvents(params: {
  store: LedgerStore | undefined;
  storage?: StorageAdapter;
  filter: MemoryEventRedactFilter;
  logger?: LedgerLogger;
}): Promise<{ jsonl: boolean; redacted?: number }> {
  const { store, storage, filter, logger } = params;
  const out: { jsonl: boolean; redacted?: number } = { jsonl: false };
  if (storage) {
    const marker: RedactionMarker = { redact: filter, marker_ts: new Date().toISOString() };
    try {
      await storage.appendFile(StoragePaths.event(shardDateOf(marker.marker_ts)), JSON.stringify(marker) + "\n");
      out.jsonl = true;
    } catch (err) {
      logger?.warn?.(`${TAG} redaction marker append failed (non-fatal): ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  if (store?.redactMemoryEvents) {
    try {
      out.redacted = await store.redactMemoryEvents(filter);
    } catch (err) {
      pendingRedactions(store).set(redactionKey(filter), { filter, cleared: new Set() });
      logger?.warn?.(
        `${TAG} store redaction failed (non-fatal${out.jsonl ? ", re-applied by backfill" : ""}): ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
  return out;
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
 * (same event_id) are no-ops on every backend. Redaction markers found in the
 * scanned shards are applied first, so cleared content is never restored.
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
  const out: LedgerReplayResult = { files: 0, scanned: 0, replayed: 0, skipped: 0, malformed: 0, failed: 0, redacted: 0, redactions_applied: 0 };
  if (!store.appendMemoryEvent) return out;

  const sinceDate = since ? new Date(since).toISOString().slice(0, 10) : undefined;
  const names = (await storage.readdirNames(StoragePaths.eventsDir, ".jsonl"))
    .filter((n) => /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(n))
    .filter((n) => !sinceDate || n.slice(0, 10) >= sinceDate)
    .sort();

  const events: MemoryEvent[] = [];
  const markers: MemoryEventRedactFilter[] = [];
  for (const name of names) {
    const content = await storage.readFile(`${StoragePaths.eventsDir}${name}`);
    if (content === null) continue;
    out.files += 1;
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
  if (store.redactMemoryEvents) {
    const pendingR = pendingRedactions(store);
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
        if (pending && redactionKey(narrowed) === redactionKey(m)) pendingR.delete(redactionKey(m));
        else if (pending && tenant !== undefined) pending.cleared.add(tenant);
      } catch (err) {
        out.failed += 1;
        logger?.warn?.(`${TAG} replay redaction failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }

  const pendingByTenant = new Map<string, TenantHealth>();
  for (const h of tenantsFor(store).values()) pendingByTenant.set(tenantKey(h.team_id, h.agent_id), h);

  for (const raw of events) {
    if ((since && raw.event_ts < since) || !inScope(scope, raw)) {
      out.skipped += 1;
      continue;
    }
    let event = raw;
    if (markers.some((m) => markerCovers(m, raw)) && (raw.content || raw.snapshot_json)) {
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
  const names = (await storage.readdirNames(StoragePaths.eventsDir, ".jsonl"))
    .filter((n) => /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(n) && n.slice(0, 10) < beforeDate);
  for (const name of names) await storage.unlink(`${StoragePaths.eventsDir}${name}`);
  return names.length;
}
