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
 */
import type { StorageAdapter } from "../storage/adapter.js";
import { StoragePaths } from "../storage/types.js";
import type { IMemoryStore, MemoryEvent } from "../store/types.js";

/** The only store capability the ledger needs; health is tracked per store object. */
export type LedgerStore = Pick<IMemoryStore, "appendMemoryEvent">;
import { withMemoryEventId } from "../store/memory-event-id.js";
import type { Logger } from "../types.js";

const TAG = "[memory-tdai][event-ledger]";

export interface LedgerHealth {
  store_failures: number;
  jsonl_failures: number;
  last_failure_at?: string;
  last_error?: string;
  /** Events that reached the outbox but not the store and have not been replayed yet. */
  pending_store_events: number;
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
}

type LedgerLogger = Pick<Logger, "warn"> & Partial<Pick<Logger, "debug">>;

const MAX_PENDING_TRACKED = 10_000;

interface HealthState {
  store_failures: number;
  jsonl_failures: number;
  last_failure_at?: string;
  last_error?: string;
  /** event_ids written to the outbox but missing from the store. */
  pending: Set<string>;
  /** Store failures that cannot be cleared by replay (no outbox copy, or pending overflowed). */
  unrecoverable: number;
}

const healthByStore = new WeakMap<object, HealthState>();
const UNBOUND_STORE = {};

function healthFor(store: LedgerStore | undefined): HealthState {
  const key = store ?? UNBOUND_STORE;
  let h = healthByStore.get(key);
  if (!h) {
    h = { store_failures: 0, jsonl_failures: 0, pending: new Set(), unrecoverable: 0 };
    healthByStore.set(key, h);
  }
  return h;
}

function recordFailure(
  store: LedgerStore | undefined,
  kind: "store" | "jsonl",
  err: unknown,
  pendingEventId?: string,
): void {
  const h = healthFor(store);
  if (kind === "store") {
    h.store_failures += 1;
    if (pendingEventId && h.pending.size < MAX_PENDING_TRACKED) h.pending.add(pendingEventId);
    else h.unrecoverable += 1;
  } else {
    h.jsonl_failures += 1;
  }
  h.last_failure_at = new Date().toISOString();
  h.last_error = `${kind}: ${err instanceof Error ? err.message : String(err)}`;
}

/**
 * Snapshot of this process's ledger health for the given store. `degraded`
 * stays set while events are missing from the store: pending events clear
 * once a backfill replays them; failures without an outbox copy (or beyond
 * the tracking cap) clear only via `resetLedgerHealth` or a restart. Outbox
 * write failures alone do not degrade review data (the store has the event)
 * and are reported via `jsonl_failures` only.
 */
export function getLedgerHealth(store: LedgerStore | undefined): LedgerHealth & { degraded: boolean } {
  const h = healthFor(store);
  return {
    store_failures: h.store_failures,
    jsonl_failures: h.jsonl_failures,
    ...(h.last_failure_at ? { last_failure_at: h.last_failure_at } : {}),
    ...(h.last_error ? { last_error: h.last_error } : {}),
    pending_store_events: h.pending.size + h.unrecoverable,
    degraded: h.pending.size > 0 || h.unrecoverable > 0,
  };
}

/** Test/ops hook: clear the failure counters after a successful backfill. */
export function resetLedgerHealth(store: LedgerStore | undefined): void {
  healthByStore.delete(store ?? UNBOUND_STORE);
}

function shardDateOf(event: MemoryEvent): string {
  const d = new Date(event.event_ts);
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
      await storage.appendFile(StoragePaths.event(shardDateOf(event)), JSON.stringify(event) + "\n");
      result.jsonl = true;
    } catch (err) {
      recordFailure(store, "jsonl", err);
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
      recordFailure(store, "store", err, result.jsonl ? event.event_id : undefined);
      logger?.warn?.(
        `${TAG} store append failed (non-fatal${result.jsonl ? ", recoverable via backfill" : ""}) ` +
        `event_id=${event.event_id} record_id=${event.record_id} op=${event.op}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
  return result;
}

/**
 * Re-apply outbox events to the store. Idempotent: events already present
 * (same event_id) are no-ops on every backend.
 */
export async function replayLedgerEvents(params: {
  store: LedgerStore;
  storage: StorageAdapter;
  /** Only replay shards dated on/after this ISO timestamp (and events at/after it). */
  since?: string;
  /** Only replay events whose tenancy matches (unset fields are not filtered). */
  scope?: { team_id?: string; agent_id?: string };
  logger?: LedgerLogger;
}): Promise<LedgerReplayResult> {
  const { store, storage, since, scope, logger } = params;
  const out: LedgerReplayResult = { files: 0, scanned: 0, replayed: 0, skipped: 0, malformed: 0, failed: 0 };
  if (!store.appendMemoryEvent) return out;
  const pending = healthFor(store).pending;

  const sinceDate = since ? new Date(since).toISOString().slice(0, 10) : undefined;
  const names = (await storage.readdirNames(StoragePaths.eventsDir, ".jsonl"))
    .filter((n) => /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(n))
    .filter((n) => !sinceDate || n.slice(0, 10) >= sinceDate)
    .sort();

  for (const name of names) {
    const content = await storage.readFile(`${StoragePaths.eventsDir}${name}`);
    if (content === null) continue;
    out.files += 1;
    for (const line of content.split("\n")) {
      if (!line.trim()) continue;
      out.scanned += 1;
      let event: MemoryEvent;
      try {
        event = JSON.parse(line) as MemoryEvent;
      } catch {
        out.malformed += 1;
        continue;
      }
      if (!event.event_id || !event.op || !event.record_id || !event.event_ts) {
        out.malformed += 1;
        continue;
      }
      if (
        (since && event.event_ts < since) ||
        (scope?.team_id !== undefined && (event.team_id ?? "") !== scope.team_id) ||
        (scope?.agent_id !== undefined && (event.agent_id ?? "") !== scope.agent_id)
      ) {
        out.skipped += 1;
        continue;
      }
      try {
        await store.appendMemoryEvent(event);
        out.replayed += 1;
        pending.delete(event.event_id);
      } catch (err) {
        out.failed += 1;
        logger?.warn?.(
          `${TAG} replay failed event_id=${event.event_id}: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
  }
  return out;
}
