import { randomUUID } from "node:crypto";
import { DEFAULT_ISOLATION_ID } from "./isolation.js";
import type { MemoryEvent, MemoryEventRedactFilter } from "./types.js";

/**
 * Stable per-event identity: `evt-` + 32 hex chars (36 chars total), well
 * under the TCVDB document id limit of 128. Generated once at the logical
 * write point and carried through the JSONL outbox and every store backend,
 * so replaying the same event is idempotent.
 */
export function newMemoryEventId(): string {
  return `evt-${randomUUID().replace(/-/g, "")}`;
}

export function withMemoryEventId<T extends MemoryEvent>(event: T): T & { event_id: string } {
  return event.event_id ? (event as T & { event_id: string }) : { ...event, event_id: newMemoryEventId() };
}

/**
 * Ledger timestamp contract: every timestamp that reaches a string
 * comparison (event_ts bounds, redaction `until`, stored rows) must be the
 * canonical millisecond form `YYYY-MM-DDTHH:mm:ss.sssZ`, where lexical order
 * equals chronological order. Admits every ms-exact ISO 8601 shape —
 * `…ssZ`, `…ss.ssZ`, `…ss+08:00`, `…ss+0800`, even `…HH:mmZ` — and
 * normalizes it; rejects date-only, zone-less, space-separated,
 * sub-millisecond (>3 fractional digits), and unparseable values so nothing
 * lossy or ambiguous reaches a lexical compare. Returns null for anything
 * not losslessly representable at ms precision.
 */
const MS_EXACT_ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,3})?)?(Z|[+-]\d{2}:?\d{2})$/;

export function canonIsoTs(v: string): string | null {
  if (!MS_EXACT_ISO_RE.test(v)) return null;
  const ms = Date.parse(v);
  return Number.isNaN(ms) ? null : new Date(ms).toISOString();
}

/** Event identity contract: `evt-` + 32 lowercase hex. */
export const EVENT_ID_RE = /^evt-[0-9a-f]{32}$/;

/**
 * L0/L1 instant columns (updated_time / created_time / recorded_at) share
 * the canonical-instant contract with event_ts, plus one sentinel: "" means
 * "no timestamp" and TTL guards deliberately skip it (`!= ''` in sqlite,
 * `_ms > 0` for the numeric twins). Canonicalize anything else; null = the
 * write is rejected rather than persisting an uncomparable value.
 */
export function canonRecordTs(v: string | undefined): string | null {
  return v === undefined || v === "" ? "" : canonIsoTs(v);
}

/**
 * Isolation-id contract: "" and "default" denote the same logical "no
 * value" (writes converge on "default"; legacy/foreign rows may still
 * carry ""). A defined filter value heals "" → "default"; undefined stays
 * undefined (unconstrained). Row-side healing is plain `v || "default"`.
 */
export function healIsoId(v: string | undefined): string | undefined {
  return v === undefined ? undefined : v || DEFAULT_ISOLATION_ID;
}

/**
 * Redact-filter whitelist shared by the marker-read path and the live
 * redaction entry. Only {team_id, agent_id, user_id, until} carry meaning —
 * coverage and store wipes ignore every other field, so a filter or marker
 * smuggling an unknown field (e.g. a runtime-cast `task_id`) would erase
 * wider than it claims. Rejected rather than silently narrowed.
 */
export function isValidRedactFilter(f: unknown): f is MemoryEventRedactFilter {
  if (typeof f !== "object" || f === null || Array.isArray(f)) return false;
  const r = f as Record<string, unknown>;
  for (const k of Object.keys(r)) {
    if (k !== "team_id" && k !== "agent_id" && k !== "user_id" && k !== "until") return false;
  }
  for (const k of ["team_id", "agent_id", "user_id"] as const) {
    if (r[k] !== undefined && typeof r[k] !== "string") return false;
  }
  return typeof r.until === "string" && canonIsoTs(r.until) !== null;
}
