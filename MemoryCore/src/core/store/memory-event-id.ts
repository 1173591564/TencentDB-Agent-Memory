import { randomUUID } from "node:crypto";
import type { MemoryEvent } from "./types.js";

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
