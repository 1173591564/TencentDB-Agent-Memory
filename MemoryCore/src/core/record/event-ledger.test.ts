/**
 * 变更账 JSONL outbox：先写 outbox 再写 store；store 失败时记入健康度，
 * 事后可从 outbox 幂等回放补齐。
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { VectorStore } from "../store/sqlite/memory-store.js";
import { StorageAdapter } from "../storage/adapter.js";
import { createLocalStorageBackend } from "../storage/factory.js";
import { StoragePaths } from "../storage/types.js";
import type { IMemoryStore, MemoryEvent } from "../store/types.js";
import { appendLedgerEvent, getLedgerHealth, redactLedgerEvents, replayLedgerEvents } from "./event-ledger.js";
import { writeMemory, type DedupDecision, type ExtractedMemory } from "./l1-writer.js";

const silent = { warn() {}, debug() {} };

const memory = (content: string): ExtractedMemory => ({
  content, type: "work_fact", priority: 50,
  source_message_ids: [], metadata: {}, scene_name: "default",
});
const decision = (record_id: string, action: DedupDecision["action"], target_ids: string[] = [], merged_content?: string): DedupDecision => ({
  record_id, action, target_ids, merged_content,
});

const ev = (over: Partial<MemoryEvent> = {}): MemoryEvent => ({
  event_ts: "2026-03-01T10:00:00.000Z", session_key: "sk", session_id: "ses",
  team_id: "t1", agent_id: "a1", op: "created", record_id: "m_x", content: "v1", ...over,
});

describe("event ledger outbox", () => {
  let dir: string;
  let store: VectorStore;
  let storage: StorageAdapter;

  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), "ledger-"));
    store = new VectorStore(path.join(dir, "vectors.db"), 0);
    store.init();
    storage = new StorageAdapter(createLocalStorageBackend(path.join(dir, "data")));
  });

  afterEach(() => {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const outboxLines = async (date = "2026-03-01") =>
    ((await storage.readFile(StoragePaths.event(date))) ?? "").split("\n").filter(Boolean).map((l) => JSON.parse(l) as MemoryEvent);

  it("writes the same event_id to the outbox and the store", async () => {
    const r = await appendLedgerEvent({ store, storage, event: ev(), logger: silent });
    expect(r.jsonl).toBe(true);
    expect(r.store).toBe(true);
    const lines = await outboxLines();
    expect(lines).toHaveLength(1);
    expect(lines[0]!.event_id).toBe(r.event_id);
    expect(store.queryMemoryEvents({ record_id: "m_x" })[0]!.event_id).toBe(r.event_id);
  });

  it("store failure keeps the outbox line, marks the ledger degraded, and backfill recovers it", async () => {
    const failing = {
      appendMemoryEvent: () => { throw new Error("vdb down"); },
    } as unknown as IMemoryStore;
    const r = await appendLedgerEvent({ store: failing, storage, event: ev(), logger: silent });
    expect(r.jsonl).toBe(true);
    expect(r.store).toBe(false);
    const h = getLedgerHealth(failing);
    expect(h.degraded).toBe(true);
    expect(h.store_failures).toBe(1);

    const first = await replayLedgerEvents({ store, storage, logger: silent });
    expect(first).toMatchObject({ files: 1, scanned: 1, replayed: 1, malformed: 0, failed: 0 });
    // Replaying again is a no-op: event_id dedup.
    await replayLedgerEvents({ store, storage, logger: silent });
    const events = store.queryMemoryEvents({ record_id: "m_x" });
    expect(events).toHaveLength(1);
    expect(events[0]!.event_id).toBe(r.event_id);
  });

  it("outbox failure does not block the store write", async () => {
    const brokenStorage = { appendFile: async () => { throw new Error("cos 503"); } } as unknown as StorageAdapter;
    const r = await appendLedgerEvent({ store, storage: brokenStorage, event: ev(), logger: silent });
    expect(r.jsonl).toBe(false);
    expect(r.store).toBe(true);
    expect(getLedgerHealth(store).jsonl_failures).toBe(1);
    expect(getLedgerHealth(store).degraded).toBe(false);
  });

  it("backfill honours since and tenant scope, and counts malformed lines", async () => {
    await appendLedgerEvent({ store: undefined, storage, event: ev({ record_id: "m_old", event_ts: "2026-02-01T00:00:00.000Z" }), logger: silent });
    await appendLedgerEvent({ store: undefined, storage, event: ev({ record_id: "m_new" }), logger: silent });
    await appendLedgerEvent({ store: undefined, storage, event: ev({ record_id: "m_other", team_id: "t2" }), logger: silent });
    await storage.appendFile(StoragePaths.event("2026-03-01"), "{not json\n");

    const r = await replayLedgerEvents({ store, storage, since: "2026-03-01T00:00:00.000Z", scope: { team_id: "t1" }, logger: silent });
    expect(r).toMatchObject({ files: 1, replayed: 1, skipped: 1, malformed: 1 });
    expect(store.queryMemoryEvents({ limit: 10 }).map((e) => e.record_id)).toEqual(["m_new"]);
  });

  it("non-object outbox rows count as malformed without stopping later rows or shards", async () => {
    await storage.appendFile(StoragePaths.event("2026-03-01"), "null\n42\n\"str\"\n[1,2]\n");
    await appendLedgerEvent({ store: undefined, storage, event: ev({ record_id: "m_a" }), logger: silent });
    await appendLedgerEvent({ store: undefined, storage, event: ev({ record_id: "m_b", event_ts: "2026-03-02T00:00:00.000Z" }), logger: silent });

    const r = await replayLedgerEvents({ store, storage, logger: silent });
    expect(r).toMatchObject({ files: 2, scanned: 6, replayed: 2, malformed: 4, failed: 0 });
    expect(store.queryMemoryEvents({ limit: 10 }).map((e) => e.record_id)).toEqual(["m_a", "m_b"]);
  });

  it("rows with an unknown op or unparseable fields are malformed, not store failures", async () => {
    const good = ev({ event_id: "evt-good" });
    await storage.appendFile(StoragePaths.event("2026-03-01"), [
      JSON.stringify({ ...good, event_id: "evt-badop", op: "exploded" }),
      JSON.stringify({ ...good, event_id: "evt-badts", event_ts: "yesterday" }),
      JSON.stringify({ ...good, event_id: 7 }),
      JSON.stringify(good),
    ].join("\n") + "\n");
    const r = await replayLedgerEvents({ store, storage, logger: silent });
    expect(r).toMatchObject({ scanned: 4, replayed: 1, malformed: 3, failed: 0 });
  });

  it("a failed store redaction degrades the ledger until backfill re-applies the marker", async () => {
    await appendLedgerEvent({ store, storage, event: ev({ content: "secret", snapshot_json: "{\"content\":\"secret\"}" }), logger: silent });
    const realRedact = store.redactMemoryEvents.bind(store);
    store.redactMemoryEvents = () => { throw new Error("db locked"); };
    const filter = { team_id: "t1", agent_id: "a1", until: "2026-12-31T00:00:00.000Z" };
    await redactLedgerEvents({ store, storage, filter, logger: silent });
    const scope = { team_id: "t1", agent_id: "a1" };
    expect(getLedgerHealth(store, scope)).toMatchObject({ degraded: true, pending_redactions: 1 });
    expect(getLedgerHealth(store, { team_id: "t2", agent_id: "a1" })).toMatchObject({ degraded: false, pending_redactions: 0 });
    expect(store.queryMemoryEvents({ record_id: "m_x" })[0]!.content).toBe("secret");

    store.redactMemoryEvents = realRedact;
    const r = await replayLedgerEvents({ store, storage, scope, logger: silent });
    expect(r).toMatchObject({ redactions_applied: 1, failed: 0 });
    const [row] = store.queryMemoryEvents({ record_id: "m_x" });
    expect(row!.content).toBe("");
    expect(row!.snapshot_json).toBeUndefined();
    expect(getLedgerHealth(store, scope)).toMatchObject({ degraded: false, pending_redactions: 0 });
  });

  it("a failed unscoped (TTL) redaction clears per tenant as each tenant backfills", async () => {
    await appendLedgerEvent({ store, storage, event: ev({ content: "a" }), logger: silent });
    await appendLedgerEvent({ store, storage, event: ev({ team_id: "t2", record_id: "m_y", content: "b" }), logger: silent });
    const realRedact = store.redactMemoryEvents.bind(store);
    store.redactMemoryEvents = () => { throw new Error("db locked"); };
    await redactLedgerEvents({ store, storage, filter: { until: "2026-12-31T00:00:00.000Z" }, logger: silent });
    store.redactMemoryEvents = realRedact;
    const t1 = { team_id: "t1", agent_id: "a1" };
    const t2 = { team_id: "t2", agent_id: "a1" };
    expect(getLedgerHealth(store, t1).pending_redactions).toBe(1);
    expect(getLedgerHealth(store, t2).pending_redactions).toBe(1);

    await replayLedgerEvents({ store, storage, scope: t1, logger: silent });
    expect(getLedgerHealth(store, t1)).toMatchObject({ degraded: false, pending_redactions: 0 });
    expect(getLedgerHealth(store, t2).pending_redactions).toBe(1);
    expect(store.queryMemoryEvents({ record_id: "m_x" })[0]!.content).toBe("");
    expect(store.queryMemoryEvents({ record_id: "m_y" })[0]!.content).toBe("b");

    await replayLedgerEvents({ store, storage, logger: silent });
    expect(getLedgerHealth(store, t2).pending_redactions).toBe(0);
  });

  it("replay into a store that rejects appends keeps the event pending until a real write succeeds", async () => {
    const rejecting = {
      appendMemoryEvent: async () => { throw new Error("memory_events append rejected: store is degraded"); },
    } as unknown as IMemoryStore;
    await appendLedgerEvent({ store: rejecting, storage, event: ev(), logger: silent });
    expect(getLedgerHealth(rejecting, { team_id: "t1", agent_id: "a1" })).toMatchObject({ degraded: true, pending_store_events: 1 });

    const r = await replayLedgerEvents({ store: rejecting, storage, logger: silent });
    expect(r).toMatchObject({ replayed: 0, failed: 1 });
    expect(getLedgerHealth(rejecting, { team_id: "t1", agent_id: "a1" })).toMatchObject({ degraded: true, pending_store_events: 1 });
  });

  it("rebuilding a store from the outbox reproduces writeMemory's events exactly", async () => {
    const base = { sessionKey: "sk-x", sessionId: "ses-x", teamId: "t1", userId: "u1", agentId: "a1", baseDir: dir, vectorStore: store, storage };
    await writeMemory({ ...base, memory: memory("salary 5000"), decision: decision("m_a", "store") });
    await writeMemory({ ...base, memory: memory("salary 6000"), decision: decision("m_b", "update", ["m_a"], "salary 6000") });
    const original = store.queryMemoryEvents({ session_id: "ses-x", limit: 10 });
    expect(original.map((e) => e.op)).toEqual(["created", "superseded", "updated"]);

    const rebuilt = new VectorStore(path.join(dir, "rebuilt.db"), 0);
    rebuilt.init();
    try {
      const r = await replayLedgerEvents({ store: rebuilt, storage, logger: silent });
      expect(r.replayed).toBe(3);
      const replayed = rebuilt.queryMemoryEvents({ session_id: "ses-x", limit: 10 });
      expect(replayed.map((e) => [e.event_id, e.op, e.record_id])).toEqual(original.map((e) => [e.event_id, e.op, e.record_id]));
      expect(replayed[1]!.snapshot_json).toBe(original[1]!.snapshot_json);
    } finally {
      rebuilt.close();
    }
  });
});
