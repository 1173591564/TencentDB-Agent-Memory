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
import { appendLedgerEvent, getLedgerHealth, replayLedgerEvents } from "./event-ledger.js";
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
