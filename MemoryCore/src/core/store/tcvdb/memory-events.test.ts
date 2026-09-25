/**
 * TCVDB memory_events 在降级（init 失败）时必须拒绝写入，
 * 让账本 wrapper 保留 pending，直到真正写入成功。
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { appendLedgerEvent, getLedgerHealth, replayLedgerEvents } from "../../record/event-ledger.js";
import { StorageAdapter } from "../../storage/adapter.js";
import { createLocalStorageBackend } from "../../storage/factory.js";
import type { MemoryEvent } from "../types.js";
import { TcvdbMemoryStore } from "./memory-store.js";

const silent = { warn() {}, debug() {}, info() {}, error() {} };

const ev = (over: Partial<MemoryEvent> = {}): MemoryEvent => ({
  event_ts: "2026-03-01T10:00:00.000Z", session_key: "sk", session_id: "ses",
  team_id: "t1", agent_id: "a1", op: "created", record_id: "m_x", content: "v1", ...over,
});

describe("tcvdb memory_events when degraded", () => {
  let dir: string;
  let storage: StorageAdapter;
  let store: TcvdbMemoryStore;

  beforeEach(async () => {
    dir = mkdtempSync(path.join(tmpdir(), "tcvdb-ledger-"));
    storage = new StorageAdapter(createLocalStorageBackend(path.join(dir, "data")));
    store = new TcvdbMemoryStore({
      url: "http://127.0.0.1:1", username: "root", apiKey: "k", database: "db",
      embeddingModel: "none", timeout: 500, logger: silent,
    });
    await store.init();
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("rejects appends so a failed write and a replay both keep the event pending", async () => {
    await expect(store.appendMemoryEvent(ev({ event_id: "evt-direct" }))).rejects.toThrow(/degraded/);

    const r = await appendLedgerEvent({ store, storage, event: ev(), logger: silent });
    expect(r).toMatchObject({ jsonl: true, store: false });
    const scope = { team_id: "t1", agent_id: "a1" };
    expect(getLedgerHealth(store, scope)).toMatchObject({ degraded: true, pending_store_events: 1 });

    const replay = await replayLedgerEvents({ store, storage, logger: silent });
    expect(replay).toMatchObject({ replayed: 0, failed: 1 });
    expect(getLedgerHealth(store, scope)).toMatchObject({ degraded: true, pending_store_events: 1 });
  });

  it("rejects queries instead of reporting an empty ledger", async () => {
    await expect(store.queryMemoryEvents({ record_id: "m_x" })).rejects.toThrow(/degraded/);
  });
});
