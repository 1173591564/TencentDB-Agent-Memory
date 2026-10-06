import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { VectorStore } from "./sqlite/memory-store.js";
import { __setMemoryReviewEnabledForTests } from "./visibility.js";
import { derivedProfileAllowed, profileReviewFence, resolveReviewRows, setReviewStatus, ReviewConflictError } from "./review.js";
import type { MemoryRecord } from "../record/l1-writer.js";
import type { MemoryEvent } from "./types.js";
import { StorageAdapter, scopeProfileStorageView } from "../storage/adapter.js";
import { createLocalStorageBackend } from "../storage/factory.js";
import { appendLedgerEvent, redactLedgerEvents, replayLedgerEvents } from "../record/event-ledger.js";
import { clearChatMemoryContentResilient } from "../../gateway/chat-memory-handlers.js";
import { migrateReviewLedger, runMigrationCli, type MigrationTargetStore } from "../../../scripts/migrate-sqlite-to-tcvdb/sqlite-to-tcvdb.js";

const ISO = { teamId: "t1", userId: "u1", agentId: "a1" };
const op = (id: string) => `rop-${createHash("sha256").update(id).digest("hex")}`;
const record = (id: string, sources: string[] = []): MemoryRecord => ({
  id, content: `fact ${id}`, type: "work_fact", priority: 50, scene_name: "default", source_message_ids: [],
  metadata: {}, timestamps: [], createdAt: "2026-01-01T00:00:00.000Z", updatedAt: "2026-01-01T00:00:00.000Z",
  sessionKey: "sk", sessionId: "ses", ...ISO, review_sources: sources,
});

describe("committed observed-retraction review protocol", () => {
  let dir: string;
  let store: VectorStore;
  let storage: StorageAdapter;

  beforeEach(() => {
    __setMemoryReviewEnabledForTests(true);
    dir = mkdtempSync(path.join(tmpdir(), "review-protocol-"));
    store = new VectorStore(path.join(dir, "vectors.db"), 0);
    store.init();
    store.upsertL1(record("root"), undefined);
    storage = new StorageAdapter(createLocalStorageBackend(path.join(dir, "data")));
  });

  afterEach(() => {
    store.close();
    __setMemoryReviewEnabledForTests(undefined);
    rmSync(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });

  const all = () => store.queryL1Records({ ...ISO, visibility: "all" }, { strict: true });
  const state = (id = "root") => all().find((r) => r.record_id === id)?.review_status;

  it("commit failure changes no materialized state and emits no phantom review", () => {
    vi.spyOn(store, "appendMemoryEvent").mockImplementationOnce(() => { throw new Error("write failed"); });
    expect(() => store.setL1ReviewStatus("root", "quarantined", ISO)).toThrow("write failed");
    expect(state()).toBe("active");
    expect(store.queryMemoryEvents({ record_id: "root" })).toEqual([]);
  });

  it("foreign legacy empty owner fields stay inside the explicit default bucket", () => {
    const defaults = { teamId: "default", userId: "default", agentId: "default" };
    store.upsertL1({ ...record("legacy-default"), teamId: undefined, userId: undefined, agentId: undefined }, undefined);
    store.getRawDb().prepare("UPDATE l1_records SET team_id='', user_id='', agent_id='' WHERE record_id=?").run("legacy-default");
    expect(store.queryL1Records(defaults).map((r) => r.record_id)).toEqual(["legacy-default"]);
    expect(store.countL1({ ...defaults, visibility: "all" })).toBe(1);
    expect(store.queryL1Paginated({ ...defaults, visibility: "all", limit: 50, offset: 0 }).total).toBe(1);
    store.setL1ReviewStatus("legacy-default", "quarantined", defaults);
    expect(store.queryL1Records(defaults)).toEqual([]);
    expect(store.queryL1Records(ISO).map((r) => r.record_id)).toEqual(["root"]);
  });

  it("a committed review survives restart without an outbox or status-column write", () => {
    store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("retry"), reason: "incorrect" });
    expect(store.queryL1Records({ ...ISO }, { review: false })[0]?.review_status).toBe("active");
    store.close();
    store = new VectorStore(path.join(dir, "vectors.db"), 0);
    store.init();
    expect(state()).toBe("quarantined");
    const retry = store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("retry"), reason: "incorrect" });
    expect(retry?.changed).toBe(true);
    expect(store.queryMemoryEvents({ record_id: "root", op: "retracted" })).toHaveLength(1);
  });

  it("a timeout after server commit is recoverable with the original logical identity", () => {
    const append = store.appendMemoryEvent.bind(store);
    vi.spyOn(store, "appendMemoryEvent").mockImplementationOnce((e) => { append(e); throw new Error("response lost"); });
    expect(() => store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("lost") })).toThrow("response lost");
    expect(state()).toBe("quarantined");
    expect(store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("lost") })?.event).toBeDefined();
    expect(store.queryMemoryEvents({ record_id: "root", op: "retracted" })).toHaveLength(1);
  });

  it("reusing an identity with another action or reason is rejected", () => {
    store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("same"), reason: "first" });
    expect(() => store.setL1ReviewStatus("root", "active", ISO, { operation_id: op("same"), reason: "first" })).toThrow(ReviewConflictError);
    expect(() => store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("same"), reason: "second" })).toThrow(ReviewConflictError);
    expect(state()).toBe("quarantined");
  });

  it("an explicit no-op receipt remains a no-op after a later review", () => {
    store.setL1ReviewStatus("root", "active", ISO, { operation_id: op("noop"), persist_no_op: true });
    store.setL1ReviewStatus("root", "quarantined", ISO);
    expect(store.setL1ReviewStatus("root", "active", ISO, { operation_id: op("noop"), persist_no_op: true })?.changed).toBe(false);
    expect(state()).toBe("quarantined");
  });

  it("restore only cancels observed tokens, regardless of clock ordering", () => {
    const first = store.setL1ReviewStatus("root", "quarantined", ISO)!.event!;
    const restore: MemoryEvent = { ...first, event_id: "evt-" + "2".repeat(32), event_ts: "2001-01-01T00:00:00.000Z", op: "restored", review: { protocol: 1, observed: [first.event_id!] } };
    const second: MemoryEvent = { ...first, event_id: "evt-" + "3".repeat(32), event_ts: "2000-01-01T00:00:00.000Z" };
    store.appendMemoryEvent(second);
    store.appendMemoryEvent(restore);
    expect(state()).toBe("quarantined");
    expect(all()[0]?.review_tokens).toEqual([second.event_id]);
  });

  it("duplicate physical deliveries use one logical token and delayed delivery cannot undo restore", () => {
    const first = store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("logical") })!.event!;
    store.setL1ReviewStatus("root", "active", ISO);
    store.appendMemoryEvent({ ...first, event_id: "evt-" + "4".repeat(32), event_ts: "2099-01-01T00:00:00.000Z" });
    expect(state()).toBe("active");
  });

  it("concurrent deliveries of one restore identity cannot widen its observed cancellation", () => {
    const first = store.setL1ReviewStatus("root", "quarantined", ISO)!.event!;
    const restore = store.setL1ReviewStatus("root", "active", ISO, { operation_id: op("same-restore") })!.event!;
    const second = store.setL1ReviewStatus("root", "quarantined", ISO)!.event!;
    store.appendMemoryEvent({ ...restore, event_id: "evt-" + "9".repeat(32), review: { ...restore.review!, observed: [first.event_id!, second.event_id!] } });
    expect(state()).toBe("quarantined");
    expect(all()[0]?.review_tokens).toEqual([second.event_id]);
  });

  it("a no-op delivery cannot acquire future cancellation through a duplicate restore", () => {
    const receipt = store.setL1ReviewStatus("root", "active", ISO, { operation_id: op("same-noop"), persist_no_op: true })!.event!;
    const retract = store.setL1ReviewStatus("root", "quarantined", ISO)!.event!;
    store.appendMemoryEvent({ ...receipt, event_id: "evt-" + "a".repeat(32), review: { ...receipt.review!, no_op: false, observed: [retract.event_id!] } });
    expect(state()).toBe("quarantined");
  });

  it("conflicting concurrent deliveries fail closed instead of silently selecting a body", () => {
    const first = store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("collision") })!.event!;
    store.appendMemoryEvent({ ...first, event_id: "evt-" + "5".repeat(32), op: "restored", review: { protocol: 1, operation_id: op("collision"), request_hash: "b".repeat(64), observed: [op("collision")] } });
    expect(state()).toBe("quarantined");
    expect(all()[0]?.review_tokens?.some((t) => t.startsWith("conflict:"))).toBe(true);
    store.setL1ReviewStatus("root", "active", ISO, { operation_id: op("resolved") });
    expect(state()).toBe("active");
  });

  it("a retraction committed after merge propagates through a deleted source", () => {
    store.appendMemoryEvent({ event_ts: "2026-01-01T00:00:00.000Z", session_key: "sk", session_id: "ses", team_id: "t1", user_id: "u1", agent_id: "a1", record_id: "root", content: "original", op: "created", source: "extraction" });
    store.upsertL1(record("child", ["root"]), undefined);
    store.deleteL1("root", ISO);
    store.setL1ReviewStatus("root", "quarantined", ISO);
    expect(state("child")).toBe("quarantined");
    expect(store.queryL1Records(ISO)).toEqual([]);
    store.setL1ReviewStatus("root", "active", ISO);
    expect(state("child")).toBe("active");
    expect(all().some((r) => r.record_id === "root")).toBe(false);
  });

  it("lineage cycles converge and missing lineage is suppressed rather than assumed clean", () => {
    store.upsertL1(record("a", ["b"]), undefined);
    store.upsertL1(record("b", ["a"]), undefined);
    store.setL1ReviewStatus("a", "quarantined", ISO);
    expect(state("b")).toBe("quarantined");
    store.upsertL1(record("orphan", ["missing"]), undefined);
    expect(state("orphan")).toBe("quarantined");
    expect(all().find((r) => r.record_id === "orphan")?.review_incomplete).toBe(true);
    store.setL1ReviewStatus("orphan", "active", ISO);
    expect(all().find((r) => r.record_id === "orphan")?.review_incomplete).toBe(true);
  });

  it("clear invalidates a late generation on read and cannot be undone by restore", () => {
    expect(store.upsertL1({ ...record("late"), review_guard_at: "2026-01-01T10:00:00.000Z" }, undefined)).toBe(true);
    expect(store.upsertL1({ ...record("unknown-age"), createdAt: "", updatedAt: "" }, undefined)).toBe(true);
    store.appendMemoryEvent({ event_ts: "2026-01-01T11:00:00.000Z", session_key: "", session_id: "", team_id: "t1", agent_id: "a1", record_id: "agent-clear", op: "deleted", source: "api_mutation", scope: "agent", content: "" });
    expect(state("late")).toBe("quarantined");
    expect(state("unknown-age")).toBe("quarantined");
    expect(() => store.setL1ReviewStatus("late", "active", ISO)).toThrow(/invalidated|restored/);
    expect(store.upsertL1({ ...record("refused"), review_guard_at: "2026-01-01T10:00:00.000Z" }, undefined)).toBe(false);
    expect(all().some((r) => r.record_id === "refused")).toBe(false);
  });

  it("an expected-existing update cannot recreate a concurrently removed row", () => {
    expect(store.upsertL1({ ...record("gone"), expected_existing: true }, undefined)).toBe(false);
    expect(all().some((r) => r.record_id === "gone")).toBe(false);
  });

  it("a stale cross-instance restore cannot cancel a retraction committed while it waits", async () => {
    const initial = store.setL1ReviewStatus("root", "quarantined", ISO)!.event!;
    const peer = new VectorStore(path.join(dir, "vectors.db"), 0);
    peer.init();
    let ready!: () => void;
    let release!: () => void;
    const observed = new Promise<void>((r) => { ready = r; });
    const gate = new Promise<void>((r) => { release = r; });
    const append = store.appendMemoryEvent.bind(store);
    vi.spyOn(store, "appendMemoryEvent").mockImplementationOnce(async (event) => { ready(); await gate; append(event); });
    try {
      const restoring = setReviewStatus(store, "root", "active", ISO);
      await observed;
      const concurrent = { ...initial, event_id: "evt-" + "8".repeat(32), event_ts: "2000-01-01T00:00:00.000Z" };
      peer.appendMemoryEvent(concurrent);
      release();
      await restoring;
      expect(state()).toBe("quarantined");
      expect(all()[0]?.review_tokens).toEqual([concurrent.event_id]);
    } finally { release(); peer.close(); }
  });

  it("clear commits its durable fence before physical deletion and stops if commit fails", async () => {
    const logger = { info() {}, debug() {}, warn() {}, error() {} };
    const physical = store.clearMemoryContent.bind(store);
    const clear = vi.spyOn(store, "clearMemoryContent");
    vi.spyOn(store, "appendMemoryEvent").mockImplementationOnce(() => { throw new Error("durable fence failed"); });
    await expect(clearChatMemoryContentResilient({ store, storage, ...ISO, logger })).rejects.toThrow("durable fence failed");
    expect(clear).not.toHaveBeenCalled();
    expect(state()).toBe("active");
    clear.mockImplementation((filter) => {
      expect(store.queryMemoryEvents({ scope: "agent", layer: "l1", op: "deleted" })).toHaveLength(1);
      expect(store.queryL1Records(ISO)).toEqual([]);
      return physical(filter);
    });
    await clearChatMemoryContentResilient({ store, storage, ...ISO, logger });
    expect(all()).toEqual([]);
  });

  it("clear racing a generation commit suppresses the late row even after restart", () => {
    const guard = new Date(Date.now() - 1000).toISOString();
    const check = store.queryMemoryEvents.bind(store);
    vi.spyOn(store, "queryMemoryEvents").mockImplementationOnce((filter) => {
      const before = check(filter);
      store.appendMemoryEvent({ event_ts: new Date().toISOString(), session_key: "", session_id: "", team_id: "t1", agent_id: "a1", record_id: "clear", content: "", op: "deleted", scope: "agent", layer: "l1", source: "api_mutation" });
      return before;
    });
    expect(store.upsertL1({ ...record("late"), review_guard_at: guard }, undefined)).toBe(true);
    store.close();
    store = new VectorStore(path.join(dir, "vectors.db"), 0);
    store.init();
    expect(store.queryL1Records({ ...ISO, recordIds: ["late"] })).toEqual([]);
    expect(all().find((r) => r.record_id === "late")?.review_tokens?.[0]).toMatch(/^clear:/);
  });

  it("migration preserves raw legacy state and all review facts, including deleted ancestors", async () => {
    store.appendMemoryEvent({ event_ts: "2026-01-01T00:00:00.000Z", session_key: "sk", session_id: "ses", team_id: "t1", user_id: "u1", agent_id: "a1", record_id: "root", content: "original", op: "created", source: "extraction" });
    store.upsertL1(record("child", ["root"]), undefined);
    store.setL1ReviewStatus("root", "quarantined", ISO, { operation_id: op("migration") });
    store.deleteL1("root", ISO);
    const target = new VectorStore(path.join(dir, "migrated.db"), 0);
    target.init();
    try {
      const raw = store.queryL1RecordsCursor("", 50, { review: false });
      for (const row of raw) target.upsertL1({ ...record(row.record_id, JSON.parse(row.review_sources_json ?? "[]") as string[]), review_status: row.review_status }, undefined);
      expect(await migrateReviewLedger(store, target)).toBe(2);
      expect(target.queryL1Records(ISO)).toEqual([]);
      target.setL1ReviewStatus("root", "active", ISO);
      expect(target.queryL1Records(ISO).map((r) => r.record_id)).toEqual(["child"]);
      await expect(migrateReviewLedger(store, { ...target, appendMemoryEvent: undefined } as never)).rejects.toThrow("preserve the review ledger");
    } finally { target.close(); }
  });

  it("the actual migration CLI verifies physical counts and keeps ancestor review controls", async () => {
    store.upsertL1(record("child", ["root"]), undefined);
    store.setL1ReviewStatus("root", "quarantined", ISO);
    store.deleteL1("root", ISO);
    const destinationPath = path.join(dir, "cli-target.db");
    writeFileSync(path.join(dir, "unused-config.json"), "{}");
    const createTargetStore = (): MigrationTargetStore => {
      const destination = new VectorStore(destinationPath, 0);
      return {
        init: () => destination.init(), close: () => destination.close(), isDegraded: () => destination.isDegraded(),
        countL0: () => destination.countL0(), countL1: (filter) => destination.countL1(filter),
        upsertL0: destination.upsertL0.bind(destination), upsertL1: destination.upsertL1.bind(destination),
        appendMemoryEvent: destination.appendMemoryEvent.bind(destination),
      };
    };
    const summary = await runMigrationCli([
      "--plugin-data-dir", dir, "--sqlite-path", path.join(dir, "vectors.db"),
      "--openclaw-config-path", path.join(dir, "unused-config.json"),
      "--tcvdb-url", "http://fixture", "--tcvdb-username", "fixture", "--tcvdb-api-key", "fixture",
      "--tcvdb-database", "fixture", "--tcvdb-embedding-model", "none",
      "--no-apply-config", "--no-rewrite-manifest", "--yes",
    ], { createTargetStore, verifyDelayMs: 0 });
    expect(summary.migration).toMatchObject({ l1Migrated: 1, eventsMigrated: 1, targetL1Count: 1, configWritten: false });
    const reopened = new VectorStore(destinationPath, 0);
    reopened.init();
    try {
      expect(reopened.queryL1Records(ISO)).toEqual([]);
      reopened.setL1ReviewStatus("root", "active", ISO);
      expect(reopened.queryL1Records(ISO).map((r) => r.record_id)).toEqual(["child"]);
    } finally { reopened.close(); }
  });

  it("async and sync resolvers produce the same state", async () => {
    store.setL1ReviewStatus("root", "quarantined", ISO);
    const raw = store.queryL1Records({ ...ISO, visibility: "all" }, { strict: true, review: false });
    expect(await resolveReviewRows(store, raw)).toEqual(all());
  });

  it("outbox replay changes effective visibility without a second status projection", async () => {
    const first = store.setL1ReviewStatus("root", "quarantined", ISO)!.event!;
    await appendLedgerEvent({ store, storage, event: first, storeAlreadyCommitted: true });
    const other = new VectorStore(path.join(dir, "other.db"), 0);
    other.init();
    try {
      other.upsertL1(record("root"), undefined);
      const result = await replayLedgerEvents({ store: other, storage });
      expect(result.failed).toBe(0);
      expect(other.queryL1Records(ISO)).toEqual([]);
      await replayLedgerEvents({ store: other, storage });
      expect(other.queryMemoryEvents({ record_id: "root" })).toHaveLength(1);
    } finally { other.close(); }
  });

  it("redaction removes reason from both durable legs but preserves review identity", async () => {
    const first = store.setL1ReviewStatus("root", "quarantined", ISO, { reason: "private-review-reason" })!.event!;
    await appendLedgerEvent({ store, storage, event: first, storeAlreadyCommitted: true });
    await redactLedgerEvents({ store, storage, filter: { team_id: "t1", agent_id: "a1", until: "2099-01-01T00:00:00.000Z" } });
    expect(store.queryMemoryEvents({ record_id: "root" })[0]?.reason).toBeUndefined();
    const shards = await storage.readdirNames("events/", ".jsonl");
    for (const name of shards) expect(await storage.readFile(`events/${name}`)).not.toContain("private-review-reason");
    await replayLedgerEvents({ store, storage });
    expect(state()).toBe("quarantined");
  });

  it("all profile read forms are fenced until current content is acknowledged", async () => {
    const scoped = scopeProfileStorageView(storage.withReviewStore(() => store), "profiles/test/", ISO);
    await scoped.writeFile("persona.md", "derived stale fact");
    expect(await scoped.readFile("persona.md")).toBe("derived stale fact");
    store.setL1ReviewStatus("root", "quarantined", ISO);
    expect(await scoped.readFile("persona.md")).toBeNull();
    expect(await scoped.readFileBuffer("persona.md")).toBeNull();
    const content = await scoped.readFile("persona.md", { review: false });
    const fence = await profileReviewFence(store, ISO);
    store.appendMemoryEvent({ event_ts: new Date().toISOString(), session_key: "", session_id: "", team_id: "t1", user_id: "u1", agent_id: "a1", op: "updated", source: "review", layer: "l3", record_id: "persona.md", content: "", review: { protocol: 1, fence_hash: createHash("sha256").update(JSON.stringify([...fence].sort())).digest("hex"), content_hash: createHash("sha256").update(content!).digest("hex") } });
    expect(await scoped.readFile("persona.md")).toBe(content);
    await scoped.writeFile("persona.md", "changed after acknowledgement");
    expect(await scoped.readFile("persona.md")).toBeNull();
    expect(await derivedProfileAllowed(store, "persona.md", content!, fence, ISO)).toBe(true);
    expect(state()).toBe("quarantined");
  });

  it("clear fences a late profile even if no memory was ever manually retracted", async () => {
    const scoped = scopeProfileStorageView(storage.withReviewStore(() => store), "profiles/test/", ISO);
    store.appendMemoryEvent({ event_ts: new Date().toISOString(), session_key: "", session_id: "", team_id: "t1", agent_id: "a1", record_id: "clear", content: "", op: "deleted", scope: "agent", layer: "l1", source: "api_mutation" });
    await scoped.writeFile("persona.md", "late derived bytes");
    expect(await scoped.readFile("persona.md")).toBeNull();
  });

  it("auto-recall cannot fall back to raw profile files when its ledger is unavailable and writes are disabled", async () => {
    const { performAutoRecall } = await import("../hooks/auto-recall.js");
    __setMemoryReviewEnabledForTests(false);
    const scoped = scopeProfileStorageView(storage, `profiles/${encodeURIComponent("team:t1|agent:a1")}/`, ISO);
    await scoped.writeFile("persona.md", "unverified fallback profile");
    const result = await performAutoRecall({ userText: "", actorId: "u1", sessionKey: "sk", cfg: { recall: { timeoutMs: 1000 } } as never, pluginDataDir: dir, storage, profileIsolation: ISO });
    expect(result).toMatchObject({ recalledL3Persona: null, prependContext: "", appendSystemContext: "", error: { code: 20002 }, partial: false });
  });

  it("a long-lived storage view cannot cache a clean fence across later retraction", async () => {
    const scoped = scopeProfileStorageView(storage.withReviewStore(() => store), "profiles/test/", ISO);
    await scoped.writeFile("persona.md", "initial profile");
    expect(await scoped.readFile("persona.md")).toBe("initial profile");
    store.setL1ReviewStatus("root", "quarantined", ISO);
    __setMemoryReviewEnabledForTests(false);
    expect(await scoped.readFile("persona.md")).toBeNull();
  });
});
