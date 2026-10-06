/**
 * memory_events 的 op CHECK 重建迁移（N03）。
 *
 * 为什么单独一个测试文件：这是整套审核机制里**风险最高**的一段代码。
 * SQLite 不能 ALTER 一个 CHECK 约束，所以新增 retracted/restored 必须
 * create-copy-drop-rename 重建整张表。重建一旦写错：
 *  - 漏列  ⇒ 静默丢掉 event_id / reason / target_event_id 等列的数据；
 *  - 漏索引 ⇒ event_id 的部分唯一索引没了，outbox 回放不再幂等，事件重复堆积；
 *  - 不重建 ⇒ 老库永久拒收审核事件（接口 200、物化列改了、账本写不进去）。
 *
 * 而这三种错误在**全新建库**的测试里全都发现不了 —— 新库直接拿到新 DDL。
 * 因此这里显式构造一个"老 schema + 有数据"的库再打开它。
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { VectorStore } from "./memory-store.js";

const OLD_DDL = `
  CREATE TABLE memory_events (
    seq                INTEGER PRIMARY KEY AUTOINCREMENT,
    event_ts           TEXT NOT NULL,
    session_key        TEXT NOT NULL DEFAULT '',
    session_id         TEXT NOT NULL DEFAULT '',
    origin_session_id  TEXT NOT NULL DEFAULT '',
    origin_session_key TEXT NOT NULL DEFAULT '',
    team_id            TEXT NOT NULL DEFAULT '',
    user_id            TEXT NOT NULL DEFAULT '',
    agent_id           TEXT NOT NULL DEFAULT '',
    task_id            TEXT NOT NULL DEFAULT '',
    op                 TEXT NOT NULL CHECK (op IN ('created','updated','merged','superseded','reverted','deleted')),
    record_id          TEXT NOT NULL,
    content            TEXT NOT NULL,
    memory_type        TEXT NOT NULL DEFAULT '',
    version            INTEGER NOT NULL DEFAULT 0,
    supersedes         TEXT NOT NULL DEFAULT '[]',
    superseded_by      TEXT NOT NULL DEFAULT '',
    snapshot_json      TEXT NOT NULL DEFAULT '',
    reviewer_id        TEXT NOT NULL DEFAULT '',
    layer              TEXT NOT NULL DEFAULT 'l1',
    source             TEXT NOT NULL DEFAULT '',
    request_id         TEXT NOT NULL DEFAULT '',
    event_id           TEXT NOT NULL DEFAULT '',
    reason             TEXT NOT NULL DEFAULT '',
    target_event_id    TEXT NOT NULL DEFAULT '',
    scope              TEXT NOT NULL DEFAULT '',
    until_ts           TEXT NOT NULL DEFAULT ''
  )`;

describe("memory_events op CHECK 重建迁移", () => {
  let dir: string;
  let dbPath: string;

  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), "mem-mig-"));
    dbPath = path.join(dir, "vectors.db");

    // 构造一个"PR 当前版本"的老库：有完整列，但 op CHECK 不含 retracted。
    const db = new DatabaseSync(dbPath);
    db.exec(OLD_DDL);
    db.exec("CREATE INDEX idx_memory_events_record ON memory_events(record_id)");
    db.exec("CREATE UNIQUE INDEX idx_memory_events_event_id ON memory_events(event_id) WHERE event_id != ''");
    db.prepare(`INSERT INTO memory_events
        (event_ts, session_key, session_id, team_id, user_id, agent_id, task_id, op, record_id, content,
         memory_type, version, supersedes, superseded_by, snapshot_json, reviewer_id, layer, source,
         request_id, event_id, reason, target_event_id, scope, until_ts)
      VALUES
        ('2026-01-01T00:00:00.000Z','sk','ses','t1','u1','a1','task','created','m_1','老数据内容',
         'work_fact', 7, '["m_0"]', 'm_2', '{"snap":1}', 'rev-1', 'l1', 'extraction',
         'req-1', 'evt-11111111111111111111111111111111', '旧理由', 'evt-22222222222222222222222222222222', 'sc', '2026-02-02T00:00:00.000Z')`).run();
    db.close();
  });

  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  it("老库打开后：CHECK 被重建，retracted/restored 可写入", () => {
    const store = new VectorStore(dbPath, 0);
    store.init();

    const sql = (store as unknown as { db: DatabaseSync }).db
      .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='memory_events'")
      .get() as { sql: string };
    expect(sql.sql).toContain("'retracted'");
    expect(sql.sql).toContain("'restored'");

    store.appendMemoryEvent!({
      event_ts: "2026-03-01T00:00:00.000Z", session_key: "sk", session_id: "ses",
      team_id: "t1", user_id: "u1", agent_id: "a1",
      op: "retracted", record_id: "m_1", content: "x",
      event_id: "evt-33333333333333333333333333333333",
    });
    const got = store.queryMemoryEvents!({ record_id: "m_1" });
    expect(got.some((e) => e.op === "retracted")).toBe(true);
    store.close?.();
  });

  it("重建**不得丢列**：每一个字段都要原样保留", () => {
    const store = new VectorStore(dbPath, 0);
    store.init();
    const row = (store as unknown as { db: DatabaseSync }).db
      .prepare("SELECT * FROM memory_events WHERE record_id = 'm_1'")
      .get() as Record<string, unknown>;

    expect(row.event_ts).toBe("2026-01-01T00:00:00.000Z");
    expect(row.content).toBe("老数据内容");
    expect(row.memory_type).toBe("work_fact");
    expect(row.version).toBe(7);
    expect(row.supersedes).toBe('["m_0"]');
    expect(row.superseded_by).toBe("m_2");
    expect(row.snapshot_json).toBe('{"snap":1}');
    expect(row.reviewer_id).toBe("rev-1");
    expect(row.source).toBe("extraction");
    expect(row.request_id).toBe("req-1");
    // 这几列正是旧迁移硬编码列清单里**缺失**的那些 —— 复用旧迁移会把它们清空
    expect(row.event_id).toBe("evt-11111111111111111111111111111111");
    expect(row.reason).toBe("旧理由");
    expect(row.target_event_id).toBe("evt-22222222222222222222222222222222");
    expect(row.scope).toBe("sc");
    expect(row.until_ts).toBe("2026-02-02T00:00:00.000Z");
    store.close?.();
  });

  it("重建**不得丢索引**：event_id 部分唯一索引必须还在，否则回放不再幂等", () => {
    const store = new VectorStore(dbPath, 0);
    store.init();
    const db = (store as unknown as { db: DatabaseSync }).db;

    const idx = db.prepare(
      "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='memory_events'",
    ).all() as Array<{ name: string; sql: string | null }>;
    const uniq = idx.find((i) => i.name === "idx_memory_events_event_id");
    expect(uniq).toBeDefined();
    expect(uniq!.sql).toContain("UNIQUE");
    expect(uniq!.sql).toContain("event_id != ''");

    // 行为验证：同 event_id 重复写入必须是幂等的（回放不产生重复事件）
    const before = (db.prepare("SELECT COUNT(*) AS c FROM memory_events").get() as { c: number }).c;
    const ev = {
      event_ts: "2026-03-01T00:00:00.000Z", session_key: "sk", session_id: "ses",
      team_id: "t1", user_id: "u1", agent_id: "a1",
      op: "restored" as const, record_id: "m_1", content: "y",
      event_id: "evt-44444444444444444444444444444444",
    };
    store.appendMemoryEvent!(ev);
    store.appendMemoryEvent!(ev);
    const after = (db.prepare("SELECT COUNT(*) AS c FROM memory_events").get() as { c: number }).c;
    expect(after).toBe(before + 1);
    store.close?.();
  });

  it("preserves sequence identities, allocation high-water mark and custom indexes", () => {
    const db = new DatabaseSync(dbPath);
    db.exec("UPDATE memory_events SET seq = 57");
    db.exec("UPDATE sqlite_sequence SET seq = 1000 WHERE name = 'memory_events'");
    db.exec("CREATE INDEX idx_review_custom ON memory_events(reason, seq)");
    db.exec("CREATE TABLE review_insert_audit (record_id TEXT)");
    db.exec("CREATE TRIGGER trg_review_insert AFTER INSERT ON memory_events BEGIN INSERT INTO review_insert_audit VALUES (NEW.record_id); END");
    db.close();

    const store = new VectorStore(dbPath, 0);
    store.init();
    const migrated = (store as unknown as { db: DatabaseSync }).db;
    expect(migrated.prepare("SELECT seq FROM memory_events WHERE record_id = 'm_1'").get()).toMatchObject({ seq: 57 });
    expect(migrated.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'memory_events'").get()).toMatchObject({ seq: 1000 });
    expect(migrated.prepare("SELECT sql FROM sqlite_master WHERE name = 'idx_review_custom'").get()).toMatchObject({ sql: "CREATE INDEX idx_review_custom ON memory_events(reason, seq)" });
    const trigger = migrated.prepare("SELECT name FROM sqlite_master WHERE name = 'trg_review_insert'").get();
    store.close();
    expect(trigger).toMatchObject({ name: "trg_review_insert" });
  });

  it("preserves allocation history even when the legacy ledger is empty", () => {
    const db = new DatabaseSync(dbPath);
    db.exec("DELETE FROM memory_events");
    db.exec("UPDATE sqlite_sequence SET seq = 1000 WHERE name = 'memory_events'");
    db.close();
    const store = new VectorStore(dbPath, 0);
    store.init();
    try {
      const migrated = (store as unknown as { db: DatabaseSync }).db;
      expect(migrated.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'memory_events'").get()).toMatchObject({ seq: 1000 });
    } finally {
      store.close();
    }
  });

  it("migrates the pre-deleted CHECK directly without losing existing source and layer", () => {
    const db = new DatabaseSync(dbPath);
    const rows = db.prepare("SELECT * FROM memory_events").all();
    db.exec("DROP TABLE memory_events");
    db.exec(OLD_DDL.replace(",\u0027deleted\u0027", ""));
    const keys = Object.keys(rows[0]!);
    db.prepare(`INSERT INTO memory_events (${keys.join(",")}) VALUES (${keys.map(() => "?").join(",")})`).run(...Object.values(rows[0]!));
    db.exec("UPDATE memory_events SET layer = 'l2', source = 'api_mutation', seq = 57");
    db.close();

    const store = new VectorStore(dbPath, 0);
    store.init();
    const migrated = (store as unknown as { db: DatabaseSync }).db;
    expect(migrated.prepare("SELECT seq, layer, source, event_id FROM memory_events").get()).toMatchObject({
      seq: 57, layer: "l2", source: "api_mutation", event_id: "evt-11111111111111111111111111111111",
    });
    store.close();
  });

  it("unknown columns abort migration and disable the store without dropping user data", () => {
    const db = new DatabaseSync(dbPath);
    db.exec("ALTER TABLE memory_events ADD COLUMN future_field TEXT DEFAULT 'preserved'");
    db.close();
    const store = new VectorStore(dbPath, 0);
    store.init();
    expect(store.isDegraded()).toBe(true);
    expect(() => store.queryMemoryEvents({})).toThrow(/degraded/);
    store.close();
    const verify = new DatabaseSync(dbPath);
    try {
      expect(verify.prepare("SELECT future_field, seq FROM memory_events").get()).toMatchObject({ future_field: "preserved", seq: 1 });
      expect(verify.prepare("SELECT name FROM sqlite_master WHERE name='memory_events_oprebuild'").get()).toBeUndefined();
    } finally { verify.close(); }
  });

  it("迁移是幂等的：再次打开不重复重建，数据行数不变", () => {
    const s1 = new VectorStore(dbPath, 0); s1.init();
    const c1 = (s1 as unknown as { db: DatabaseSync }).db
      .prepare("SELECT COUNT(*) AS c FROM memory_events").get() as { c: number };
    s1.close?.();

    const s2 = new VectorStore(dbPath, 0); s2.init();
    const db2 = (s2 as unknown as { db: DatabaseSync }).db;
    const c2 = db2.prepare("SELECT COUNT(*) AS c FROM memory_events").get() as { c: number };
    expect(c2.c).toBe(c1.c);
    // 重建残骸不得遗留
    const debris = db2.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'memory_events_%'",
    ).all() as Array<{ name: string }>;
    expect(debris).toHaveLength(0);
    s2.close?.();
  });
});
