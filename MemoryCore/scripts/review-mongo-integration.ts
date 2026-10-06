import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { MongoClientPool, buildMongoClientOptions } from "../src/core/store/mongodb/client-pool.js";
import { MongoMemoryStore } from "../src/core/store/mongodb/memory-store.js";
import { __setMemoryReviewEnabledForTests } from "../src/core/store/visibility.js";
import type { MemoryRecord } from "../src/core/record/l1-writer.js";

const config = {
  endpoint: "mongodb://127.0.0.1:27139/?directConnection=true",
  user: "", password: "", database: `review_integration_${randomUUID().replaceAll("-", "")}`,
};
const silent = { info() {}, debug() {}, warn() {}, error() {} };
const pool = new MongoClientPool(silent);
const first = new MongoMemoryStore({ pool, mongoConfig: config, logger: silent, searchIndexWaitMs: 30_000 });
const otherPool = new MongoClientPool(silent);
const second = new MongoMemoryStore({ pool: otherPool, mongoConfig: config, logger: silent, searchIndexWaitMs: 30_000 });
const iso = { teamId: "t1", userId: "u1", agentId: "a1" };
const rec = (id: string, review_sources: string[] = []): MemoryRecord => ({
  id, content: `kubernetes deployment ${id}`, type: "work_fact", priority: 50, scene_name: "default",
  source_message_ids: [], metadata: {}, timestamps: [], createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
  sessionKey: "sk", sessionId: "ses", ...iso, review_sources,
});

try {
  __setMemoryReviewEnabledForTests(true);
  assert.equal((await pool.getClusterProfile(config)).mongot, true);
  assert.equal(buildMongoClientOptions(config).writeConcern?.w, "majority");
  assert.equal(buildMongoClientOptions(config).readPreference, "primary");
  await first.init();
  await second.init();
  await first.upsertL1(rec("root"));
  await first.upsertL1(rec("clean"));
  assert.equal(await first.refreshSearchIndexReady(30_000), true);
  let found = false;
  for (let attempt = 0; attempt < 60; attempt++) {
    if ((await first.searchL1Fts("kubernetes", 10, iso)).some((r) => r.record_id === "root")) { found = true; break; }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  assert.equal(found, true, "search index did not ingest fixture");
  await first.setL1ReviewStatus("root", "quarantined", iso);
  assert.equal((await second.queryL1Records(iso)).some((r) => r.record_id === "root"), false);
  assert.equal((await second.searchL1Fts("kubernetes", 10, iso)).some((r) => r.record_id === "root"), false, "index lag leaked quarantined content");
  assert.equal(await second.countL1(iso), 1);
  assert.equal((await second.queryL1Paginated({ ...iso, visibility: "quarantined", limit: 10, offset: 0 })).rows[0]?.record_id, "root");
  assert.equal((await second.setL1ReviewStatus("root", "active", { ...iso, userId: "other" })), undefined);
  await second.setL1ReviewStatus("root", "active", iso);
  assert.equal(await first.countL1(iso), 2);
  await first.upsertL1(rec("child", ["root"]));
  await second.setL1ReviewStatus("root", "quarantined", iso);
  assert.equal((await first.queryL1Records({ ...iso, recordIds: ["child"] })).length, 0, "lineage retraction did not propagate across instances");
  await first.upsertL1({ ...rec("root"), content: "kubernetes updated payload" });
  assert.equal((await first.queryL1Records({ ...iso, recordIds: ["root"] })).length, 0, "ordinary upsert reset review state");
  const defaults = { teamId: "default", userId: "default", agentId: "default" };
  assert.equal(await first.upsertL1({ ...rec("default-bucket"), teamId: undefined, userId: undefined, agentId: undefined }), true);
  assert.equal((await second.queryL1Records(defaults)).length, 1);
  await second.setL1ReviewStatus("default-bucket", "quarantined", defaults);
  assert.equal((await first.queryL1Records(defaults)).length, 0);
  __setMemoryReviewEnabledForTests(false);
  assert.equal((await first.queryL1Records({ ...iso, recordIds: ["root"] })).length, 0, "disabled writes resurrected reviewed data");
  console.log("MongoDB 8.3 + mongot: primary/majority durability, cross-instance review, search-index lag protection, count/pagination, isolation and lineage checks passed.");
} finally {
  first.close();
  second.close();
  await pool.closeAll();
  await otherPool.closeAll();
  __setMemoryReviewEnabledForTests(undefined);
}
