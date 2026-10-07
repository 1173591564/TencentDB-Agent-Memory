import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { MongoClientPool, buildMongoClientOptions } from "../src/core/store/mongodb/client-pool.js";
import { MongoMemoryStore } from "../src/core/store/mongodb/memory-store.js";
import { __setMemoryReviewEnabledForTests } from "../src/core/store/visibility.js";
import type { MemoryRecord } from "../src/core/record/l1-writer.js";
import { revertMemory } from "../src/core/record/memory-revert.js";

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
  const restoreIdentity = `rop-${"a".repeat(64)}`;
  const receipts = await Promise.all(Array.from({ length: 16 }, (_, i) =>
    (i % 2 ? first : second).setL1ReviewStatus("root", "active", iso, { operation_id: restoreIdentity })));
  for (const receipt of receipts) assert.deepEqual(receipt, receipts[0]);
  assert.equal((await first.queryMemoryEvents({ record_id: "root", operation_id: restoreIdentity })).length, 1);
  assert.equal(await first.countL1(iso), 2);
  await assert.rejects(second.setL1ReviewStatus("root", "quarantined", iso, { operation_id: restoreIdentity }), /different input/);
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
  await first.upsertL1(rec("revert-parent"));
  const preimage = (await first.queryL1Records({ ...iso, recordIds: ["revert-parent"], visibility: "all" }))[0]!;
  await second.upsertL1(rec("revert-child", ["revert-parent"]));
  await first.deleteL1("revert-parent", iso);
  const origin = { event_ts: new Date().toISOString(), session_key: "sk", session_id: "ses", team_id: "t1", user_id: "u1", agent_id: "a1", source: "extraction" as const, layer: "l1" as const };
  await first.appendMemoryEvent({ ...origin, record_id: "revert-parent", content: preimage.content, op: "superseded", superseded_by: "revert-child", snapshot_json: JSON.stringify(preimage) });
  await first.appendMemoryEvent({ ...origin, record_id: "revert-child", content: "kubernetes deployment revert-child", op: "updated", supersedes: ["revert-parent"] });
  const realCommit = first.commitMemoryEvent.bind(first);
  first.commitMemoryEvent = async (event) => { await realCommit(event); throw new Error("injected pre-COMMIT failure"); };
  const reverting = { recordId: "revert-child", options: { operationId: "mongo-atomic-revert" }, isolation: iso, logger: silent };
  const failed = await revertMemory({ ...reverting, store: first });
  assert.equal(failed.ok, false);
  assert.equal((await second.queryL1Records({ ...iso, recordIds: ["revert-parent", "revert-child"], visibility: "all" })).length, 1);
  assert.equal((await second.queryMemoryEvents({ record_id: "revert-child", op: "reverted" })).length, 0);
  first.commitMemoryEvent = realCommit;
  const reverted = await Promise.all([revertMemory({ ...reverting, store: first }), revertMemory({ ...reverting, store: second })]);
  for (const receipt of reverted) assert.equal(receipt.ok, true);
  assert.deepEqual(reverted[0], reverted[1]);
  assert.equal((await first.queryMemoryEvents({ record_id: "revert-child", op: "reverted" })).length, 1);
  assert.equal((await second.queryL1Records({ ...iso, recordIds: ["revert-parent", "revert-child"], visibility: "all" }))[0]?.record_id, "revert-parent");
  const epoch = await first.getClearEpoch(iso);
  const fence = { event_id: "evt-" + "c".repeat(32), event_ts: "2000-01-01T00:00:00.000Z", session_key: "", session_id: "", team_id: "t1", agent_id: "a1", record_id: "clear", content: "", op: "deleted" as const, source: "api_mutation" as const, scope: "agent" as const, layer: "l1" as const };
  await first.commitClearFence(fence);
  assert.equal(await second.getClearEpoch(iso), epoch + 1);
  await first.commitClearFence(fence);
  assert.equal(await second.getClearEpoch(iso), epoch + 1);
  await assert.rejects(second.upsertL1({ ...rec("skewed"), review_epoch: epoch, review_guard_at: "2099-01-01T00:00:00.000Z" }), /invalidated/);
  assert.equal(await second.upsertL1({ ...rec("fresh-epoch"), review_epoch: epoch + 1 }), true);
  assert.equal((await first.queryL1Records({ ...iso, recordIds: ["fresh-epoch"] })).length, 1);
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
