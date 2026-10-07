/**
 * L1 Memory Writer: writes extracted memories to JSONL files.
 *
 * File naming: records/YYYY-MM-DD.jsonl (daily shards, all sessions merged).
 * Each record includes sessionKey for traceability.
 *
 * Write strategy:
 * - JSONL is the append-only persistent store (source of truth for backup/recovery).
 * - VectorStore (SQLite) is the primary retrieval engine.
 * - On update/merge, old records are deleted from VectorStore in real-time;
 *   JSONL is append-only and cleaned up periodically by memory-cleaner.
 *
 * Supports store (append), update, merge, and skip operations.
 *
 * v3: Aligned with Kenty's prompt output format — 3 memory types (persona/episodic/instruction),
 * numeric priority, scene_name, source_message_ids, metadata, timestamps.
 */

import crypto from "node:crypto";
import { assertClearGuard } from "../store/review.js";
import { DEFAULT_ISOLATION_ID, type IMemoryStore } from "../store/types.js";
import type { EmbeddingService } from "../store/embedding.js";
import type { StorageAdapter } from "../storage/adapter.js";
import { StoragePaths } from "../storage/types.js";
import { appendLedgerEvent } from "./event-ledger.js";
import type { Logger } from "../types.js";

// ============================
// Types
// ============================

/** L1 memory types: chat-mode legacy types + code/work-mode team memory types. */
export type MemoryType =
  | "persona"
  | "episodic"
  | "instruction"
  | "work_fact"
  | "work_task"
  | "work_method"
  | "work_artifact";

/** Metadata for episodic memories (activity time range) */
export interface EpisodicMetadata {
  activity_start_time?: string; // ISO 8601
  activity_end_time?: string; // ISO 8601
}

/**
 * A persisted memory record in L1 JSONL files.
 *
 * v3 changes from v2:
 * - `importance: "high"|"medium"|"low"` → `priority: number` (0-100, -1 for strict global instructions)
 * - Added `scene_name`, `source_message_ids`, `metadata`, `timestamps`
 * - Removed `keywords` (will be rebuilt from content for search)
 * - MemoryType reduced from 4 to 3 (removed "preference", folded into "persona")
 */
export interface MemoryRecord {
  /** Unique ID for dedup updates */
  id: string;
  /** Memory content */
  content: string;
  /** Memory type: persona / episodic / instruction */
  type: MemoryType;
  /** Priority score: 0-100 (higher = more important), -1 = strict global instruction */
  priority: number;
  /** Scene name this memory belongs to */
  scene_name: string;
  /** Source message IDs that contributed to this memory */
  source_message_ids: string[];
  /** Type-specific metadata (e.g., activity_start_time for episodic) */
  metadata: EpisodicMetadata | Record<string, never>;
  /** Timestamp trail: all timestamps related to this memory (for merge history tracking) */
  timestamps: string[];
  /** Creation timestamp (ISO) */
  createdAt: string;
  /** Last update timestamp (ISO) */
  updatedAt: string;
  /** Monotonic version. New memories start at 0; update/merge = max(target versions)+1. */
  version?: number;
  review_sources?: string[];
  review_guard_at?: string;
  review_epoch?: number;
  expected_existing?: boolean;
  review_status?: import("../store/visibility.js").ReviewStatus;
  /** Source session key (conversation channel identifier) */
  sessionKey: string;
  /** Source session ID (single conversation instance identifier) */
  sessionId: string;
  /** Optional task dimension for L0/L1 filtering. */
  taskId?: string;
  /**
   * Three-dim tenancy isolation (new in this branch).
   *
   * `userId` / `agentId` are mandatory for new writes once gateway-level
   * isolation enforcement is on, but kept optional on the type to avoid
   * breaking pre-isolation call sites and tests during rollout. The SQLite
   * upsert defaults them to '' if missing; the migration script backfills
   * existing rows with `__legacy__`.
   *
   * See `docs/l0l3-tenant-isolation-design.md`.
   */
  teamId?: string;
  userId?: string;
  agentId?: string;
}

/**
 * A memory as extracted by LLM (before dedup / persistence).
 * Matches the output format of Kenty's extraction prompt.
 */
export interface ExtractedMemory {
  content: string;
  type: MemoryType;
  priority: number;
  source_message_ids: string[];
  metadata: EpisodicMetadata | Record<string, never>;
  /** Scene name this memory was extracted in */
  scene_name: string;
}

export type DedupAction = "store" | "update" | "merge" | "skip";

/**
 * v3 batch dedup decision — one per new memory, aligned with Kenty's conflict detection prompt.
 *
 * Key changes:
 * - `targetId` → `target_ids` (array, supports multi-target merge/update)
 * - Added `merged_type`, `merged_priority`, `merged_timestamps` for cross-type merge
 */
export interface DedupDecision {
  /** Which new memory this decision is about */
  record_id: string;
  action: DedupAction;
  /** IDs of existing records to replace/remove (for update/merge) */
  target_ids: string[];
  /** Merged/updated content text (for update/merge) */
  merged_content?: string;
  /** Best type after merge (for update/merge, may differ from original) */
  merged_type?: MemoryType;
  /** Priority after merge (for update/merge) */
  merged_priority?: number;
  /** Union of all related timestamps (for update/merge) */
  merged_timestamps?: string[];
}

const TAG = "[memory-tdai][l1-writer]";

// ============================
// Core functions
// ============================

/**
 * Generate a unique memory ID.
 */
export function generateMemoryId(): string {
  return `m_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
}

// ── Revert tombstones (JSONL replay protection) ─────────────────────────────
//
// JSONL is the declared source of truth for backup/recovery, but the review
// revert path only deletes from the vector store — a later replay/recovery
// from JSONL would silently resurrect reverted records. A tombstone line is
// appended to the day's shard on revert; readers (l1-reader) collect
// tombstoned ids first and skip those records. Tombstones are shaped nothing
// like MemoryRecord (no sessionKey/content fields), so legacy readers that
// don't know them simply drop the line.

/** Shape of a JSONL tombstone line appended by the review revert path. */
export interface L1RevertTombstone {
  /** Marker + layer tag; lets future L0/L3 tombstones reuse the mechanism. */
  tombstone: "l1";
  /** The reverted (deleted) record — replay must skip this id. */
  record_id: string;
  /** When the revert landed (ISO 8601). */
  reverted_at: string;
  /** Who rejected the change (v3 isolation user), when known. */
  reviewer_id?: string;
}

export function buildRevertTombstoneLine(
  recordId: string,
  reviewerId?: string,
  at: string = new Date().toISOString(),
): string {
  const tombstone: L1RevertTombstone = {
    tombstone: "l1",
    record_id: recordId,
    reverted_at: at,
    ...(reviewerId ? { reviewer_id: reviewerId } : {}),
  };
  return JSON.stringify(tombstone);
}

/**
 * Append a revert tombstone to today's JSONL shard (best-effort).
 *
 * The gateway has no local baseDir, so no fs fallback is attempted here —
 * when no StorageAdapter is available the tombstone is skipped with a warn.
 * The `reverted` event in the store remains the authoritative audit trail;
 * the JSONL tombstone only protects replay/recovery from resurrection.
 */
export async function appendRevertTombstone(params: {
  recordId: string;
  reviewerId?: string;
  storage?: StorageAdapter;
  logger?: Logger;
}): Promise<boolean> {
  const { recordId, reviewerId, storage, logger } = params;
  if (!storage) {
    logger?.warn?.(`${TAG} revert tombstone skipped: no storage adapter (replay protection relies on store events only)`);
    return false;
  }
  const shardDate = formatLocalDate(new Date());
  try {
    await storage.appendFile(StoragePaths.record(shardDate), buildRevertTombstoneLine(recordId, reviewerId) + "\n");
    return true;
  } catch (err) {
    logger?.warn?.(
      `${TAG} revert tombstone append failed (non-fatal): ${err instanceof Error ? err.message : String(err)}`,
    );
    return false;
  }
}

/**
 * Write a memory record according to the dedup decision.
 *
 * - store: append new record
 * - update: remove target records + append updated record
 * - merge: remove target records + append merged record
 * - skip: do nothing
 *
 * v3: supports multi-target removal for update/merge.
 * v3.1: optional VectorStore + EmbeddingService for dual-write (JSONL + vector).
 */
export async function writeMemory(params: {
  memory: ExtractedMemory;
  decision: DedupDecision;
  baseDir: string;
  sessionKey: string;
  sessionId?: string;
  taskId?: string;
  /** Tenancy isolation propagated into MemoryRecord and downstream store. */
  teamId?: string;
  userId?: string;
  agentId?: string;
  logger?: Logger;
  /** Optional vector store for dual-write (JSONL + vector DB) */
  vectorStore?: IMemoryStore;
  /** Optional embedding service (required when vectorStore is provided) */
  embeddingService?: EmbeddingService;
  /** StorageAdapter for file operations (COS/local). Falls back to fs when absent. */
  storage?: StorageAdapter;
  startedAt?: string;
  reviewEpoch?: number;
}): Promise<MemoryRecord | null> {
  const { memory, decision, baseDir, sessionKey, sessionId, taskId, teamId, userId, agentId, logger, vectorStore, embeddingService, storage } = params;

  if (decision.action === "skip") {
    logger?.debug?.(`${TAG} Skipping memory contentLen=${memory.content.length}`);
    return null;
  }

  const now = new Date().toISOString();
  const reviewEpoch = params.reviewEpoch ?? (vectorStore?.getClearEpoch ? await vectorStore.getClearEpoch({ teamId, agentId }) : undefined);
  if (params.startedAt && params.reviewEpoch === undefined && (reviewEpoch ?? 0) > 0) throw new Error("A generation started before its epoch was captured; retry extraction");

  let nextVersion = 0;
  // Superseded targets snapshot — reused for memory_events `superseded` rows so
  // the diff can show old content. Empty when the query fails or there are no
  // targets (store action): superseded events are best-effort, the authoritative
  // write path (JSONL + vector upsert) is unaffected either way.
  //
  // `queryL1Records` honors `recordIds` on all backends (sqlite PK-IN lookup,
  // MongoDB $in, TCVDB documentIds), so `existing` is already narrowed to the
  // targeted lineage — the same rows drive both the superseded snapshots and
  // the next-version computation.
  let supersededTargets: Awaited<ReturnType<NonNullable<typeof vectorStore>["queryL1Records"]>> = [];
  /** The scoped target read succeeded — `supersededTargets` is then the authoritative target set. */
  let targetsQueried = false;
  if ((decision.action === "update" || decision.action === "merge") && decision.target_ids.length > 0 && vectorStore) {
    try {
      // Scope the snapshot read to the same tenant filter used for the delete:
      // a hallucinated/out-of-scope target_id must not leak a foreign record's
      // content into a superseded event (which is stored under OUR tenancy and
      // readable via /memory/diff).
      supersededTargets = await vectorStore.queryL1Records({
        recordIds: decision.target_ids,
        ...(teamId || userId || agentId || taskId
          ? { teamId, userId, agentId, taskId }
          : sessionId ? { sessionId } : {}),
        // DP-14：必须看得见被撤回的目标。
        // 若这里走默认 active 口径，被撤回的目标查不到 ⇒ 代码当作"目标不存在"
        // 继续往下写 ⇒ 同一事实以新 record_id 落库 ⇒ 被撤回的内容复活。
        visibility: "all",
      }, { strict: true });
      targetsQueried = true;

      // 被撤回的记忆**不得通过合并复活**（DP-14）。
      // 已知隔离源在模型决策落地前拦截；读后发生的撤回由持久来源图继续抑制。
      const quarantinedTargets = supersededTargets.filter(
        (r) => r.review_status === "quarantined",
      );
      if (quarantinedTargets.length > 0) {
        // 不打 content。这条日志描述的正是一条**刚被撤回**的记忆——
        // 撤回的常见理由就是内容错误或敏感，再把它抄进应用日志等于把撤回漏回去：
        // 日志聚合的访问面和留存期通常都比记忆库更宽。
        // 定位用 record_id 足够（要正文就去 /memory/history，那是受控的审计面）。
        logger?.warn?.(
          `${TAG} [memory-review] 丢弃一次写入：去重目标 ` +
          `[${quarantinedTargets.map((r) => r.record_id).join(", ")}] 已被撤回，` +
          `不得通过合并复活。新内容长度=${memory.content.length}`,
        );
        return null;
      }
      const maxVersion = supersededTargets.reduce((max, row) => Math.max(max, row.version ?? 0), 0);
      nextVersion = maxVersion + 1;
    } catch (err) {
      logger?.warn?.(`${TAG} Failed to read existing memory version: ${err instanceof Error ? err.message : String(err)}`);
      return null;
    }
  }

  // Determine final content, type, priority based on action
  let finalContent: string;
  let finalType: MemoryType;
  let finalPriority: number;
  let finalTimestamps: string[];

  if (decision.action === "merge" || decision.action === "update") {
    finalContent = decision.merged_content ?? memory.content;
    finalType = decision.merged_type ?? memory.type;
    finalPriority = decision.merged_priority ?? memory.priority;
    finalTimestamps = decision.merged_timestamps ?? [now];
  } else {
    // store
    finalContent = memory.content;
    finalType = memory.type;
    finalPriority = memory.priority;
    finalTimestamps = [now];
  }

  const record: MemoryRecord = {
    id: decision.record_id || generateMemoryId(),
    content: finalContent,
    type: finalType,
    priority: finalPriority,
    scene_name: memory.scene_name,
    source_message_ids: memory.source_message_ids,
    metadata: memory.metadata,
    timestamps: finalTimestamps,
    createdAt: now,
    updatedAt: now,
    version: nextVersion,
    review_sources: [...new Set(supersededTargets.map((r) => r.record_id))],
    review_guard_at: params.startedAt ?? now,
    review_epoch: reviewEpoch,
    sessionKey,
    sessionId: sessionId || DEFAULT_ISOLATION_ID,
    taskId,
    teamId,
    // Tenancy isolation — propagated end-to-end so SQLite / TCVDB upsert
    // can persist the row's owner. Empty strings preserve pre-isolation
    // behaviour for callers that haven't been updated yet.
    userId: userId || DEFAULT_ISOLATION_ID,
    agentId: agentId || DEFAULT_ISOLATION_ID,
  };

  if (vectorStore) {
    try { await assertClearGuard(vectorStore, record); }
    catch { logger?.warn?.(`${TAG} Write refused: generation guard could not be verified`); return null; }
  }
  const shardDate = formatLocalDate(new Date());
  const recordKey = StoragePaths.record(shardDate);

  // Helper: append a JSONL line
  // - standalone (no storage): write to local fs
  // - service (storage provided): write via StorageAdapter, no fs fallback
  //
  // Guard log (CR-2 fix, 2026-05-19): if storage is absent, emit a warn so any
  // missed wiring (e.g. caller forgot to pass storage in service mode) is
  // immediately visible instead of silently writing to ephemeral pod fs.
  // In standalone mode this warn is benign — the gateway auto-wires a
  // LocalStorageBackend at startup (server.ts:199-203), so storage should
  // normally be defined. Seeing this warn = caller forgot to pass it.
  const appendRecord = async (line: string) => {
    if (storage) {
      await storage.appendFile(recordKey, line);
    } else {
      logger?.warn?.(
        `${TAG} [CR-2 guard] writeMemory called without storage adapter; ` +
        `falling back to local fs at ${baseDir}/records/${shardDate}.jsonl. ` +
        `In service mode this means JSONL is written to ephemeral pod fs and ` +
        `will be lost on restart. Caller must pass 'storage' to writeMemory.`,
      );
      const fs = await import("node:fs/promises");
      const path = await import("node:path");
      const recordsDir = path.default.join(baseDir, "records");
      await fs.default.mkdir(recordsDir, { recursive: true });
      await fs.default.appendFile(path.default.join(recordsDir, `${shardDate}.jsonl`), line, "utf-8");
    }
  };

  // Outcome flags gate the event ledger below: events must record what the
  // store actually did, not what the dedup decision intended.
  let targetsDeleted = true;
  let upsertOk = false;

  // === Vector Store dual-write ===
  if (vectorStore) {
    try {
      logger?.debug?.(`${TAG} [vec-dual-write] START id=${record.id}, contentLen=${record.content.length}`);

      let embedding: Float32Array | undefined;

      if (embeddingService) {
        try {
          embedding = await embeddingService.embed(record.content);
          logger?.debug?.(
            `${TAG} [vec-dual-write] Embedding OK: dims=${embedding.length}, ` +
            `norm=${Math.sqrt(Array.from(embedding).reduce((s, v) => s + v * v, 0)).toFixed(4)}`,
          );
        } catch (embedErr) {
          // Embedding failed — pass undefined to upsert() which writes
          // metadata + FTS only, skipping the vec0 table.
          logger?.warn(
            `${TAG} [vec-dual-write] Embedding FAILED for id=${record.id}, ` +
            `will write metadata only: ${embedErr instanceof Error ? embedErr.message : String(embedErr)}`,
          );
        }
      }

      upsertOk = await vectorStore.upsertL1(record, embedding);
      logger?.debug?.(`${TAG} [vec-dual-write] upsert result=${upsertOk} id=${record.id}`);
    } catch (err) {
      // Vector write failure should NOT block the main JSONL write
      logger?.warn?.(
        `${TAG} [vec-dual-write] FAILED (JSONL already written) id=${record.id}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  } else {
    logger?.debug?.(
      `${TAG} [vec-dual-write] SKIPPED id=${record.id}: vectorStore=${!!vectorStore}`,
    );
  }

  if (vectorStore && !upsertOk) return null;
  try {
    await appendRecord(JSON.stringify(record) + "\n");
  } catch (err) {
    logger?.warn?.(`${TAG} JSONL append failed id=${record.id}: ${err instanceof Error ? err.message : String(err)}`);
    if (!vectorStore) return null;
  }
  if (vectorStore && (decision.action === "update" || decision.action === "merge") && supersededTargets.length) {
    try {
      const deleteFilter = teamId || userId || agentId || taskId
        ? { teamId, userId, agentId, taskId }
        : sessionId ? { sessionId } : undefined;
      targetsDeleted = await vectorStore.deleteL1Batch(supersededTargets.map((r) => r.record_id).filter((id) => id !== record.id), deleteFilter);
    } catch (err) {
      targetsDeleted = false;
      logger?.warn?.(`${TAG} VectorStore delete failed for ${decision.action}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // === Memory event append (session diff) ===
  // Best-effort append-only event rows; failures never block the write path.
  // - store        → 1 × created
  // - update/merge → 1 × superseded per target (old-content snapshot, when the
  //                  version query above succeeded) + 1 × updated/merged
  // - skip         → no event (nothing was written)
  // Events describe committed outcomes. The successor is durable before any
  // target delete. A failed delete records `created` rather than claiming that
  // replacement succeeded; persisted review sources still guard the successor.
  //
  // `supersedes` must list what was actually replaced, not what the LLM
  // proposed: a hallucinated or out-of-tenant target id was neither read nor
  // deleted (both are tenant-scoped), and claiming it would make the diff
  // report an incomplete group forever and make revert 409 on a
  // "snapshot-less restore target". When the scoped read succeeded, its rows
  // ARE the replaced set; only when it failed do we fall back to the
  // decision (the delete used the same filter, so those ids were real).
  const replacedIds = targetsQueried ? supersededTargets.map((r) => r.record_id) : decision.target_ids;
  if (targetsQueried && replacedIds.length < decision.target_ids.length) {
    const found = new Set(replacedIds);
    logger?.warn?.(
      `${TAG} ${decision.action} id=${record.id}: target ids not found in scope, excluded from supersedes: ` +
      `[${decision.target_ids.filter((id) => !found.has(id)).join(",")}]`,
    );
  }
  if (vectorStore?.appendMemoryEvent || storage) {
    try {
      // Isolation ids are normalized by appendLedgerEvent ("" → "default").
      const base = {
        event_ts: now,
        review: { protocol: 1 as const, sources: record.review_sources, guard_at: record.review_guard_at, guard_epoch: record.review_epoch },
        session_key: sessionKey,
        session_id: record.sessionId,
        team_id: record.teamId ?? "",
        user_id: record.userId ?? "",
        agent_id: record.agentId ?? "",
        task_id: record.taskId ?? "",
      };
      if (decision.action === "store" || !targetsDeleted) {
        if (!targetsDeleted) {
          logger?.warn?.(
            `${TAG} supersede delete failed for ${decision.action} id=${record.id}; ` +
            `recording the write as 'created' (no superseded/supersedes events — nothing was actually replaced)`,
          );
        }
        await appendLedgerEvent({ store: vectorStore, storage, logger, event: {
          ...base,
          op: "created",
          record_id: record.id,
          content: record.content,
          memory_type: record.type,
          version: record.version ?? 0,
          source: "extraction",
        } });
      } else {
        for (const old of supersededTargets) {
          await appendLedgerEvent({ store: vectorStore, storage, logger, event: {
            ...base,
            origin_session_id: old.session_id || undefined,
            origin_session_key: old.session_key || undefined,
            op: "superseded",
            record_id: old.record_id,
            content: old.content,
            memory_type: old.type,
            version: old.version,
            superseded_by: record.id,
            // 完整旧记录快照：revert 时按它重建（content/type/version 不够恢复）。
            snapshot_json: JSON.stringify(old),
            source: "extraction",
          } });
        }
        await appendLedgerEvent({ store: vectorStore, storage, logger, event: {
          ...base,
          op: decision.action === "merge" ? "merged" : "updated",
          record_id: record.id,
          content: record.content,
          memory_type: record.type,
          version: record.version ?? 0,
          supersedes: replacedIds,
          source: "extraction",
        } });
      }
    } catch (err) {
      logger?.warn?.(
        `${TAG} memory event append failed (non-fatal) id=${record.id}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  return record;
}

// ============================
// Helpers
// ============================

function formatLocalDate(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}