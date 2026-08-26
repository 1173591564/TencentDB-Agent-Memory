import { describe, expect, it } from "vitest";
import {
  ConversationAllocator,
  shouldAllocateConversationId,
} from "../conversation-allocator.js";

const USER_HI = [{ role: "user", content: "hi" }];
const FOLLOW_UP = [
  { role: "user", content: "hi" },
  { role: "assistant", content: "hello" },
  { role: "user", content: "more" },
];
const TOOL_ROUND = [
  { role: "user", content: "hi" },
  { role: "assistant", content: "call tool" },
  { role: "tool", content: "result" },
];

describe("shouldAllocateConversationId", () => {
  it("显式 conversationId 不分配", () => {
    expect(shouldAllocateConversationId({ conversationId: "c1", keyId: "k1" })).toBe(false);
  });
  it("keyId=unknown 不分配", () => {
    expect(shouldAllocateConversationId({ conversationId: null, keyId: "unknown" })).toBe(false);
  });
  it("enabled=false 不分配", () => {
    expect(shouldAllocateConversationId({ conversationId: null, keyId: "k1", enabled: false })).toBe(false);
  });
  it("skip (aux/headless/sidequery) 不分配", () => {
    expect(shouldAllocateConversationId({ conversationId: null, keyId: "k1", skip: true })).toBe(false);
  });
  it("无头 + 合法 key + main 分配", () => {
    expect(shouldAllocateConversationId({ conversationId: null, keyId: "k1" })).toBe(true);
  });
});

describe("ConversationAllocator per-key", () => {
  it("非 fresh 同 keyId 在 TTL 内复用同一 id", () => {
    const a = new ConversationAllocator({ ttlMinutes: 30, strategy: "per-key" });
    const first = a.resolve("k1", USER_HI);
    const second = a.resolve("k1", TOOL_ROUND);
    expect(second.isNew).toBe(false);
    expect(second.id).toBe(first.id);
  });

  it("isFreshConversation 即使 TTL 未到也 mint 新 id", () => {
    const a = new ConversationAllocator({ ttlMinutes: 30, strategy: "per-key" });
    const first = a.resolve("k1", USER_HI);
    a.resolve("k1", FOLLOW_UP);
    const fresh = a.resolve("k1", [{ role: "user", content: "brand new" }]);
    expect(fresh.isNew).toBe(true);
    expect(fresh.id).not.toBe(first.id);
  });

  it("超过 ttl 后非 fresh 请求 mint 新 id", () => {
    let now = 1_000_000;
    const a = new ConversationAllocator({
      ttlMinutes: 30,
      strategy: "per-key",
      now: () => now,
    });
    const first = a.resolve("k1", USER_HI);
    a.resolve("k1", FOLLOW_UP);
    now += 31 * 60 * 1000;
    const next = a.resolve("k1", FOLLOW_UP);
    expect(next.isNew).toBe(true);
    expect(next.id).not.toBe(first.id);
  });

  it("非 fresh 命中刷新滑动 TTL", () => {
    let now = 1_000_000;
    const a = new ConversationAllocator({
      ttlMinutes: 30,
      strategy: "per-key",
      now: () => now,
    });
    const first = a.resolve("k1", USER_HI);
    now += 29 * 60 * 1000;
    const mid = a.resolve("k1", FOLLOW_UP);
    expect(mid.id).toBe(first.id);
    now += 29 * 60 * 1000;
    const late = a.resolve("k1", FOLLOW_UP);
    expect(late.isNew).toBe(false);
    expect(late.id).toBe(first.id);
  });
});

describe("ConversationAllocator per-key-msg", () => {
  it("不同首条 user 消息 → 不同会话", () => {
    const a = new ConversationAllocator({ ttlMinutes: 30, strategy: "per-key-msg" });
    const s1 = a.resolve("k1", [{ role: "user", content: "alpha" }]);
    const s2 = a.resolve("k1", [{ role: "user", content: "beta" }]);
    expect(s2.id).not.toBe(s1.id);
    expect(s2.isNew).toBe(true);
  });

  it("同首消息多轮历史 → 同会话", () => {
    const a = new ConversationAllocator({ ttlMinutes: 30, strategy: "per-key-msg" });
    const s1 = a.resolve("k1", [{ role: "user", content: "alpha" }]);
    const s2 = a.resolve("k1", [
      { role: "user", content: "alpha" },
      { role: "assistant", content: "ok" },
      { role: "user", content: "next" },
    ]);
    expect(s2.id).toBe(s1.id);
    expect(s2.isNew).toBe(false);
  });

  it("指纹未命中时回落 lastActive（compaction 漂移）", () => {
    const a = new ConversationAllocator({ ttlMinutes: 30, strategy: "per-key-msg" });
    const s1 = a.resolve("k1", [{ role: "user", content: "alpha" }]);
    // compacted history whose first user text no longer matches, but not fresh
    const drifted = a.resolve("k1", [
      { role: "user", content: "summary of prior chat" },
      { role: "assistant", content: "ok" },
    ]);
    expect(drifted.isNew).toBe(false);
    expect(drifted.id).toBe(s1.id);
  });

  it("非 fresh 请求不创建新 alias", () => {
    const a = new ConversationAllocator({ ttlMinutes: 30, strategy: "per-key-msg" });
    const s1 = a.resolve("k1", [{ role: "user", content: "alpha" }]);
    const otherWindowMid = a.resolve("k1", [
      { role: "user", content: "other window" },
      { role: "assistant", content: "x" },
      { role: "tool", content: "y" },
    ]);
    expect(otherWindowMid.id).toBe(s1.id);
  });
});

describe("ConversationAllocator LRU", () => {
  it("超过 maxEntries 淘汰最久未用；lastActive 指针惰性清理", () => {
    const a = new ConversationAllocator({
      ttlMinutes: 30,
      strategy: "per-key",
      maxEntries: 2,
    });
    const k1 = a.resolve("k1", USER_HI);
    a.resolve("k2", USER_HI);
    a.resolve("k3", USER_HI);
    // k1 should have been evicted; a non-fresh request on k1 mints new
    const again = a.resolve("k1", FOLLOW_UP);
    expect(again.isNew).toBe(true);
    expect(again.id).not.toBe(k1.id);
  });
});
