import { describe, expect, it, vi } from "vitest";
import {
  ConversationAllocator,
  getConversationAllocator,
  shouldAllocateConversationId,
  __resetConversationAllocator,
} from "../conversation-allocator.js";
import type { SessionInitConfig } from "../../types.js";

/**
 * Handler-wiring contract tests.
 *
 * The OpenAI/Anthropic handlers call shouldAllocateConversationId then
 * ConversationAllocator.resolve. Full Hono handler tests would need a live
 * upstream; these lock the decision table and the "tool round dropped extra
 * headers" reuse path that sessionKey is derived from.
 */

describe("handler wiring: allocation decision", () => {
  it("无头 main 请求 → 分配", () => {
    expect(
      shouldAllocateConversationId({ conversationId: null, keyId: "deadbeef" }),
    ).toBe(true);
  });

  it("显式 x-conversation-id → 分配器不被调用", () => {
    expect(
      shouldAllocateConversationId({ conversationId: "explicit-conv", keyId: "deadbeef" }),
    ).toBe(false);
  });

  it("显式 x-session-id（OpenCode 路径）→ 不分配", () => {
    // resolveConversationId already mapped X-Session-Id → conversationId
    expect(
      shouldAllocateConversationId({ conversationId: "oc-session-1", keyId: "deadbeef" }),
    ).toBe(false);
  });

  it("auxiliary 请求不分配", () => {
    expect(
      shouldAllocateConversationId({ conversationId: null, keyId: "deadbeef", skip: true }),
    ).toBe(false);
  });

  it("dsh headless 不分配", () => {
    expect(
      shouldAllocateConversationId({ conversationId: null, keyId: "deadbeef", skip: true }),
    ).toBe(false);
  });

  it('keyId="unknown" 不分配', () => {
    expect(
      shouldAllocateConversationId({ conversationId: null, keyId: "unknown" }),
    ).toBe(false);
  });

  it("anthropic sidequery 不分配 / main 分配", () => {
    expect(
      shouldAllocateConversationId({
        conversationId: null,
        keyId: "deadbeef",
        skip: true, // requestKind === "sidequery"
      }),
    ).toBe(false);
    expect(
      shouldAllocateConversationId({
        conversationId: null,
        keyId: "deadbeef",
        skip: false,
      }),
    ).toBe(true);
  });
});

describe("handler wiring: tool 轮丢头续接", () => {
  it("第二轮无头请求复用第一轮分配的 id", () => {
    const alloc = new ConversationAllocator({ ttlMinutes: 30, strategy: "per-key" });
    const turn1 = alloc.resolve("deadbeef", [{ role: "user", content: "hello" }]);
    // tool follow-up: extra headers gone, but messages contain assistant/tool
    const turn2 = alloc.resolve("deadbeef", [
      { role: "user", content: "hello" },
      { role: "assistant", content: "calling tool" },
      { role: "tool", content: "{}" },
    ]);
    expect(turn2.id).toBe(turn1.id);
    expect(turn2.isNew).toBe(false);
  });
});

describe("getConversationAllocator singleton", () => {
  it("same ttl/strategy reuses instance; config change rebuilds", () => {
    __resetConversationAllocator();
    const cfgA: SessionInitConfig = {
      enabled: true,
      maxRetries: 3,
      autoConversationId: { enabled: true, ttlMinutes: 30, strategy: "per-key" },
    };
    const a = getConversationAllocator(cfgA);
    const b = getConversationAllocator(cfgA);
    expect(a).toBe(b);
    const cfgB: SessionInitConfig = {
      enabled: true,
      maxRetries: 3,
      autoConversationId: { enabled: true, ttlMinutes: 30, strategy: "per-key-msg" },
    };
    const c = getConversationAllocator(cfgB);
    expect(c).not.toBe(a);
    __resetConversationAllocator();
  });
});

describe("handler wiring: spy explicit header skips allocator.resolve", () => {
  it("callers must not resolve when conversationId is set", () => {
    const alloc = new ConversationAllocator({ ttlMinutes: 30, strategy: "per-key" });
    const spy = vi.spyOn(alloc, "resolve");
    const conversationId = "x-conversation-id-value";
    if (shouldAllocateConversationId({ conversationId, keyId: "deadbeef" })) {
      alloc.resolve("deadbeef", []);
    }
    expect(spy).not.toHaveBeenCalled();
  });
});
