/**
 * Auto-assign a conversation ID when the client sent no session/conversation header.
 *
 * Explicit headers (x-conversation-id / x-session-id / …) never enter this module —
 * callers must skip allocation when resolveConversationId already hit.
 *
 * per-key (default): one active session per API-key hash.
 *   - isFreshConversation → mint a new UUID (covers "new chat" without a static id)
 *   - otherwise reuse lastActive if within TTL (covers tool-call follow-ups that drop extra headers)
 * per-key-msg: alias by first user-message fingerprint so parallel windows stay apart.
 */
import { createHash } from "node:crypto";
import { uuidv7 } from "../opik.js";
import { isFreshConversation } from "./session-key.js";
import type { SessionInitConfig } from "../types.js";

export interface Allocation {
  id: string;
  isNew: boolean;
}

export interface ConversationAllocatorConfig {
  ttlMinutes: number;
  strategy: "per-key" | "per-key-msg";
  maxEntries?: number;
  now?: () => number;
}

interface Entry {
  conversationId: string;
  lastSeen: number;
}

const DEFAULT_MAX_ENTRIES = 10_000;

export function shouldAllocateConversationId(opts: {
  conversationId: string | null | undefined;
  keyId: string;
  enabled?: boolean;
  skip?: boolean;
}): boolean {
  return (
    !opts.conversationId &&
    opts.keyId !== "unknown" &&
    opts.enabled !== false &&
    !opts.skip
  );
}

export class ConversationAllocator {
  private byAlias = new Map<string, Entry>();
  private lastActive = new Map<string, string>();
  private ttlMs: number;
  private strategy: "per-key" | "per-key-msg";
  private maxEntries: number;
  private now: () => number;

  constructor(cfg: ConversationAllocatorConfig) {
    this.ttlMs = Math.max(1, cfg.ttlMinutes) * 60 * 1000;
    this.strategy = cfg.strategy;
    this.maxEntries = cfg.maxEntries ?? DEFAULT_MAX_ENTRIES;
    this.now = cfg.now ?? Date.now;
  }

  get strategyName(): "per-key" | "per-key-msg" {
    return this.strategy;
  }

  get ttlMinutes(): number {
    return this.ttlMs / 60_000;
  }

  resolve(keyId: string, messages: Array<{ role?: string; content?: unknown }>): Allocation {
    const fresh = isFreshConversation(messages);
    const aliasKey = this.aliasOf(keyId, messages);

    if (this.strategy === "per-key") {
      if (fresh) return this.mint(aliasKey, keyId);
      const reused = this.tryReuse(aliasKey);
      if (reused) return reused;
      return this.mint(aliasKey, keyId);
    }

    const hit = this.tryReuse(aliasKey);
    if (hit) return hit;

    if (fresh) return this.mint(aliasKey, keyId);

    const fallbackAlias = this.lastActive.get(keyId);
    if (fallbackAlias && fallbackAlias !== aliasKey) {
      const fallback = this.tryReuse(fallbackAlias);
      if (fallback) return fallback;
      this.lastActive.delete(keyId);
    }
    return this.mint(aliasKey, keyId);
  }

  private aliasOf(
    keyId: string,
    messages: Array<{ role?: string; content?: unknown }>,
  ): string {
    if (this.strategy !== "per-key-msg") return keyId;
    const fp = firstUserFingerprint(messages);
    return fp ? `${keyId}:${fp}` : keyId;
  }

  private tryReuse(aliasKey: string): Allocation | null {
    const entry = this.byAlias.get(aliasKey);
    if (!entry) return null;
    if (this.now() - entry.lastSeen > this.ttlMs) {
      this.byAlias.delete(aliasKey);
      return null;
    }
    this.touch(aliasKey, entry);
    return { id: entry.conversationId, isNew: false };
  }

  private mint(aliasKey: string, keyId: string): Allocation {
    this.evictIfNeeded();
    const id = uuidv7();
    const entry: Entry = { conversationId: id, lastSeen: this.now() };
    if (this.byAlias.has(aliasKey)) this.byAlias.delete(aliasKey);
    this.byAlias.set(aliasKey, entry);
    this.lastActive.set(keyId, aliasKey);
    return { id, isNew: true };
  }

  private touch(aliasKey: string, entry: Entry): void {
    entry.lastSeen = this.now();
    this.byAlias.delete(aliasKey);
    this.byAlias.set(aliasKey, entry);
  }

  private evictIfNeeded(): void {
    while (this.byAlias.size >= this.maxEntries) {
      const oldest = this.byAlias.keys().next().value as string | undefined;
      if (oldest === undefined) return;
      this.byAlias.delete(oldest);
      for (const [keyId, alias] of this.lastActive) {
        if (alias === oldest) this.lastActive.delete(keyId);
      }
    }
  }
}

function firstUserFingerprint(messages: Array<{ role?: string; content?: unknown }>): string | null {
  for (const m of messages) {
    if ((m.role ?? "") !== "user") continue;
    const text = extractText(m.content).trim();
    if (!text) return null;
    return createHash("sha256").update(text).digest("hex").slice(0, 16);
  }
  return null;
}

function extractText(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((block) => {
        if (typeof block === "string") return block;
        if (block && typeof block === "object" && "text" in block) {
          const t = (block as { text?: unknown }).text;
          return typeof t === "string" ? t : "";
        }
        return "";
      })
      .join("");
  }
  return "";
}

let _allocator: ConversationAllocator | undefined;
let _allocatorSig = "";

export function getConversationAllocator(cfg: SessionInitConfig): ConversationAllocator {
  const ac = cfg.autoConversationId;
  const ttlMinutes = ac?.ttlMinutes ?? 30;
  const strategy: "per-key" | "per-key-msg" = ac?.strategy === "per-key-msg" ? "per-key-msg" : "per-key";
  const sig = `${ttlMinutes}:${strategy}`;
  if (!_allocator || _allocatorSig !== sig) {
    _allocator = new ConversationAllocator({ ttlMinutes, strategy });
    _allocatorSig = sig;
  }
  return _allocator;
}

/** Test-only: drop the process-wide singleton. */
export function __resetConversationAllocator(): void {
  _allocator = undefined;
  _allocatorSig = "";
}
