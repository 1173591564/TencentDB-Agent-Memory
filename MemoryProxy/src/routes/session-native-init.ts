/**
 * POST /v3/session/native-init
 *
 * 给 dsh 原生插件用的 session 写入端点：选完三元组后把 session 状态灌进 proxy 的
 * SessionStore（L1 内存 + L2 SQLite），让后续走 proxy 的请求直接 loadAllInitialized
 * 恢复，不再重问身份。
 *
 * 实验用，无 admin 鉴权（仅本机）。body:
 *   { session_key, agent_source, space_id, user_id, session_info, agent_detail, task_detail }
 *
 * session_info: { session_id, team_id, agent_id, user_id, task_id?, user_key?, space_id?, created_at? }
 * agent_detail: { id, name?, description?, prompt? }
 * task_detail:  { id, name?, description?, goal? }
 */
import type { Context } from "hono";
import type { ProxyConfig } from "../types.js";
import { getSessionStore } from "../session/store.js";
import type { SessionInitState, SessionInfo, AgentDetail, TaskDetail } from "../session/types.js";

export function createSessionNativeInitHandler(_config: ProxyConfig) {
  return async (c: Context): Promise<Response> => {
    let body: Record<string, unknown>;
    try {
      body = await c.req.json<Record<string, unknown>>();
    } catch {
      return c.json({ code: 40001, message: "Invalid JSON body", request_id: `native-init-${Date.now()}` }, 400);
    }

    const sessionKey = typeof body.session_key === "string" ? body.session_key : "";
    const agentSource = typeof body.agent_source === "string" ? body.agent_source : "";
    const spaceId = typeof body.space_id === "string" ? body.space_id : "";
    const userId = typeof body.user_id === "string" ? body.user_id : "";

    if (!sessionKey || !agentSource || !userId) {
      return c.json({ code: 40001, message: "missing session_key / agent_source / user_id", request_id: `native-init-${Date.now()}` }, 400);
    }

    const sessionInfo = body.session_info as SessionInfo | undefined;
    if (!sessionInfo?.team_id || !sessionInfo?.agent_id) {
      return c.json({ code: 40001, message: "session_info missing team_id / agent_id", request_id: `native-init-${Date.now()}` }, 400);
    }

    const agentDetail = (body.agent_detail ?? null) as AgentDetail | null;
    const taskDetail = (body.task_detail ?? null) as TaskDetail | null;

    const compositeKey = `${agentSource}:${sessionKey}`;
    const store = getSessionStore();

    // bind identity（store.set 需要 identity 才能 L2 持久化）
    store.bind(sessionKey, { userId, agentSource, sessionId: sessionKey, spaceId: spaceId || undefined });

    const state: SessionInitState = {
      status: "initialized",
      keyId: sessionKey,
      startedAt: Date.now(),
      attemptCount: 0,
      sessionInfo,
      userId,
      agentDetail,
      taskDetail,
    };
    await store.set(compositeKey, state);

    const requestId = `native-init-${Date.now()}`;
    return c.json({
      code: 0,
      message: "ok",
      request_id: requestId,
      data: {
        composite_key: compositeKey,
        status: "initialized",
        team_id: sessionInfo.team_id,
        agent_id: sessionInfo.agent_id,
        task_id: sessionInfo.task_id ?? null,
      },
    });
  };
}
