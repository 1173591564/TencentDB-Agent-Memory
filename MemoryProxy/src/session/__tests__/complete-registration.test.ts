import { describe, expect, it, vi } from "vitest";
import { completeRegistration as completeCb } from "../codebuddy/init.js";
import { completeRegistration as completeCc } from "../claude-code/init.js";
import { SessionStore } from "../store.js";
import {
  injectSessionContextWithToggles,
  buildSessionContextBlockWithToggles,
  NO_TASK_NOTICE,
} from "../context-injector.js";
import type { SessionInitConfig } from "../../types.js";
import type { SessionInitState, TeamOption } from "../types.js";
import type { MetadataClient } from "../../meta/client.js";

const teams: TeamOption[] = [
  {
    team_id: "team-1",
    team_name: "T1",
    agents: [{ agent_id: "agent-1", agent_name: "A1" }],
    tasks: [{ task_id: "task-1", task_name: "Tk1" }],
  },
];

function cfg(partial: Partial<SessionInitConfig> = {}): SessionInitConfig {
  return {
    enabled: true,
    maxRetries: 3,
    injectAgentContext: true,
    injectTaskContext: true,
    ...partial,
  };
}

function state(): SessionInitState {
  return {
    status: "uninitialized",
    keyId: "sess-1",
    startedAt: Date.now(),
    attemptCount: 0,
    userId: "user-1",
    cachedTeams: teams,
  };
}

function mockMeta() {
  const getAgent = vi.fn(async () => ({
    agent_id: "agent-1",
    team_id: "team-1",
    name: "A1",
    description: "d",
    prompt: "p",
  }));
  const getTask = vi.fn(async () => ({
    task_id: "task-1",
    team_id: "team-1",
    title: "Tk1",
    description: "td",
  }));
  const appendParticipationLog = vi.fn(async () => ({}));
  return {
    getAgent,
    getTask,
    appendParticipationLog,
  } as unknown as MetadataClient & {
    getAgent: typeof getAgent;
    getTask: typeof getTask;
    appendParticipationLog: typeof appendParticipationLog;
  };
}

describe("completeRegistration CodeBuddy", () => {
  it("skip: 无 task 注册成功", async () => {
    const store = new SessionStore();
    const meta = mockMeta();
    const result = await completeCb(
      { agent_id: "agent-1" },
      state(),
      teams,
      "cb:sess-1",
      "sess-1",
      "user-1",
      cfg({ taskMissingPolicy: "skip" }),
      store,
      [{ role: "user", content: "hi" }],
      meta,
    );
    expect(result.bypassed).toBeFalsy();
    expect(result.sessionInfo?.task_id).toBeUndefined();
    expect(meta.getTask).not.toHaveBeenCalled();
    expect(meta.appendParticipationLog).not.toHaveBeenCalled();
    expect(store.get("cb:sess-1")?.status).toBe("initialized");
    expect(store.get("cb:sess-1")?.bypassed).toBeFalsy();
    const blob = JSON.stringify(result.messages);
    expect(blob).toContain("[Notice]");
    expect(blob).toContain(NO_TASK_NOTICE);
  });

  it("default + 已配 defaultTaskId: task_id=占位值、getTask 未调用", async () => {
    const store = new SessionStore();
    const meta = mockMeta();
    const result = await completeCb(
      { agent_id: "agent-1" },
      state(),
      teams,
      "cb:sess-1",
      "sess-1",
      "user-1",
      cfg({ taskMissingPolicy: "default", defaultTaskId: "virtual-skip" }),
      store,
      [{ role: "user", content: "hi" }],
      meta,
    );
    expect(result.bypassed).toBeFalsy();
    expect(result.sessionInfo?.task_id).toBe("virtual-skip");
    expect(meta.getTask).not.toHaveBeenCalled();
  });

  it("default 未配 defaultTaskId: 降级 skip", async () => {
    const store = new SessionStore();
    const meta = mockMeta();
    const result = await completeCb(
      { agent_id: "agent-1" },
      state(),
      teams,
      "cb:sess-1",
      "sess-1",
      "user-1",
      cfg({ taskMissingPolicy: "default" }),
      store,
      [{ role: "user", content: "hi" }],
      meta,
    );
    expect(result.sessionInfo?.task_id).toBeUndefined();
    expect(meta.getTask).not.toHaveBeenCalled();
  });

  it("reject: bypassed=true", async () => {
    const store = new SessionStore();
    const meta = mockMeta();
    const result = await completeCb(
      { agent_id: "agent-1" },
      state(),
      teams,
      "cb:sess-1",
      "sess-1",
      "user-1",
      cfg({ taskMissingPolicy: "reject" }),
      store,
      [{ role: "user", content: "hi" }],
      meta,
    );
    expect(result.bypassed).toBe(true);
    expect(store.get("cb:sess-1")?.bypassed).toBe(true);
    expect(meta.getAgent).not.toHaveBeenCalled();
  });
});

describe("completeRegistration Claude Code", () => {
  const reqCtx = { stream: false, modelId: "m" };

  it("skip: 无 task 注册成功", async () => {
    const store = new SessionStore();
    const meta = mockMeta();
    const result = await completeCc(
      { agent_id: "agent-1" },
      state(),
      teams,
      "team-1",
      "cc:sess-1",
      "sess-1",
      "user-1",
      cfg({ taskMissingPolicy: "skip" }),
      store,
      reqCtx,
      [{ role: "user", content: "hi" }],
      meta,
    );
    expect(result.bypassed).toBeFalsy();
    expect(result.sessionInfo?.task_id).toBeUndefined();
    expect(meta.getTask).not.toHaveBeenCalled();
    expect(meta.appendParticipationLog).not.toHaveBeenCalled();
    expect(store.get("cc:sess-1")?.status).toBe("initialized");
    const blob = JSON.stringify(result.messages);
    expect(blob).toContain("[Notice]");
  });

  it("default + 已配 defaultTaskId: task_id=占位值、getTask 未调用", async () => {
    const store = new SessionStore();
    const meta = mockMeta();
    const result = await completeCc(
      { agent_id: "agent-1" },
      state(),
      teams,
      "team-1",
      "cc:sess-1",
      "sess-1",
      "user-1",
      cfg({ taskMissingPolicy: "default", defaultTaskId: "virtual-skip" }),
      store,
      reqCtx,
      [{ role: "user", content: "hi" }],
      meta,
    );
    expect(result.sessionInfo?.task_id).toBe("virtual-skip");
    expect(meta.getTask).not.toHaveBeenCalled();
  });

  it("default 未配 defaultTaskId: 降级 skip", async () => {
    const store = new SessionStore();
    const meta = mockMeta();
    const result = await completeCc(
      { agent_id: "agent-1" },
      state(),
      teams,
      "team-1",
      "cc:sess-1",
      "sess-1",
      "user-1",
      cfg({ taskMissingPolicy: "default" }),
      store,
      reqCtx,
      [{ role: "user", content: "hi" }],
      meta,
    );
    expect(result.sessionInfo?.task_id).toBeUndefined();
  });

  it("reject: bypassed=true", async () => {
    const store = new SessionStore();
    const meta = mockMeta();
    const result = await completeCc(
      { agent_id: "agent-1" },
      state(),
      teams,
      "team-1",
      "cc:sess-1",
      "sess-1",
      "user-1",
      cfg({ taskMissingPolicy: "reject" }),
      store,
      reqCtx,
      [{ role: "user", content: "hi" }],
      meta,
    );
    expect(result.bypassed).toBe(true);
    expect(meta.getAgent).not.toHaveBeenCalled();
  });
});

describe("no-task Notice", () => {
  const agent = { id: "agent-1", name: "A1" };
  const task = { id: "task-1", name: "Tk1" };

  it("agent && !task → <session_context> 含 [Notice]", () => {
    const msgs = injectSessionContextWithToggles(
      [{ role: "user", content: "hi" }],
      agent,
      null,
      { injectAgentContext: true, injectTaskContext: true },
      "s1",
    );
    const text = JSON.stringify(msgs);
    expect(text).toContain("[Notice]");
    expect(text).toContain(NO_TASK_NOTICE);
  });

  it("agent && task → 无 [Notice]", () => {
    const msgs = injectSessionContextWithToggles(
      [{ role: "user", content: "hi" }],
      agent,
      task,
      { injectAgentContext: true, injectTaskContext: true },
      "s1",
    );
    expect(JSON.stringify(msgs)).not.toContain("[Notice]");
  });

  it("injectTaskContext=false 且 task 存在 → 无 [Notice]", () => {
    const block = buildSessionContextBlockWithToggles(
      agent,
      task,
      { injectAgentContext: true, injectTaskContext: false },
      "s1",
    );
    expect(block ?? "").not.toContain("[Notice]");
  });
});
