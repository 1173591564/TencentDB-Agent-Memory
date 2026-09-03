/**
 * Hermes session-init form（clarify 载体）单测。
 *
 * 覆盖：
 *   - SSE 3-chunk 骨架（role+tool_call decl → arguments delta → finish）+ [DONE]
 *   - non-stream 形态
 *   - clarify arguments schema（questions[0].choices 平铺字符串、≤4、multi_select=false）
 *   - agent/task 分页（3+MORE 非末页、末页无 MORE）
 *   - team 截断（>4 截到 4，不 MORE）
 *   - containsFormTitle / isSessionInitToolCallId
 */
import { describe, expect, it } from "vitest";
import {
  ASSET_CONFIRM_NO,
  ASSET_CONFIRM_YES,
  MORE_LABEL,
  TOOLCALL_PREFIX,
  TOOL_NAME,
  buildFormResponse,
  containsFormTitle,
  isSessionInitToolCallId,
  type FormData,
} from "../form.js";
import type { TeamOption } from "../../types.js";

function makeTeam(overrides?: Partial<TeamOption>): TeamOption {
  return {
    team_id: "team-d03qdb2oty",
    team_name: "测试团队",
    agents: [
      { agent_id: "agt-11111111", agent_name: "agent-one" },
      { agent_id: "agt-22222222", agent_name: "agent-two" },
    ],
    tasks: [
      { task_id: "task-33333333", task_name: "任务一" },
      { task_id: "task-44444444", task_name: "任务二" },
    ],
    ...overrides,
  };
}

/** 解析 SSE 响应体为 data 帧（`[DONE]` 帧原样保留为字符串）。 */
async function parseSse(res: Response): Promise<Array<Record<string, unknown> | "[DONE]">> {
  const text = await res.text();
  return text
    .split("\n\n")
    .filter((f) => f.startsWith("data: "))
    .map((f) => (f.slice(6) === "[DONE]" ? "[DONE]" : JSON.parse(f.slice(6))));
}

describe("hermes form — asset_confirm", () => {
  it("streaming: 3 chunks + DONE, clarify tool_call with hermes prefix", async () => {
    const data: FormData = { teams: [makeTeam()], stage: "asset_confirm", stream: true, modelId: "deepseek-v4-flash" };
    const res = buildFormResponse(data);
    expect(res.headers.get("content-type")).toContain("text/event-stream");

    const frames = await parseSse(res);
    expect(frames.length).toBe(4);
    expect(frames[3]).toBe("[DONE]");

    const chunk1 = frames[0] as Record<string, any>;
    expect(chunk1.object).toBe("chat.completion.chunk");
    const tc1 = chunk1.choices[0].delta.tool_calls[0];
    expect(tc1.id.startsWith(TOOLCALL_PREFIX)).toBe(true);
    expect(tc1.type).toBe("function");
    expect(tc1.function.name).toBe(TOOL_NAME);
    expect(chunk1.choices[0].finish_reason).toBeNull();

    const chunk2 = frames[1] as Record<string, any>;
    const args = JSON.parse(chunk2.choices[0].delta.tool_calls[0].function.arguments);
    expect(args.questions.length).toBe(1);
    expect(args.questions[0].multi_select).toBe(false);
    expect(args.questions[0].choices).toEqual([ASSET_CONFIRM_YES, ASSET_CONFIRM_NO]);

    const chunk3 = frames[2] as Record<string, any>;
    expect(chunk3.choices[0].finish_reason).toBe("tool_calls");
  });

  it("non-stream: message.tool_calls with finish_reason=tool_calls", async () => {
    const data: FormData = { teams: [makeTeam()], stage: "asset_confirm", stream: false };
    const res = buildFormResponse(data);
    expect(res.headers.get("content-type")).toContain("application/json");

    const body = (await res.json()) as Record<string, any>;
    expect(body.object).toBe("chat.completion");
    const choice = body.choices[0];
    expect(choice.finish_reason).toBe("tool_calls");
    expect(choice.message.tool_calls[0].function.name).toBe(TOOL_NAME);
    const args = JSON.parse(choice.message.tool_calls[0].function.arguments);
    expect(args.questions[0].choices).toEqual([ASSET_CONFIRM_YES, ASSET_CONFIRM_NO]);
  });
});

describe("hermes form — team stage", () => {
  it("truncates to 4 choices (clarify MAX_CHOICES hard limit), no MORE", () => {
    const teams = Array.from({ length: 6 }, (_, i) =>
      makeTeam({ team_id: `team-0000000${i}`, team_name: `团队${i}` }),
    );
    const res = buildFormResponse({ teams, stage: "team", stream: false });
    return res.json().then((body: any) => {
      const args = JSON.parse(body.choices[0].message.tool_calls[0].function.arguments);
      expect(args.questions[0].choices.length).toBe(4);
      expect(args.questions[0].choices.some((c: string) => c.includes(MORE_LABEL))).toBe(false);
    });
  });

  it("throws when <2 teams (caller must auto-select)", () => {
    expect(() =>
      buildFormResponse({ teams: [makeTeam()], stage: "team", stream: false }),
    ).toThrow(/≥2 teams/);
  });
});

describe("hermes form — agent/task pagination", () => {
  const eightAgents = Array.from({ length: 8 }, (_, i) => ({
    agent_id: `agt-0000000${i}`,
    agent_name: `agent-${i}`,
  }));

  it("non-last page: 3 real options + MORE", () => {
    const team = makeTeam({ agents: eightAgents });
    const res = buildFormResponse({ teams: [team], stage: "agent_select", selectedTeamId: team.team_id, pageIndex: 0, stream: false });
    return res.json().then((body: any) => {
      const args = JSON.parse(body.choices[0].message.tool_calls[0].function.arguments);
      const choices: string[] = args.questions[0].choices;
      expect(choices.length).toBe(4);
      expect(choices[3]).toBe(MORE_LABEL);
      expect(args.questions[0].question).toContain("第 1/3 页");
    });
  });

  it("last page: remaining options, no MORE", () => {
    const team = makeTeam({ agents: eightAgents });
    const res = buildFormResponse({ teams: [team], stage: "agent_select", selectedTeamId: team.team_id, pageIndex: 2, stream: false });
    return res.json().then((body: any) => {
      const args = JSON.parse(body.choices[0].message.tool_calls[0].function.arguments);
      const choices: string[] = args.questions[0].choices;
      expect(choices.length).toBe(2);
      expect(choices.some((c) => c === MORE_LABEL)).toBe(false);
    });
  });

  it("task pagination keeps virtual default task label untouched", () => {
    const team = makeTeam({
      tasks: [
        { task_id: "task-default", task_name: "本次不关联任务", isDefault: true },
        ...Array.from({ length: 5 }, (_, i) => ({ task_id: `task-0000000${i}`, task_name: `任务${i}` })),
      ],
    });
    const res = buildFormResponse({ teams: [team], stage: "task_select", selectedTeamId: team.team_id, selectedAgentId: "agt-11111111", pageIndex: 0, stream: false });
    return res.json().then((body: any) => {
      const args = JSON.parse(body.choices[0].message.tool_calls[0].function.arguments);
      const choices: string[] = args.questions[0].choices;
      expect(choices[0]).toBe("本次不关联任务");
      expect(choices.length).toBe(4);
      expect(choices[3]).toBe(MORE_LABEL);
    });
  });
});

describe("hermes form — markers", () => {
  it("containsFormTitle / isSessionInitToolCallId", () => {
    expect(containsFormTitle("会话初始化 — 是否关联团队资产")).toBe(true);
    expect(containsFormTitle("会话初始化 — 选择 Team")).toBe(true);
    expect(containsFormTitle("会话初始化 — 选择 Agent 与任务")).toBe(true);
    expect(containsFormTitle("会话初始化 — 未能识别选择，请重新选择".replace("会话初始化 — ", ""))).toBe(true);
    expect(containsFormTitle("随便一段普通文本")).toBe(false);

    expect(isSessionInitToolCallId("call_hermes_session_init_1737000000000")).toBe(true);
    expect(isSessionInitToolCallId("call_dsh_session_init_1737000000000")).toBe(false);
    expect(isSessionInitToolCallId("call_00_w3yZzbqGXT6")).toBe(false);
  });

  it("retry prefix lands in question text", () => {
    const res = buildFormResponse({ teams: [makeTeam()], stage: "asset_confirm", retry: true, stream: false });
    return res.json().then((body: any) => {
      const args = JSON.parse(body.choices[0].message.tool_calls[0].function.arguments);
      expect(args.questions[0].question.startsWith("⚠️ ")).toBe(true);
    });
  });
});
