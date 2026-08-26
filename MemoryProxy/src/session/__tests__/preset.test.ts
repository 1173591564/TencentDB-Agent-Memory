import { describe, expect, it } from "vitest";
import { resolvePresetIdentity, type PresetIdentity } from "../preset.js";
import type { TeamOption } from "../types.js";

const teams: TeamOption[] = [
  {
    team_id: "team-1",
    team_name: "T1",
    agents: [{ agent_id: "agent-1", agent_name: "A1" }],
    tasks: [{ task_id: "task-1", task_name: "Tk1" }],
  },
];

function preset(p: PresetIdentity): PresetIdentity {
  return p;
}

describe("resolvePresetIdentity / taskMissingPolicy", () => {
  const cases: Array<{
    name: string;
    policy: "skip" | "default" | "reject";
    input: PresetIdentity;
    canRegister: boolean;
    hadMismatch: boolean;
  }> = [
    { name: "skip + team+agent+task", policy: "skip", input: { teamId: "team-1", agentId: "agent-1", taskId: "task-1" }, canRegister: true, hadMismatch: false },
    { name: "skip + team+agent", policy: "skip", input: { teamId: "team-1", agentId: "agent-1" }, canRegister: true, hadMismatch: false },
    { name: "skip + team+agent+invalid task", policy: "skip", input: { teamId: "team-1", agentId: "agent-1", taskId: "nope" }, canRegister: false, hadMismatch: true },
    { name: "skip + team only", policy: "skip", input: { teamId: "team-1" }, canRegister: false, hadMismatch: false },
    { name: "default + team+agent+task", policy: "default", input: { teamId: "team-1", agentId: "agent-1", taskId: "task-1" }, canRegister: true, hadMismatch: false },
    { name: "default + team+agent", policy: "default", input: { teamId: "team-1", agentId: "agent-1" }, canRegister: true, hadMismatch: false },
    { name: "default + team+agent+invalid task", policy: "default", input: { teamId: "team-1", agentId: "agent-1", taskId: "nope" }, canRegister: false, hadMismatch: true },
    { name: "default + team only", policy: "default", input: { teamId: "team-1" }, canRegister: false, hadMismatch: false },
    { name: "reject + team+agent+task", policy: "reject", input: { teamId: "team-1", agentId: "agent-1", taskId: "task-1" }, canRegister: true, hadMismatch: false },
    { name: "reject + team+agent", policy: "reject", input: { teamId: "team-1", agentId: "agent-1" }, canRegister: false, hadMismatch: false },
    { name: "reject + team+agent+invalid task", policy: "reject", input: { teamId: "team-1", agentId: "agent-1", taskId: "nope" }, canRegister: false, hadMismatch: true },
    { name: "reject + team only", policy: "reject", input: { teamId: "team-1" }, canRegister: false, hadMismatch: false },
  ];

  for (const c of cases) {
    it(c.name, () => {
      const r = resolvePresetIdentity(teams, preset(c.input), c.policy);
      expect(r.canRegister).toBe(c.canRegister);
      expect(r.hadMismatch).toBe(c.hadMismatch);
    });
  }

  it("缺省第三参 = reject（WorkBuddy 契约）", () => {
    const r = resolvePresetIdentity(teams, { teamId: "team-1", agentId: "agent-1" });
    expect(r.canRegister).toBe(false);
    expect(r.hadMismatch).toBe(false);
  });
});
