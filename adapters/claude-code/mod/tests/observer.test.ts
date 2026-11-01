import type { On } from "claude-code"
import { expect, mock, test } from "claude-code/testing"

import { FIXTURE, PROCESS_OK, TURN_STEP_RESULT } from "./fixtures"

type Emission = {
  argv: readonly string[]
  type: string
  emitterEventId: string
  turnRef?: string
  toolRef?: string
  requestRef?: string
  result?: string
  toolKind?: string
}

function flag(argv: readonly string[], name: string): string | undefined {
  const index = argv.indexOf(name)
  return index < 0 ? undefined : argv[index + 1]
}

function emissions(calls: readonly (readonly string[])[]): Emission[] {
  return calls.map((argv) => ({
    argv,
    type: flag(argv, "--type") ?? "",
    emitterEventId: flag(argv, "--emitter-event-id") ?? "",
    turnRef: flag(argv, "--turn-ref"),
    toolRef: flag(argv, "--tool-ref"),
    requestRef: flag(argv, "--request-ref"),
    result: flag(argv, "--result"),
    toolKind: flag(argv, "--tool-kind"),
  }))
}

function installFixture(on: On): (readonly string[])[] {
  const calls: (readonly string[])[] = []
  mock.clock(on, { now: FIXTURE.now })
  on("session.id", () => ({ value: FIXTURE.sessionRef }))
  on("process.run", ($, e) => {
    calls.push(e.argv)
    return { value: PROCESS_OK }
  })
  return calls
}

async function settle(): Promise<void> {
  for (let index = 0; index < 12; index += 1) await Promise.resolve()
}

function expectContentFree(calls: readonly (readonly string[])[]): void {
  const wire = calls.flat().join("\n")
  expect(wire).not.toContain("CANARY_")
  for (const argv of calls) {
    expect(argv.slice(0, 3)).toEqual(["gifford", "adapter", "emit"])
    expect(flag(argv, "--harness")).toBe("claude-code")
    expect(flag(argv, "--harness-version")).toBe("2.1.316")
    expect(flag(argv, "--emitter-type")).toBe("mod")
    expect(flag(argv, "--schema-version")).toBe("2")
    expect(flag(argv, "--session-ref")).toBe(FIXTURE.sessionRef)
  }
}

test("passes lifecycle results through and suppresses reload duplicates", async ($, on) => {
  const calls = installFixture(on)
  on("session.start", ($, e) => ({ cwd: `${e.cwd}/core` }))
  on("session.end", ($, e) => ({ sessionId: e.sessionId }))
  mock.session(on)

  const startInput = {
    cwd: "/fixture",
    surface: null,
    isInteractive: false,
  } as const
  const first = await $.session.start(startInput)
  const reloaded = await $.session.start(startInput)
  expect(first).toEqual({ cwd: "/fixture/core" })
  expect(reloaded).toEqual(first)

  const appendInput = {
    message: {
      type: "user",
      role: "user",
      content: [{ type: "text", text: FIXTURE.canaryAppend }],
    },
    door: "prompt",
    origin: { kind: "composer" },
    uuid: "row-316-fixture",
  } as const
  const appended = await $.session.append(appendInput)
  expect(appended).toEqual({
    message: appendInput.message,
    uuid: appendInput.uuid,
  })
  const ended = await $.session.end({
    reason: "clear",
    sessionId: FIXTURE.sessionRef,
    resume: { id: FIXTURE.sessionRef },
  })
  expect(ended).toEqual({ sessionId: FIXTURE.sessionRef })

  await settle()
  const projected = emissions(calls)
  expect(projected.map((event) => event.type)).toEqual([
    "session.started",
    "session.ended",
  ])
  expect(projected[0]?.emitterEventId).toMatch(
    /^ccm-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
  )
  expectContentFree(calls)
})

test("preserves the turn.step generator without reading its chunks", async ($, on) => {
  const calls = installFixture(on)
  const chunks = [
    { kind: "text", index: 0, text: FIXTURE.canaryPrompt },
    { kind: "text", index: 0, text: FIXTURE.canaryAppend },
  ] as const
  on("turn.step", async function* () {
    yield chunks[0]
    yield chunks[1]
    return TURN_STEP_RESULT
  })

  const stream = $.turn.step({
    turnId: FIXTURE.turnRef,
    index: 0,
    model: "fixture-model",
    messageCount: 1,
  })
  expect(await stream.next()).toEqual({ done: false, value: chunks[0] })
  expect(await stream.next()).toEqual({ done: false, value: chunks[1] })
  expect(await stream.next()).toEqual({ done: true, value: TURN_STEP_RESULT })

  await settle()
  const projected = emissions(calls)
  expect(projected.map((event) => event.type)).toEqual(["turn.streaming"])
  expect(projected[0]?.turnRef).toBe(FIXTURE.turnRef)
  expectContentFree(calls)
})

test("projects tool decisions with bounded approval identity", async ($, on) => {
  const calls = installFixture(on)
  const decisions = ["ask", "ask", "allow", "deny"] as const
  let decision = 0
  on("turn.start", ($, e) => ({ turnId: e.turnId }))
  on("tool.check", () => ({ decision: decisions[decision++] ?? "deny" }))

  await $.turn.start({ text: FIXTURE.canaryPrompt, turnId: FIXTURE.turnRef })
  const check = {
    tool: "Bash",
    input: { command: FIXTURE.canaryToolInput },
    tool_use_id: FIXTURE.toolRef,
  } as const
  expect(await $.tool.check(check)).toEqual({ decision: "ask" })
  expect(await $.tool.check(check)).toEqual({ decision: "ask" })
  expect(await $.tool.check(check)).toEqual({ decision: "allow" })
  expect(
    await $.tool.check({ ...check, tool_use_id: "tool-denied-fixture" }),
  ).toEqual({ decision: "deny" })

  await settle()
  const projected = emissions(calls).filter((event) =>
    event.type.startsWith("approval.") || event.type.startsWith("tool."),
  )
  expect(projected.map((event) => event.type)).toEqual([
    "approval.requested",
    "approval.resolved",
    "tool.started",
    "tool.failed",
  ])
  expect(projected[0]?.requestRef).toMatch(
    /^perm-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
  )
  expect(projected[1]?.requestRef).toBe(projected[0]?.requestRef)
  expect(projected[1]?.result).toBe("ok")
  expect(projected[2]).toMatchObject({
    turnRef: FIXTURE.turnRef,
    toolRef: FIXTURE.toolRef,
    toolKind: "shell",
  })
  expect(projected[3]).toMatchObject({
    toolRef: "tool-denied-fixture",
    result: "denied",
  })
  expectContentFree(calls)
})

test("resolves an ask from the correlated terminal tool result", async ($, on) => {
  const calls = installFixture(on)
  on("turn.start", ($, e) => ({ turnId: e.turnId }))
  on("tool.check", () => ({ decision: "ask" }))
  on("tool.call", () => ({
    result: undefined,
    ref: 1,
    text: FIXTURE.canaryToolOutput,
    isError: true,
  }))

  await $.turn.start({ text: FIXTURE.canaryPrompt, turnId: FIXTURE.turnRef })
  expect(
    await $.tool.check({
      tool: "Bash",
      input: { command: FIXTURE.canaryToolInput },
      tool_use_id: FIXTURE.toolRef,
    }),
  ).toEqual({ decision: "ask" })
  const result = await $.tool.call({
    tool: "Bash",
    command: FIXTURE.canaryToolInput,
    tool_use_id: FIXTURE.toolRef,
  })
  expect(result).toMatchObject({ ref: 1, isError: true })

  await settle()
  const projected = emissions(calls).filter((event) =>
    event.type.startsWith("approval.") || event.type.startsWith("tool."),
  )
  expect(projected.map((event) => event.type)).toEqual([
    "approval.requested",
    "approval.resolved",
    "tool.failed",
  ])
  expect(projected[1]).toMatchObject({
    requestRef: projected[0]?.requestRef,
    turnRef: FIXTURE.turnRef,
    result: "failed",
  })
  expect(projected[2]).toMatchObject({
    toolRef: FIXTURE.toolRef,
    turnRef: FIXTURE.turnRef,
    result: "failed",
  })
  expectContentFree(calls)
})

test("does not resolve an ask from another tool result", async ($, on) => {
  const calls = installFixture(on)
  const otherToolRef = "tool-other-fixture"
  let checked = false
  on("turn.start", ($, e) => ({ turnId: e.turnId }))
  on("tool.check", () => {
    const decision = checked ? "allow" : "ask"
    checked = true
    return { decision }
  })
  on("tool.call", () => ({
    result: {
      stdout: FIXTURE.canaryToolOutput,
      stderr: "",
      interrupted: false,
    },
    ref: 2,
    text: FIXTURE.canaryToolOutput,
  }))

  await $.turn.start({ text: FIXTURE.canaryPrompt, turnId: FIXTURE.turnRef })
  expect(
    await $.tool.check({
      tool: "Bash",
      input: { command: FIXTURE.canaryToolInput },
      tool_use_id: FIXTURE.toolRef,
    }),
  ).toEqual({ decision: "ask" })
  await $.tool.call({
    tool: "Bash",
    command: FIXTURE.canaryToolInput,
    tool_use_id: otherToolRef,
  })

  await settle()
  const projected = emissions(calls).filter((event) =>
    event.type.startsWith("approval.") || event.type.startsWith("tool."),
  )
  expect(projected.map((event) => event.type)).toEqual([
    "approval.requested",
    "tool.started",
    "tool.completed",
  ])
  expect(projected[2]).toMatchObject({
    toolRef: otherToolRef,
    turnRef: FIXTURE.turnRef,
    result: "ok",
  })
  expectContentFree(calls)
})

test("projects tool and question completion without their content", async ($, on) => {
  const calls = installFixture(on)
  let questionRef = ""
  let markQuestionStarted = (ref: string) => ref
  const questionStarted = new Promise<string>((resolve) => {
    markQuestionStarted = (ref) => {
      resolve(ref)
      return ref
    }
  })
  let releaseQuestion = () => {}
  const questionHeld = new Promise<void>((resolve) => {
    releaseQuestion = resolve
  })
  on("turn.start", ($, e) => ({ turnId: e.turnId }))
  on("ui.render", () => ({ type: "engine", ref: 0 }))
  on("tool.call", async ($, e) => {
    if (e.tool === "AskUserQuestion") {
      questionRef = e.tool_use_id
      markQuestionStarted(questionRef)
      await questionHeld
      return {
        result: {
          questions: [],
          answers: { fixture: FIXTURE.canaryToolOutput },
        },
        ref: "question-result-fixture",
        text: FIXTURE.canaryToolOutput,
      }
    }
    return {
      result: {
        content: [{ type: "text", text: FIXTURE.canaryToolOutput }],
        isError: false,
      },
      ref: "tool-result-fixture",
      text: FIXTURE.canaryToolOutput,
    }
  })

  await $.turn.start({ text: FIXTURE.canaryPrompt, turnId: FIXTURE.turnRef })
  const toolResult = await $.tool.call({
    tool: "mcp__fixture__run",
    secret: FIXTURE.canaryToolInput,
  })
  expect(toolResult.text).toBe(FIXTURE.canaryToolOutput)
  const pendingQuestion = $.tool.call({
    tool: "AskUserQuestion",
    questions: [
      {
        question: FIXTURE.canaryPrompt,
        header: "Fixture",
        options: [
          { label: "One", description: "First" },
          { label: "Two", description: "Second" },
        ],
        multiSelect: false,
      },
    ],
  })
  questionRef = await questionStarted
  expect(questionRef).toMatch(/^toolu_/)
  expect(questionRef).toMatch(/^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$/)
  const renderInput = {
    surface: "terminal",
    component: "AskUserQuestion",
    requestId: questionRef,
    props: { tool: "AskUserQuestion", questions: [] },
  } as const
  await $.ui.render(renderInput)
  await $.ui.render(renderInput)
  releaseQuestion()
  const questionResult = await pendingQuestion
  expect(questionResult.text).toBe(FIXTURE.canaryToolOutput)

  await settle()
  const projected = emissions(calls).filter((event) =>
    event.type.startsWith("tool.") || event.type.startsWith("question."),
  )
  expect(projected.map((event) => event.type)).toEqual([
    "tool.started",
    "tool.completed",
    "question.requested",
    "question.resolved",
  ])
  expect(projected[2]?.requestRef).toBe(questionRef)
  expect(projected[3]).toMatchObject({
    requestRef: questionRef,
    result: "ok",
  })
  expectContentFree(calls)
})

test("maps aborted turns and successful spawns without prompt content", async ($, on) => {
  const calls = installFixture(on)
  on("turn.start", ($, e) => ({ turnId: e.turnId }))
  on("agent.spawn", () => ({ model: "fixture-model", agentId: FIXTURE.agentRef }))
  on("turn.complete", () => ({ text: FIXTURE.canaryToolOutput }))

  await $.turn.start({ text: FIXTURE.canaryPrompt, turnId: FIXTURE.turnRef })
  const spawned = await $.agent.spawn({
    tool_use_id: "spawn-tool-fixture",
    prompt: FIXTURE.canaryPrompt,
    description: FIXTURE.canaryPrompt,
    subagentType: "general-purpose",
    provider: { plugin: "engine", tier: "core" },
    parentModel: "fixture-model",
    background: false,
    fork: false,
  })
  expect(spawned).toEqual({ model: "fixture-model", agentId: FIXTURE.agentRef })
  const completed = await $.turn.complete({
    answer: FIXTURE.canaryToolOutput,
    durationMs: 10,
    isAborted: true,
    turnId: FIXTURE.turnRef,
    agentId: FIXTURE.agentRef,
    reason: "aborted",
  })
  expect(completed).toEqual({ text: FIXTURE.canaryToolOutput })

  await settle()
  const projected = emissions(calls).filter((event) =>
    event.type.startsWith("subagent.") || event.type === "turn.completed",
  )
  expect(projected.map((event) => event.type)).toEqual([
    "subagent.started",
    "turn.completed",
  ])
  expect(projected[1]).toMatchObject({
    turnRef: FIXTURE.turnRef,
    result: "cancelled",
  })
  expectContentFree(calls)
})

test("remains fail-open when event emission fails", async ($, on) => {
  mock.clock(on, { now: FIXTURE.now })
  on("session.id", () => ({ value: FIXTURE.sessionRef }))
  on("process.run", () => ({ deny: "fixture process failure" }))
  on("turn.start", () => ({ turnId: "core-result-fixture" }))

  const result = await $.turn.start({
    text: FIXTURE.canaryPrompt,
    turnId: FIXTURE.turnRef,
  })
  expect(result).toEqual({ turnId: "core-result-fixture" })
})

test("bounds the asynchronous process queue", { timeoutMs: 10_000 }, async ($, on) => {
  mock.clock(on, { now: FIXTURE.now })
  on("session.id", () => ({ value: FIXTURE.sessionRef }))
  on("turn.start", ($, e) => ({ turnId: e.turnId }))

  let releaseFirst = () => {}
  const firstHeld = new Promise<void>((resolve) => {
    releaseFirst = resolve
  })
  let markDrained = () => {}
  const drained = new Promise<void>((resolve) => {
    markDrained = resolve
  })
  let processCalls = 0
  on("process.run", async () => {
    processCalls += 1
    if (processCalls === 1) await firstHeld
    if (processCalls === 65) markDrained()
    return { value: PROCESS_OK }
  })

  for (let index = 0; index < 80; index += 1) {
    await $.turn.start({
      text: FIXTURE.canaryPrompt,
      turnId: `turn-queue-${index}`,
    })
  }
  expect(processCalls).toBe(1)
  releaseFirst()
  await drained
  await settle()
  expect(processCalls).toBe(65)
})
