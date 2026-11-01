import type { EngineInterface, On } from "claude-code"

const HARNESS_VERSION = "2.1.316"
const EMITTER_VERSION = "1"
const MAX_QUEUE = 64
const MAX_SEEN_QUESTIONS = 64
const PROCESS_TIMEOUT_MS = 1000
const QUESTION_TOOL = "AskUserQuestion"
const MAIN_LOOP = "main"
const STATE = {
  plugin: "gifford-native-observer",
  key: "observations",
} as const

type NativeType =
  | "session.started"
  | "session.ended"
  | "turn.started"
  | "turn.streaming"
  | "turn.completed"
  | "tool.started"
  | "tool.completed"
  | "tool.failed"
  | "approval.requested"
  | "approval.resolved"
  | "question.requested"
  | "question.resolved"
  | "subagent.started"

type NativeResult = "ok" | "failed" | "cancelled" | "denied"
type ToolKind = "shell" | "file" | "search" | "web" | "mcp" | "other"

type Observation = {
  emitterEventId: string
  type: NativeType
  observedAt: number
  sessionRef: string
  turnRef?: string
  toolRef?: string
  requestRef?: string
  result?: NativeResult
  toolKind?: ToolKind
}

type ObservationContext = {
  observedAt: number
  sessionRef: string
}

type PendingApproval = {
  requestRef: string
  turnRef?: string
}

const queue: Observation[] = []
const activeTurns = new Map<string, string>()
const toolTurns = new Map<string, string | undefined>()
const pendingApprovals = new Map<string, PendingApproval>()
const startedTools = new Set<string>()
const seenQuestions = new Set<string>()
let draining = false
let cachedSessionRef = ""

function nativeRef(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined
  return /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$/.test(value)
    ? value
    : undefined
}

function loopKey(agentId: unknown): string | undefined {
  if (agentId === undefined) return MAIN_LOOP
  const ref = nativeRef(agentId)
  return ref === undefined ? undefined : `agent:${ref}`
}

function turnFor(agentId: unknown): string | undefined {
  const key = loopKey(agentId)
  return key === undefined ? undefined : activeTurns.get(key)
}

function toolKind(tool: string): ToolKind {
  if (tool === "Bash" || tool === "PowerShell") return "shell"
  if (
    tool === "Read" ||
    tool === "Edit" ||
    tool === "Write" ||
    tool === "NotebookEdit"
  ) {
    return "file"
  }
  if (tool === "Glob" || tool === "Grep") return "search"
  if (tool === "WebFetch" || tool === "WebSearch") return "web"
  if (tool.startsWith("mcp__")) return "mcp"
  return "other"
}

function eventId(prefix: "ccm" | "perm"): string {
  return `${prefix}-${crypto.randomUUID()}`
}

function command(observation: Observation): string[] {
  const argv = [
    "gifford",
    "adapter",
    "emit",
    "--harness",
    "claude-code",
    "--harness-version",
    HARNESS_VERSION,
    "--emitter-type",
    "mod",
    "--emitter-version",
    EMITTER_VERSION,
    "--schema-version",
    "2",
    "--seq",
    "1",
    "--emitter-event-id",
    observation.emitterEventId,
    "--type",
    observation.type,
    "--observed-at",
    String(observation.observedAt),
    "--session-ref",
    observation.sessionRef,
  ]
  if (observation.turnRef !== undefined) {
    argv.push("--turn-ref", observation.turnRef)
  }
  if (observation.toolRef !== undefined) {
    argv.push("--tool-ref", observation.toolRef)
  }
  if (observation.requestRef !== undefined) {
    argv.push("--request-ref", observation.requestRef)
  }
  if (observation.result !== undefined) {
    argv.push("--result", observation.result)
  }
  if (observation.toolKind !== undefined) {
    argv.push("--tool-kind", observation.toolKind)
  }
  return argv
}

async function drain($: EngineInterface): Promise<void> {
  if (draining) return
  draining = true
  try {
    while (queue.length > 0) {
      const observation = queue.shift()
      if (observation === undefined) continue
      try {
        await $.process.run(command(observation), {
          timeoutMs: PROCESS_TIMEOUT_MS,
        })
      } catch {
        // Observation must never change Claude Code's control flow.
      }
    }
  } finally {
    draining = false
    if (queue.length > 0) void drain($)
  }
}

function enqueue($: EngineInterface, observation: Omit<Observation, "emitterEventId">): void {
  try {
    if (queue.length >= MAX_QUEUE) return
    queue.push({ ...observation, emitterEventId: eventId("ccm") })
    void drain($)
  } catch {
    // A broken observer is silent and fail-open.
  }
}

async function currentSessionRef($: EngineInterface): Promise<string | undefined> {
  if (cachedSessionRef !== "") return cachedSessionRef
  const sessionRef = nativeRef(await $.session.id())
  if (sessionRef !== undefined) cachedSessionRef = sessionRef
  return sessionRef
}

async function context($: EngineInterface): Promise<ObservationContext | undefined> {
  try {
    const observedAt = Math.trunc(await $.clock.now())
    const sessionRef = await currentSessionRef($)
    if (!Number.isSafeInteger(observedAt) || observedAt < 1 || sessionRef === undefined) {
      return undefined
    }
    return { observedAt, sessionRef }
  } catch {
    return undefined
  }
}

async function claimSessionStart(
  $: EngineInterface,
  sessionRef: string,
): Promise<boolean> {
  try {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const held = await $.state.get(STATE)
      if (held.value?.sessionRef === sessionRef) return false
      const result = await $.state.set(
        STATE,
        { sessionRef },
        { ifVersion: held.version },
      )
      if (result.isSet) return true
    }
  } catch {
    // The state API failing must not block the session.
  }
  return false
}

function clearTurn(turnRef: string): void {
  for (const [toolRef, candidate] of toolTurns) {
    if (candidate !== turnRef) continue
    toolTurns.delete(toolRef)
    startedTools.delete(toolRef)
    pendingApprovals.delete(toolRef)
  }
}

function clearSession(): void {
  cachedSessionRef = ""
  activeTurns.clear()
  toolTurns.clear()
  pendingApprovals.clear()
  startedTools.clear()
  seenQuestions.clear()
}

async function observeSessionStart($: EngineInterface): Promise<void> {
  const seen = await context($)
  if (seen === undefined) return
  if (!(await claimSessionStart($, seen.sessionRef))) return
  enqueue($, { ...seen, type: "session.started" })
}

async function observeSessionEnd(
  $: EngineInterface,
  sessionId: unknown,
): Promise<void> {
  try {
    const observedAt = Math.trunc(await $.clock.now())
    const sessionRef = nativeRef(sessionId)
    if (!Number.isSafeInteger(observedAt) || observedAt < 1 || sessionRef === undefined) {
      return
    }
    enqueue($, { type: "session.ended", observedAt, sessionRef })
  } catch {
    // Observation is optional; session shutdown is not.
  }
}

async function observeTurn(
  $: EngineInterface,
  type: "turn.started" | "turn.streaming",
  turnId: unknown,
): Promise<void> {
  const turnRef = nativeRef(turnId)
  const seen = await context($)
  if (turnRef === undefined || seen === undefined) return
  enqueue($, { ...seen, type, turnRef })
}

async function observeTurnComplete(
  $: EngineInterface,
  turnId: unknown,
  reason: unknown,
): Promise<void> {
  const turnRef = nativeRef(turnId)
  const seen = await context($)
  if (turnRef === undefined || seen === undefined) return
  const result: NativeResult =
    reason === "aborted"
      ? "cancelled"
      : reason === "answer"
        ? "ok"
        : "failed"
  enqueue($, { ...seen, type: "turn.completed", turnRef, result })
}

async function observeToolCheck(
  $: EngineInterface,
  tool: unknown,
  toolUseId: unknown,
  agentId: unknown,
  decision: unknown,
): Promise<void> {
  if (tool === QUESTION_TOOL || typeof tool !== "string") return
  const toolRef = nativeRef(toolUseId)
  const seen = await context($)
  if (toolRef === undefined || seen === undefined) return
  const turnRef = toolTurns.get(toolRef) ?? turnFor(agentId)
  if (decision === "ask") {
    if (pendingApprovals.has(toolRef)) return
    const requestRef = eventId("perm")
    pendingApprovals.set(toolRef, { requestRef, turnRef })
    enqueue($, {
      ...seen,
      type: "approval.requested",
      turnRef,
      requestRef,
    })
    return
  }
  if (decision !== "allow" && decision !== "deny") return
  const pending = pendingApprovals.get(toolRef)
  if (pending !== undefined) {
    pendingApprovals.delete(toolRef)
    enqueue($, {
      ...seen,
      type: "approval.resolved",
      turnRef: pending.turnRef,
      requestRef: pending.requestRef,
      result: decision === "allow" ? "ok" : "denied",
    })
  }
  if (decision === "deny") {
    enqueue($, {
      ...seen,
      type: "tool.failed",
      turnRef,
      toolRef,
      result: "denied",
    })
    return
  }
  if (startedTools.has(toolRef)) return
  startedTools.add(toolRef)
  enqueue($, {
    ...seen,
    type: "tool.started",
    turnRef,
    toolRef,
    toolKind: toolKind(tool),
  })
}

async function observeToolResult(
  $: EngineInterface,
  tool: unknown,
  toolUseId: unknown,
  agentId: unknown,
  denied: boolean,
  failed: boolean,
  executed: boolean,
): Promise<void> {
  const toolRef = nativeRef(toolUseId)
  const seen = await context($)
  if (toolRef === undefined || seen === undefined) return
  if (!executed) {
    startedTools.delete(toolRef)
    toolTurns.delete(toolRef)
    return
  }
  const turnRef = toolTurns.get(toolRef) ?? turnFor(agentId)
  const result: NativeResult = denied ? "denied" : failed ? "failed" : "ok"
  const pending = pendingApprovals.get(toolRef)
  if (pending !== undefined) {
    pendingApprovals.delete(toolRef)
    enqueue($, {
      ...seen,
      type: "approval.resolved",
      turnRef: pending.turnRef,
      requestRef: pending.requestRef,
      result,
    })
  }
  if (tool === QUESTION_TOOL) {
    enqueue($, {
      ...seen,
      type: "question.resolved",
      turnRef,
      requestRef: toolRef,
      result,
    })
  } else {
    enqueue($, {
      ...seen,
      type: failed || denied ? "tool.failed" : "tool.completed",
      turnRef,
      toolRef,
      result,
    })
  }
  startedTools.delete(toolRef)
  toolTurns.delete(toolRef)
}

async function observeAgentSpawn(
  $: EngineInterface,
  parentAgentId: unknown,
  agentId: unknown,
): Promise<void> {
  if (nativeRef(agentId) === undefined) return
  const seen = await context($)
  if (seen === undefined) return
  enqueue($, {
    ...seen,
    type: "subagent.started",
    turnRef: turnFor(parentAgentId),
  })
}

async function observeQuestion(
  $: EngineInterface,
  requestId: unknown,
): Promise<void> {
  const requestRef = nativeRef(requestId)
  const seen = await context($)
  if (requestRef === undefined || seen === undefined) return
  if (seenQuestions.has(requestRef)) return
  if (seenQuestions.size >= MAX_SEEN_QUESTIONS) {
    const oldest = seenQuestions.values().next().value
    if (oldest !== undefined) seenQuestions.delete(oldest)
  }
  seenQuestions.add(requestRef)
  enqueue($, {
    ...seen,
    type: "question.requested",
    turnRef: toolTurns.get(requestRef),
    requestRef,
  })
}

export function register(on: On): void {
  on("session.start", async ($, e, next) => {
    await observeSessionStart($)
    return next(e)
  })

  on("session.append", ($, e, next) => next(e)).catch(($, e, next) => next(e))

  on("session.end", async ($, e, next) => {
    await observeSessionEnd($, e.sessionId)
    clearSession()
    return next(e)
  })

  on("turn.start", async ($, e, next) => {
    const key = loopKey(e.agentId)
    const turnRef = nativeRef(e.turnId)
    if (key !== undefined && turnRef !== undefined) activeTurns.set(key, turnRef)
    await observeTurn($, "turn.started", e.turnId)
    return next(e)
  })

  on("turn.step", async function* ($, e, next) {
    await observeTurn($, "turn.streaming", e.turnId)
    yield* next(e)
  })

  on("turn.complete", async ($, e, next) => {
    await observeTurnComplete($, e.turnId, e.reason)
    const key = loopKey(e.agentId)
    const turnRef = nativeRef(e.turnId)
    if (key !== undefined) activeTurns.delete(key)
    if (turnRef !== undefined) clearTurn(turnRef)
    return next(e)
  })

  on("tool.call", async ($, e, next) => {
    const toolRef = nativeRef(e.tool_use_id)
    if (toolRef !== undefined) toolTurns.set(toolRef, turnFor(e.agentId))
    const result = await next(e)
    await observeToolResult(
      $,
      e.tool,
      e.tool_use_id,
      e.agentId,
      result.deny !== undefined,
      result.isError === true,
      result.ref !== undefined,
    )
    return result
  }).catch(($, e, next) => next(e))

  on("tool.check", async ($, e, next) => {
    const result = await next(e)
    await observeToolCheck(
      $,
      e.tool,
      e.tool_use_id,
      e.agentId,
      result.decision,
    )
    return result
  }).catch(($, e, next) => next(e))

  on("agent.spawn", async ($, e, next) => {
    const result = await next(e)
    await observeAgentSpawn($, e.parentAgentId, result.agentId)
    return result
  }).catch(($, e, next) => next(e))

  on(
    "ui.render",
    { component: "AskUserQuestion" },
    async ($, e, next) => {
      await observeQuestion($, e.requestId)
      return next(e)
    },
  )
}
