import { createHash, randomBytes } from "node:crypto"
import { createConnection } from "node:net"

const socketEnvironment = "GIFFORD_ADAPTER_SOCKET"
const harnessVersion = "1.18.29"
const emitterVersion = "0.1.0"
const schemaVersion = 2
const maxMessageBytes = 4096
const maxQueuedEvents = 256
const connectTimeoutMilliseconds = 500
const acknowledgementTimeoutMilliseconds = 800
const maxReconnectAttempts = 6

const nativeEventTypes = new Set([
  "session.started",
  "session.ended",
  "prompt.submitted",
  "turn.started",
  "turn.streaming",
  "turn.completed",
  "tool.started",
  "tool.completed",
  "tool.failed",
  "approval.requested",
  "approval.resolved",
  "question.requested",
  "question.resolved",
  "notification.received",
  "context.compaction_started",
  "context.compaction_completed",
])

const nativeResults = new Set(["ok", "failed", "cancelled", "denied"])
const nativeToolKinds = new Set(["shell", "file", "search", "web", "mcp", "other"])
const helloRejectionReasons = new Set([
  "unsupported_schema",
  "harness_mismatch",
  "peer_not_in_tree",
  "observation_incomplete",
])
const opaqueReference = /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$/

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value)

const own = (value, key) => isObject(value) && Object.prototype.hasOwnProperty.call(value, key)

const stringField = (value, key) => (own(value, key) && typeof value[key] === "string" ? value[key] : undefined)

const hasOnlyKeys = (value, keys) => isObject(value) && Object.keys(value).every((key) => keys.has(key))

const boundedReference = (value, prefix) => {
  if (typeof value !== "string" || value.length === 0) return
  if (Buffer.byteLength(value) <= 64 && opaqueReference.test(value)) return value
  try {
    return `${prefix}-${createHash("sha256").update(value).digest("hex").slice(0, 48)}`
  } catch {}
}

const runIdentity = () => {
  try {
    return randomBytes(12).toString("hex")
  } catch {
    return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 12)}`
  }
}

const encodeMessage = (message) => {
  try {
    const body = Buffer.from(JSON.stringify(message))
    if (body.length > maxMessageBytes) return
    const frame = Buffer.allocUnsafe(4 + body.length)
    frame.writeUInt32BE(body.length, 0)
    body.copy(frame, 4)
    return frame
  } catch {}
}

class NativeEmitter {
  constructor(path) {
    this.path = path
    this.run = runIdentity()
    this.nextEvent = 0
    this.nextSequence = 0
    this.queue = []
    this.socket = undefined
    this.buffer = Buffer.alloc(0)
    this.active = undefined
    this.state = "idle"
    this.closed = false
    this.retryAttempt = 0
    this.connectTimer = undefined
    this.acknowledgementTimer = undefined
    this.retryTimer = undefined
    this.connect()
  }

  emit(observation) {
    try {
      if (this.closed || !nativeEventTypes.has(observation.type)) return
      if (this.queue.length >= maxQueuedEvents) {
        // Continuing after an unobservable local drop could restore native
        // authority with an incomplete lifecycle. Closing leaves the PTY
        // fallback authoritative for the rest of this plugin run.
        this.close()
        return
      }
      const observedAt = Date.now()
      if (!Number.isSafeInteger(observedAt) || observedAt < 1) return
      const event = {
        op: "event",
        emitter_event_id: `oc-${this.run}-${(++this.nextEvent).toString(36)}`,
        type: observation.type,
        observed_at: observedAt,
      }
      for (const [source, destination] of [
        ["sessionRef", "session_ref"],
        ["turnRef", "turn_ref"],
        ["toolRef", "tool_ref"],
        ["requestRef", "request_ref"],
      ]) {
        if (opaqueReference.test(observation[source] ?? "")) event[destination] = observation[source]
      }
      if (nativeResults.has(observation.result)) event.result = observation.result
      if (nativeToolKinds.has(observation.toolKind)) event.tool_kind = observation.toolKind
      this.queue.push(event)
      this.pump()
    } catch {}
  }

  close() {
    this.closed = true
    this.queue.length = 0
    this.clearTimers()
    const socket = this.socket
    this.socket = undefined
    this.active = undefined
    this.state = "closed"
    try {
      socket?.destroy()
    } catch {}
  }

  clearTimers() {
    clearTimeout(this.connectTimer)
    clearTimeout(this.acknowledgementTimer)
    clearTimeout(this.retryTimer)
    this.connectTimer = undefined
    this.acknowledgementTimer = undefined
    this.retryTimer = undefined
  }

  connect() {
    if (this.closed || this.socket) return
    let socket
    try {
      socket = createConnection({ path: this.path })
    } catch {
      this.scheduleReconnect()
      return
    }
    this.socket = socket
    this.buffer = Buffer.alloc(0)
    this.active = undefined
    this.nextSequence = 0
    this.state = "connecting"
    socket.unref?.()
    this.connectTimer = setTimeout(() => this.disconnect(socket), connectTimeoutMilliseconds)
    this.connectTimer.unref?.()
    socket.once("connect", () => {
      if (this.socket !== socket || this.closed) return
      this.state = "hello"
      const sent = this.write(socket, {
        op: "hello",
        harness: "opencode",
        harness_version: harnessVersion,
        emitter_type: "plugin",
        emitter_version: emitterVersion,
        schema_version: schemaVersion,
      })
      if (!sent) this.disconnect(socket)
    })
    socket.on("data", (chunk) => this.read(socket, chunk))
    socket.on("error", () => this.disconnect(socket))
    socket.on("close", () => this.disconnect(socket))
  }

  scheduleReconnect() {
    if (this.closed || this.retryTimer) return
    if (this.retryAttempt >= maxReconnectAttempts) {
      this.close()
      return
    }
    const delay = 50 * 2 ** this.retryAttempt++
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined
      this.connect()
    }, delay)
    this.retryTimer.unref?.()
  }

  disconnect(socket) {
    if (this.socket !== socket) return
    clearTimeout(this.connectTimer)
    clearTimeout(this.acknowledgementTimer)
    this.connectTimer = undefined
    this.acknowledgementTimer = undefined
    this.socket = undefined
    this.buffer = Buffer.alloc(0)
    this.active = undefined
    this.state = "idle"
    try {
      socket.destroy()
    } catch {}
    this.scheduleReconnect()
  }

  write(socket, message) {
    const frame = encodeMessage(message)
    if (!frame) return false
    try {
      socket.write(frame)
      return true
    } catch {
      return false
    }
  }

  read(socket, chunk) {
    if (this.socket !== socket || this.closed) return
    try {
      this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)])
      while (this.buffer.length >= 4) {
        const size = this.buffer.readUInt32BE(0)
        if (size > maxMessageBytes) {
          this.disconnect(socket)
          return
        }
        if (this.buffer.length < 4 + size) return
        const body = this.buffer.subarray(4, 4 + size)
        this.buffer = this.buffer.subarray(4 + size)
        const message = JSON.parse(body.toString("utf8"))
        if (!this.receive(socket, message)) return
      }
    } catch {
      this.disconnect(socket)
    }
  }

  receive(socket, message) {
    if (!isObject(message)) {
      this.disconnect(socket)
      return false
    }
    if (this.state === "hello") {
      if (message.op !== "hello_result" || typeof message.accepted !== "boolean") {
        this.close()
        return false
      }
      if (message.accepted === false) {
        if (
          !hasOnlyKeys(message, new Set(["op", "accepted", "reason"])) ||
          !helloRejectionReasons.has(message.reason)
        ) {
          this.close()
          return false
        }
        if (message.reason === "observation_incomplete") this.disconnect(socket)
        else this.close()
        return false
      }
      if (!hasOnlyKeys(message, new Set(["op", "accepted"]))) {
        this.close()
        return false
      }
      clearTimeout(this.connectTimer)
      this.connectTimer = undefined
      this.state = "ready"
      this.pump()
      return true
    }
    if (
      this.state !== "ack" ||
      !this.active ||
      !hasOnlyKeys(message, new Set(["op", "seq", "emitter_event_id"])) ||
      message.op !== "ack"
    ) {
      this.disconnect(socket)
      return false
    }
    if (message.seq !== this.active.sequence || message.emitter_event_id !== this.active.event.emitter_event_id) {
      this.disconnect(socket)
      return false
    }
    clearTimeout(this.acknowledgementTimer)
    this.acknowledgementTimer = undefined
    this.retryAttempt = 0
    if (this.queue[0] === this.active.event) this.queue.shift()
    this.active = undefined
    this.state = "ready"
    this.pump()
    return true
  }

  pump() {
    if (this.closed || this.state !== "ready" || this.active || this.queue.length === 0) return
    const event = this.queue[0]
    const sequence = ++this.nextSequence
    const message = { ...event, seq: sequence }
    this.active = { event, sequence }
    this.state = "ack"
    if (!this.write(this.socket, message)) {
      this.disconnect(this.socket)
      return
    }
    const socket = this.socket
    this.acknowledgementTimer = setTimeout(
      () => this.disconnect(socket),
      acknowledgementTimeoutMilliseconds,
    )
    this.acknowledgementTimer.unref?.()
  }
}

const knownToolKinds = new Map([
  ["bash", "shell"],
  ["shell", "shell"],
  ["read", "file"],
  ["write", "file"],
  ["edit", "file"],
  ["patch", "file"],
  ["apply_patch", "file"],
  ["grep", "search"],
  ["glob", "search"],
  ["search", "search"],
  ["webfetch", "web"],
  ["web_fetch", "web"],
  ["websearch", "web"],
  ["web_search", "web"],
])

class OpenCodeProjector {
  constructor(emitter) {
    this.emitter = emitter
    this.sessions = new Map()
  }

  session(value) {
    return boundedReference(value, "session")
  }

  state(sessionRef) {
    if (!sessionRef) return
    let state = this.sessions.get(sessionRef)
    if (!state) {
      state = {
        started: false,
        busy: false,
        turn: undefined,
        turnNumber: 0,
        steps: new Set(),
        tools: new Map(),
        prompts: new Set(),
      }
      this.sessions.set(sessionRef, state)
    }
    return state
  }

  fields(sessionRef) {
    const turnRef = this.state(sessionRef)?.turn
    return { sessionRef, turnRef }
  }

  startTurn(sessionRef) {
    const state = this.state(sessionRef)
    if (!state || state.busy) return state?.turn
    state.busy = true
    state.turnNumber++
    state.turn = boundedReference(`turn:${sessionRef}:${state.turnNumber}`, "turn")
    state.steps.clear()
    state.tools.clear()
    this.emitter.emit({ type: "turn.started", sessionRef, turnRef: state.turn })
    return state.turn
  }

  completeTurn(sessionRef, result) {
    const state = this.state(sessionRef)
    if (!state) {
      this.emitter.emit({ type: "turn.completed", result })
      return
    }
    if (!state.busy && state.turn) return
    this.emitter.emit({ type: "turn.completed", sessionRef, turnRef: state.turn, result })
    state.busy = false
  }

  toolStart(sessionRef, callID, tool) {
    const toolRef = boundedReference(callID, "tool")
    if (!toolRef) return
    const state = this.state(sessionRef)
    const previous = state?.tools.get(toolRef)
    if (previous?.started) return
    const lifecycle = previous ?? { started: false, terminal: false }
    lifecycle.started = true
    state?.tools.set(toolRef, lifecycle)
    this.emitter.emit({
      type: "tool.started",
      ...this.fields(sessionRef),
      toolRef,
      toolKind: typeof tool === "string" ? knownToolKinds.get(tool.toLowerCase()) : undefined,
    })
  }

  toolFinish(sessionRef, callID, tool, type, result) {
    const toolRef = boundedReference(callID, "tool")
    if (!toolRef) return
    const state = this.state(sessionRef)
    let lifecycle = state?.tools.get(toolRef)
    if (!lifecycle?.started) {
      this.toolStart(sessionRef, callID, tool)
      lifecycle = state?.tools.get(toolRef)
    }
    if (lifecycle?.terminal) return
    if (lifecycle) lifecycle.terminal = true
    this.emitter.emit({ type, ...this.fields(sessionRef), toolRef, result })
  }

  streamStep(part) {
    const sessionRef = this.session(stringField(part, "sessionID"))
    const state = this.state(sessionRef)
    const partID = boundedReference(stringField(part, "id"), "step")
    if (!state || !partID) return
    const key = `${part.type}:${partID}`
    if (state.steps.has(key)) return
    state.steps.add(key)
    this.emitter.emit({ type: "turn.streaming", ...this.fields(sessionRef) })
  }

  toolPart(part) {
    const sessionRef = this.session(stringField(part, "sessionID"))
    const callID = stringField(part, "callID")
    const tool = stringField(part, "tool")
    const status = stringField(part.state, "status")
    if (status === "pending" || status === "running") {
      this.toolStart(sessionRef, callID, tool)
    } else if (status === "completed") {
      this.toolFinish(sessionRef, callID, tool, "tool.completed", "ok")
    } else if (status === "error") {
      this.toolFinish(sessionRef, callID, tool, "tool.failed", "failed")
    }
  }

  event(event) {
    if (!isObject(event) || typeof event.type !== "string" || !isObject(event.properties)) return
    const properties = event.properties
    switch (event.type) {
      case "session.created": {
        const sessionRef = this.session(stringField(properties, "sessionID") ?? stringField(properties.info, "id"))
        const state = this.state(sessionRef)
        if (!state || state.started) return
        state.started = true
        this.emitter.emit({ type: "session.started", sessionRef })
        return
      }
      case "session.deleted": {
        const sessionRef = this.session(stringField(properties, "sessionID") ?? stringField(properties.info, "id"))
        if (!sessionRef) return
        this.emitter.emit({ type: "session.ended", ...this.fields(sessionRef) })
        this.sessions.delete(sessionRef)
        return
      }
      case "session.status": {
        const sessionRef = this.session(stringField(properties, "sessionID"))
        const status = stringField(properties.status, "type")
        if (status === "busy") this.startTurn(sessionRef)
        if (status === "idle") this.completeTurn(sessionRef)
        return
      }
      case "session.idle":
        this.completeTurn(this.session(stringField(properties, "sessionID")))
        return
      case "session.error":
        this.completeTurn(this.session(stringField(properties, "sessionID")), "failed")
        return
      case "message.part.updated": {
        const part = properties.part
        if (!isObject(part)) return
        if (part.type === "step-start" || part.type === "step-finish") this.streamStep(part)
        if (part.type === "tool") this.toolPart(part)
        return
      }
      case "permission.asked": {
        const sessionRef = this.session(stringField(properties, "sessionID"))
        const requestRef = boundedReference(stringField(properties, "id"), "request")
        if (!requestRef) return
        this.emitter.emit({ type: "approval.requested", ...this.fields(sessionRef), requestRef })
        return
      }
      case "permission.replied": {
        const sessionRef = this.session(stringField(properties, "sessionID"))
        const requestRef = boundedReference(
          stringField(properties, "requestID") ?? stringField(properties, "permissionID"),
          "request",
        )
        if (!requestRef) return
        const reply = (stringField(properties, "reply") ?? stringField(properties, "response"))?.toLowerCase()
        let result
        if (reply === "once" || reply === "always" || reply === "allow" || reply === "allowed") result = "ok"
        if (reply === "reject" || reply === "deny" || reply === "denied") result = "denied"
        this.emitter.emit({ type: "approval.resolved", ...this.fields(sessionRef), requestRef, result })
        return
      }
      case "question.asked": {
        const sessionRef = this.session(stringField(properties, "sessionID"))
        const requestRef = boundedReference(stringField(properties, "id"), "request")
        if (!requestRef) return
        this.emitter.emit({ type: "question.requested", ...this.fields(sessionRef), requestRef })
        return
      }
      case "question.replied":
      case "question.rejected": {
        const sessionRef = this.session(stringField(properties, "sessionID"))
        const requestRef = boundedReference(stringField(properties, "requestID"), "request")
        if (!requestRef) return
        this.emitter.emit({
          type: "question.resolved",
          ...this.fields(sessionRef),
          requestRef,
          result: event.type === "question.replied" ? "ok" : "denied",
        })
        return
      }
      case "session.compacted": {
        const sessionRef = this.session(stringField(properties, "sessionID"))
        this.emitter.emit({ type: "context.compaction_completed", ...this.fields(sessionRef) })
        return
      }
      case "tui.toast.show":
        this.emitter.emit({ type: "notification.received" })
    }
  }

  chatMessage(input) {
    const sessionRef = this.session(stringField(input, "sessionID"))
    const state = this.state(sessionRef)
    if (!state) return
    const messageID = boundedReference(stringField(input, "messageID"), "message")
    if (messageID && state.prompts.has(messageID)) return
    if (messageID) {
      if (state.prompts.size >= 256) state.prompts.delete(state.prompts.values().next().value)
      state.prompts.add(messageID)
    }
    this.emitter.emit({ type: "prompt.submitted", sessionRef })
  }

  beforeTool(input) {
    this.toolStart(
      this.session(stringField(input, "sessionID")),
      stringField(input, "callID"),
      stringField(input, "tool"),
    )
  }

  afterTool(input) {
    this.toolFinish(
      this.session(stringField(input, "sessionID")),
      stringField(input, "callID"),
      stringField(input, "tool"),
      "tool.completed",
      "ok",
    )
  }

  compacting(input) {
    const sessionRef = this.session(stringField(input, "sessionID"))
    this.emitter.emit({ type: "context.compaction_started", ...this.fields(sessionRef) })
  }
}

export const GiffordNativeEvents = async () => {
  try {
    const socketPath = process.env[socketEnvironment]
    if (typeof socketPath !== "string" || socketPath.length === 0) return {}
    const emitter = new NativeEmitter(socketPath)
    const projector = new OpenCodeProjector(emitter)
    return {
      dispose: async () => {
        try {
          emitter.close()
        } catch {}
      },
      event: async ({ event } = {}) => {
        try {
          projector.event(event)
        } catch {}
      },
      "chat.message": async (input) => {
        try {
          projector.chatMessage(input)
        } catch {}
      },
      "tool.execute.before": async (input) => {
        try {
          projector.beforeTool(input)
        } catch {}
      },
      "tool.execute.after": async (input) => {
        try {
          projector.afterTool(input)
        } catch {}
      },
      "experimental.session.compacting": async (input) => {
        try {
          projector.compacting(input)
        } catch {}
      },
    }
  } catch {
    return {}
  }
}
