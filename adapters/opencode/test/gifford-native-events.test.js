import assert from "node:assert/strict"
import { chmod, mkdtemp, rm } from "node:fs/promises"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { setTimeout as delay } from "node:timers/promises"
import test from "node:test"

import { GiffordNativeEvents } from "../gifford-native-events.js"

const socketEnvironment = "GIFFORD_ADAPTER_SOCKET"
const maxMessageBytes = 4096

const encode = (message) => {
  const body = Buffer.from(JSON.stringify(message))
  const frame = Buffer.alloc(4 + body.length)
  frame.writeUInt32BE(body.length, 0)
  body.copy(frame, 4)
  return frame
}

const waitFor = async (condition, description, timeout = 3000) => {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (condition()) return
    await delay(10)
  }
  assert.fail(`timed out waiting for ${description}`)
}

class FakeReceiver {
  constructor({ dropFirstEvent = false, rejectFirstHelloReason } = {}) {
    this.dropFirstEvent = dropFirstEvent
    this.rejectFirstHelloReason = rejectFirstHelloReason
    this.dropped = false
    this.rejectedHello = false
    this.hellos = []
    this.attempts = []
    this.events = []
    this.sockets = new Set()
    this.directory = undefined
    this.path = undefined
    this.server = undefined
  }

  async start() {
    this.directory = await mkdtemp(join(tmpdir(), "gifford-opencode-"))
    this.path = join(this.directory, "native-events.sock")
    this.server = createServer((socket) => this.accept(socket))
    await new Promise((resolve, reject) => {
      this.server.once("error", reject)
      this.server.listen(this.path, resolve)
    })
    await chmod(this.path, 0o600)
  }

  accept(socket) {
    this.sockets.add(socket)
    let buffer = Buffer.alloc(0)
    socket.on("close", () => this.sockets.delete(socket))
    socket.on("error", () => {})
    socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, Buffer.from(chunk)])
      while (buffer.length >= 4) {
        const size = buffer.readUInt32BE(0)
        assert.ok(size <= maxMessageBytes)
        if (buffer.length < size + 4) return
        const body = buffer.subarray(4, size + 4)
        buffer = buffer.subarray(size + 4)
        this.receive(socket, JSON.parse(body.toString("utf8")))
      }
    })
  }

  receive(socket, message) {
    if (message.op === "hello") {
      this.hellos.push(message)
      if (this.rejectFirstHelloReason && !this.rejectedHello) {
        this.rejectedHello = true
        socket.write(
          encode({
            op: "hello_result",
            accepted: false,
            reason: this.rejectFirstHelloReason,
          }),
        )
        return
      }
      socket.write(encode({ op: "hello_result", accepted: true }))
      return
    }
    assert.equal(message.op, "event")
    this.attempts.push(message)
    if (this.dropFirstEvent && !this.dropped) {
      this.dropped = true
      socket.destroy()
      return
    }
    this.events.push(message)
    socket.write(
      encode({
        op: "ack",
        seq: message.seq,
        emitter_event_id: message.emitter_event_id,
      }),
    )
  }

  async close() {
    for (const socket of this.sockets) socket.destroy()
    if (this.server) {
      await new Promise((resolve) => this.server.close(resolve))
    }
    if (this.directory) await rm(this.directory, { recursive: true, force: true })
  }
}

const withSocketEnvironment = async (path, action) => {
  const previous = process.env[socketEnvironment]
  if (path === undefined) delete process.env[socketEnvironment]
  else process.env[socketEnvironment] = path
  try {
    return await action()
  } finally {
    if (previous === undefined) delete process.env[socketEnvironment]
    else process.env[socketEnvironment] = previous
  }
}

const forbiddenPluginInput = new Proxy(
  {},
  {
    get() {
      assert.fail("the observe-only plugin must not use the OpenCode client")
    },
  },
)

const startPlugin = async (receiver) => {
  const hooks = await withSocketEnvironment(receiver.path, () => GiffordNativeEvents(forbiddenPluginInput))
  await waitFor(() => receiver.hellos.length >= 1, "native hello")
  return hooks
}

const eventHook = (hooks, type, properties) => hooks.event({ event: { type, properties } })

test("does nothing when the native socket variable is absent", async () => {
  const hooks = await withSocketEnvironment(undefined, () => GiffordNativeEvents(forbiddenPluginInput))
  assert.deepEqual(hooks, {})
})

test("projects each observed OpenCode lifecycle row without free text", async (t) => {
  const receiver = new FakeReceiver()
  await receiver.start()
  const hooks = await startPlugin(receiver)
  assert.deepEqual(receiver.hellos[0], {
    op: "hello",
    harness: "opencode",
    harness_version: "1.18.29",
    emitter_type: "plugin",
    emitter_version: "0.1.0",
    schema_version: 2,
  })

  const capture = async (invoke, count = 1) => {
    const offset = receiver.events.length
    await invoke()
    await waitFor(() => receiver.events.length >= offset + count, `${count} projected event(s)`)
    return receiver.events.slice(offset, offset + count)
  }

  const prepareBusy = async (sessionID) => {
    await capture(() => eventHook(hooks, "session.status", { sessionID, status: { type: "busy" } }))
  }

  const cases = [
    {
      name: "session created",
      invoke: () => eventHook(hooks, "session.created", { info: { id: "ses-created", title: "SECRET_TITLE" } }),
      want: { type: "session.started", session_ref: "ses-created" },
    },
    {
      name: "prompt submitted",
      invoke: () =>
        hooks["chat.message"](
          { sessionID: "ses-prompt", messageID: "msg-prompt" },
          { message: { content: "SECRET_PROMPT" }, parts: [{ text: "SECRET_PROMPT" }] },
        ),
      want: { type: "prompt.submitted", session_ref: "ses-prompt" },
    },
    {
      name: "turn started",
      invoke: () => eventHook(hooks, "session.status", { sessionID: "ses-turn-start", status: { type: "busy" } }),
      want: { type: "turn.started", session_ref: "ses-turn-start" },
      check: (event) => assert.ok(event.turn_ref),
    },
    {
      name: "turn completed",
      prepare: () => prepareBusy("ses-turn-complete"),
      invoke: () => eventHook(hooks, "session.status", { sessionID: "ses-turn-complete", status: { type: "idle" } }),
      want: { type: "turn.completed", session_ref: "ses-turn-complete" },
      check: (event) => assert.ok(event.turn_ref),
    },
    {
      name: "step started streams once",
      prepare: () => prepareBusy("ses-step-start"),
      invoke: () =>
        eventHook(hooks, "message.part.updated", {
          part: { id: "part-start", sessionID: "ses-step-start", messageID: "msg-step", type: "step-start" },
          delta: "SECRET_DELTA",
        }),
      want: { type: "turn.streaming", session_ref: "ses-step-start" },
    },
    {
      name: "step finished streams once",
      prepare: () => prepareBusy("ses-step-finish"),
      invoke: () =>
        eventHook(hooks, "message.part.updated", {
          part: {
            id: "part-finish",
            sessionID: "ses-step-finish",
            messageID: "msg-step",
            type: "step-finish",
            reason: "SECRET_REASON",
          },
        }),
      want: { type: "turn.streaming", session_ref: "ses-step-finish" },
    },
    {
      name: "tool before starts a known shell tool",
      prepare: () => prepareBusy("ses-tool-start"),
      invoke: () =>
        hooks["tool.execute.before"](
          { tool: "bash", sessionID: "ses-tool-start", callID: "call-shell" },
          { args: { secret: "SECRET_ARGUMENT" } },
        ),
      want: { type: "tool.started", session_ref: "ses-tool-start", tool_ref: "call-shell", tool_kind: "shell" },
    },
    {
      name: "unknown tool kind is omitted",
      prepare: () => prepareBusy("ses-tool-unknown"),
      invoke: () =>
        hooks["tool.execute.before"](
          { tool: "custom-tool", sessionID: "ses-tool-unknown", callID: "call-unknown" },
          { args: {} },
        ),
      want: { type: "tool.started", session_ref: "ses-tool-unknown", tool_ref: "call-unknown" },
      check: (event) => assert.equal("tool_kind" in event, false),
    },
    {
      name: "tool after completes the same tool",
      prepare: async () => {
        await prepareBusy("ses-tool-complete")
        await capture(() =>
          hooks["tool.execute.before"](
            { tool: "read", sessionID: "ses-tool-complete", callID: "call-complete" },
            { args: {} },
          ),
        )
      },
      invoke: () =>
        hooks["tool.execute.after"](
          {
            tool: "read",
            sessionID: "ses-tool-complete",
            callID: "call-complete",
            args: { secret: "SECRET_ARGUMENT" },
          },
          { title: "SECRET_TITLE", output: "SECRET_OUTPUT", metadata: {} },
        ),
      want: { type: "tool.completed", session_ref: "ses-tool-complete", tool_ref: "call-complete", result: "ok" },
    },
    {
      name: "tool part completion",
      prepare: async () => {
        await prepareBusy("ses-part-complete")
        await capture(() =>
          eventHook(hooks, "message.part.updated", {
            part: {
              id: "part-tool-complete",
              sessionID: "ses-part-complete",
              messageID: "msg-tool",
              type: "tool",
              callID: "call-part-complete",
              tool: "edit",
              state: { status: "running", input: { secret: "SECRET_ARGUMENT" } },
            },
          }),
        )
      },
      invoke: () =>
        eventHook(hooks, "message.part.updated", {
          part: {
            id: "part-tool-complete",
            sessionID: "ses-part-complete",
            messageID: "msg-tool",
            type: "tool",
            callID: "call-part-complete",
            tool: "edit",
            state: { status: "completed", output: "SECRET_OUTPUT", title: "SECRET_TITLE", metadata: {} },
          },
        }),
      want: { type: "tool.completed", session_ref: "ses-part-complete", tool_ref: "call-part-complete", result: "ok" },
    },
    {
      name: "tool part error",
      prepare: async () => {
        await prepareBusy("ses-tool-error")
        await capture(() =>
          eventHook(hooks, "message.part.updated", {
            part: {
              id: "part-tool-error",
              sessionID: "ses-tool-error",
              messageID: "msg-tool",
              type: "tool",
              callID: "call-error",
              tool: "write",
              state: { status: "running", input: {} },
            },
          }),
        )
      },
      invoke: () =>
        eventHook(hooks, "message.part.updated", {
          part: {
            id: "part-tool-error",
            sessionID: "ses-tool-error",
            messageID: "msg-tool",
            type: "tool",
            callID: "call-error",
            tool: "write",
            state: { status: "error", error: "SECRET_ERROR" },
          },
        }),
      want: { type: "tool.failed", session_ref: "ses-tool-error", tool_ref: "call-error", result: "failed" },
    },
    {
      name: "permission asked",
      prepare: () => prepareBusy("ses-permission"),
      invoke: () =>
        eventHook(hooks, "permission.asked", {
          id: "per-native",
          sessionID: "ses-permission",
          permission: "SECRET_PERMISSION",
          patterns: ["SECRET_PATTERN"],
        }),
      want: { type: "approval.requested", session_ref: "ses-permission", request_ref: "per-native" },
    },
    {
      name: "permission replied",
      invoke: () =>
        eventHook(hooks, "permission.replied", {
          requestID: "per-native",
          sessionID: "ses-permission",
          reply: "once",
        }),
      want: { type: "approval.resolved", session_ref: "ses-permission", request_ref: "per-native", result: "ok" },
    },
    {
      name: "permission rejected",
      invoke: () =>
        eventHook(hooks, "permission.replied", {
          requestID: "per-denied",
          sessionID: "ses-permission",
          reply: "reject",
        }),
      want: { type: "approval.resolved", session_ref: "ses-permission", request_ref: "per-denied", result: "denied" },
    },
    {
      name: "question asked",
      invoke: () =>
        eventHook(hooks, "question.asked", {
          id: "que-native",
          sessionID: "ses-question",
          questions: [{ header: "SECRET_QUESTION", question: "SECRET_QUESTION" }],
        }),
      want: { type: "question.requested", session_ref: "ses-question", request_ref: "que-native" },
    },
    {
      name: "question replied",
      invoke: () =>
        eventHook(hooks, "question.replied", {
          requestID: "que-native",
          sessionID: "ses-question",
          answers: [["SECRET_ANSWER"]],
        }),
      want: { type: "question.resolved", session_ref: "ses-question", request_ref: "que-native", result: "ok" },
    },
    {
      name: "question rejected",
      invoke: () => eventHook(hooks, "question.rejected", { requestID: "que-denied", sessionID: "ses-question" }),
      want: { type: "question.resolved", session_ref: "ses-question", request_ref: "que-denied", result: "denied" },
    },
    {
      name: "compaction started",
      invoke: () =>
        hooks["experimental.session.compacting"](
          { sessionID: "ses-compact" },
          { context: ["SECRET_CONTEXT"] },
        ),
      want: { type: "context.compaction_started", session_ref: "ses-compact" },
    },
    {
      name: "compaction completed",
      invoke: () => eventHook(hooks, "session.compacted", { sessionID: "ses-compact" }),
      want: { type: "context.compaction_completed", session_ref: "ses-compact" },
    },
    {
      name: "toast observed as a notification",
      invoke: () => eventHook(hooks, "tui.toast.show", { message: "SECRET_TOAST" }),
      want: { type: "notification.received" },
    },
    {
      name: "session error closes the turn",
      invoke: () => eventHook(hooks, "session.error", { sessionID: "ses-error", error: { message: "SECRET_ERROR" } }),
      want: { type: "turn.completed", session_ref: "ses-error", result: "failed" },
    },
    {
      name: "session deleted",
      invoke: () => eventHook(hooks, "session.deleted", { info: { id: "ses-ended", title: "SECRET_TITLE" } }),
      want: { type: "session.ended", session_ref: "ses-ended" },
    },
  ]

  try {
    for (const row of cases) {
      await t.test(row.name, async () => {
        await row.prepare?.()
        const [event] = await capture(row.invoke)
        for (const [key, value] of Object.entries(row.want)) assert.equal(event[key], value)
        row.check?.(event)
      })
    }

    const longRequestID = `request with forbidden separators/${"x".repeat(100)}`
    const [longRequest] = await capture(() =>
      eventHook(hooks, "permission.asked", { id: longRequestID, sessionID: "ses-long-request" }),
    )
    const [longResolution] = await capture(() =>
      eventHook(hooks, "permission.replied", {
        requestID: longRequestID,
        sessionID: "ses-long-request",
        reply: "once",
      }),
    )
    assert.equal(longRequest.request_ref, longResolution.request_ref)
    assert.ok(Buffer.byteLength(longRequest.request_ref) <= 64)
    assert.equal(longRequest.request_ref.includes("forbidden"), false)

    const eventCount = receiver.events.length
    await eventHook(hooks, "message.part.delta", {
      sessionID: "ses-step-start",
      partID: "part-start",
      delta: "SECRET_DELTA",
    })
    await eventHook(hooks, "message.part.updated", {
      part: { id: "part-start", sessionID: "ses-step-start", messageID: "msg-step", type: "step-start" },
      delta: "SECRET_DELTA",
    })
    await delay(30)
    assert.equal(receiver.events.length, eventCount)

    const allowedKeys = new Set([
      "op",
      "seq",
      "emitter_event_id",
      "type",
      "observed_at",
      "session_ref",
      "turn_ref",
      "tool_ref",
      "request_ref",
      "result",
      "tool_kind",
    ])
    for (const event of receiver.events) {
      assert.ok(Object.keys(event).every((key) => allowedKeys.has(key)))
      for (const key of ["session_ref", "turn_ref", "tool_ref", "request_ref", "emitter_event_id"]) {
        if (event[key] !== undefined) assert.ok(Buffer.byteLength(event[key]) <= 64)
      }
    }
    assert.deepEqual(
      receiver.events.map((event) => event.seq),
      receiver.events.map((_, index) => index + 1),
    )
    assert.equal(
      new Set(receiver.events.map((event) => event.emitter_event_id)).size,
      receiver.events.length,
    )
    const wire = JSON.stringify(receiver.events)
    for (const secret of [
      "SECRET_PROMPT",
      "SECRET_ARGUMENT",
      "SECRET_OUTPUT",
      "SECRET_QUESTION",
      "SECRET_ANSWER",
      "SECRET_ERROR",
      "SECRET_TOAST",
      "SECRET_CONTEXT",
      "SECRET_DELTA",
      "SECRET_PATTERN",
      "SECRET_PERMISSION",
      "SECRET_TITLE",
    ]) {
      assert.equal(wire.includes(secret), false)
    }
  } finally {
    await hooks.dispose()
    await receiver.close()
  }
})

test("reconnects with sequence one and retries the same unacknowledged event ID", async () => {
  const receiver = new FakeReceiver({ dropFirstEvent: true })
  await receiver.start()
  const hooks = await startPlugin(receiver)
  try {
    await eventHook(hooks, "session.created", { info: { id: "ses-reconnect" } })
    await waitFor(() => receiver.attempts.length >= 2 && receiver.events.length >= 1, "retried acknowledged event")
    assert.equal(receiver.hellos.length, 2)
    assert.equal(receiver.attempts[0].seq, 1)
    assert.equal(receiver.attempts[1].seq, 1)
    assert.equal(receiver.attempts[0].emitter_event_id, receiver.attempts[1].emitter_event_id)
    assert.equal(receiver.events.length, 1)

    await hooks["chat.message"]({ sessionID: "ses-reconnect", messageID: "msg-after-reconnect" }, {})
    await waitFor(() => receiver.events.length >= 2, "event after reconnect")
    assert.equal(receiver.events[1].seq, 2)
    assert.notEqual(receiver.events[0].emitter_event_id, receiver.events[1].emitter_event_id)
  } finally {
    await hooks.dispose()
    await receiver.close()
  }
})

test("retries a hello while process observation is incomplete", async () => {
  const receiver = new FakeReceiver({ rejectFirstHelloReason: "observation_incomplete" })
  await receiver.start()
  const hooks = await startPlugin(receiver)
  try {
    await waitFor(() => receiver.hellos.length >= 2, "hello retry")
    await eventHook(hooks, "session.created", { info: { id: "ses-observation-retry" } })
    await waitFor(() => receiver.events.length >= 1, "event after hello retry")
    assert.equal(receiver.events[0].seq, 1)
  } finally {
    await hooks.dispose()
    await receiver.close()
  }
})

test("never waits for an unavailable receiver", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gifford-opencode-missing-"))
  const path = join(directory, "missing.sock")
  const hooks = await withSocketEnvironment(path, () => GiffordNativeEvents(forbiddenPluginInput))
  try {
    const started = performance.now()
    await eventHook(hooks, "session.created", { info: { id: "ses-no-receiver" } })
    assert.ok(performance.now() - started < 100)
  } finally {
    await hooks.dispose()
    await rm(directory, { recursive: true, force: true })
  }
})
