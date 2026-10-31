const forwardedEvents = new Set([
  "session.created",
  "permission.asked",
  "permission.replied",
  "question.asked",
  "question.replied",
  "question.rejected",
  "session.error",
])

const owns = (object, key) => Object.prototype.hasOwnProperty.call(object, key)
const object = (value) => value !== null && typeof value === "object" && !Array.isArray(value)

const copy = (from, to, key) => {
  if (owns(from, key)) to[key] = from[key]
}

const project = (event) => {
  if (!object(event) || !forwardedEvents.has(event.type) || !object(event.properties)) return
  const input = event.properties
  const properties = {}

  switch (event.type) {
    case "session.created": {
      copy(input, properties, "sessionID")
      if (owns(input, "info")) {
        if (object(input.info)) {
          const info = {}
          copy(input.info, info, "id")
          if (owns(input.info, "parentID")) {
            info.parentID = input.info.parentID === undefined ? null : input.info.parentID
          }
          properties.info = info
        } else {
          properties.info = input.info
        }
      }
      break
    }
    case "permission.asked":
      copy(input, properties, "id")
      copy(input, properties, "sessionID")
      if (input.permission === "question") properties.tool_name = "question"
      break
    case "permission.replied":
      copy(input, properties, "requestID")
      copy(input, properties, "sessionID")
      copy(input, properties, "reply")
      break
    case "question.asked":
      copy(input, properties, "id")
      copy(input, properties, "sessionID")
      break
    case "question.replied":
    case "question.rejected":
      copy(input, properties, "requestID")
      copy(input, properties, "sessionID")
      break
    case "session.error":
      copy(input, properties, "sessionID")
      if (object(input.error)) {
        const error = {}
        copy(input.error, error, "name")
        properties.error = error
      }
      break
  }
  return { type: event.type, properties }
}

const forward = async (event) => {
  const child = Bun.spawn(["gifford", "label", "hook", "opencode"], {
    stdin: "pipe",
    stdout: "ignore",
    stderr: "ignore",
  })
  child.stdin.write(JSON.stringify(event))
  child.stdin.end()
  let timer
  await Promise.race([
    child.exited,
    new Promise((resolve) => {
      timer = setTimeout(() => {
        try {
          child.kill()
        } catch {}
        resolve()
      }, 250)
    }),
  ])
  clearTimeout(timer)
}

export const GiffordLabelBridge = async () => ({
  event: async ({ event }) => {
    try {
      const projected = project(event)
      if (projected) await forward(projected)
    } catch {}
  },
})
