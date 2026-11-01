export const FIXTURE = {
  now: 1_798_779_600_000,
  sessionRef: "session-316-fixture",
  turnRef: "turn-316-fixture",
  agentRef: "agent-316-fixture",
  toolRef: "tool-316-fixture",
  questionRef: "question-316-fixture",
  canaryPrompt: "CANARY_PROMPT_CONTENT",
  canaryAppend: "CANARY_APPEND_CONTENT",
  canaryToolInput: "CANARY_TOOL_INPUT",
  canaryToolOutput: "CANARY_TOOL_OUTPUT",
} as const

export const PROCESS_OK = {
  exitCode: 0,
  stdout: "",
  stderr: "",
  isStdoutTruncated: false,
  isStderrTruncated: false,
} as const

export const TURN_STEP_RESULT = {
  turnId: FIXTURE.turnRef,
  index: 0,
  answer: FIXTURE.canaryToolOutput,
  toolUses: [],
  stopReason: "end_turn",
  usage: null,
} as const
