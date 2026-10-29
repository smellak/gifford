"""The thin Glimmer endpoint: the internal/gateway wire format in front of
a vLLM OpenAI-compatible server.

Pure logic, standard library only, so it is tested without a GPU, vLLM or
Modal. It never logs a payload, a prompt, a screen or an answer.
"""

from __future__ import annotations

import hmac
import json
import math
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

# The labels of ANALYZE_AGENT_STATE, by the letter the v1 system message
# gives each, A to G.
LETTERS: dict[str, str] = {
    "A": "IDLE_AT_PROMPT",
    "B": "THINKING_STREAMING",
    "C": "RUNNING_TOOL",
    "D": "WAITING_APPROVAL",
    "E": "ASKING_QUESTION",
    "F": "ERROR_OR_EXITED",
    "G": "UNKNOWN",
}
UNKNOWN = "UNKNOWN"

# The prompt versions whose answers this endpoint can read.
KNOWN_PROMPTS = frozenset({"analyze_agent_state.v1"})

ANALYZE = "ANALYZE_AGENT_STATE"
CONTRACTS = frozenset({ANALYZE, "TRIAGE_MESSAGE", "CHOOSE_ORCHESTRATION_ACTION"})

# Below this much probability on the seven letters, the answer is UNKNOWN.
MIN_LETTER_MASS = 0.5

MAX_BODY_BYTES = 1 << 20
MAX_EVIDENCE = 3
MAX_EVIDENCE_CHARS = 120

# Generation parameters: the first token is the letter, read from its
# logprobs; the rest is the short evidence.
CHAT_PARAMS: dict[str, Any] = {
    "max_tokens": 40,
    "temperature": 0,
    "logprobs": True,
    "top_logprobs": 20,
}

REQUEST_FIELDS = frozenset(
    {"contract", "inference_profile", "adapter_name", "adapter_revision", "prompt_version", "messages", "payload"}
)


class EndpointError(Exception):
    """A refused request. Its message is fixed text, never request content."""

    def __init__(self, status: int, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


class ChatBackend(Protocol):
    """An OpenAI-compatible chat completions call (vLLM in production)."""

    def chat(self, body: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class Settings:
    """What the endpoint serves. Read from configuration, never hard-coded."""

    token: str
    base_model: str
    adapter_name: str
    adapter_revision: str
    lora_module: str
    runtime_version: str
    gpu: str
    chat_template_kwargs: dict[str, Any] = field(default_factory=lambda: {"enable_thinking": False})


def authorised(settings: Settings, header: str | None) -> bool:
    """Checks the Bearer credential in constant time. An empty configured
    token authorises nobody."""
    if not settings.token or not header or not header.startswith("Bearer "):
        return False
    return hmac.compare_digest(header[len("Bearer ") :].encode(), settings.token.encode())


def parse_request(body: bytes) -> dict[str, Any]:
    if len(body) > MAX_BODY_BYTES:
        raise EndpointError(413, "request too large")
    try:
        req = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise EndpointError(400, "request is not JSON") from None
    if not isinstance(req, dict) or set(req) - REQUEST_FIELDS:
        raise EndpointError(400, "request has an unknown shape")
    if req.get("contract") not in CONTRACTS:
        raise EndpointError(400, "unknown contract")
    if not isinstance(req.get("payload"), dict):
        raise EndpointError(400, "payload must be an object")
    return req


def select_model(settings: Settings, req: dict[str, Any]) -> str:
    """The served model for the request's inference profile."""
    profile = req.get("inference_profile")
    if profile == "base":
        if "adapter_name" in req or "adapter_revision" in req:
            raise EndpointError(400, "base takes no adapter")
        return settings.base_model
    if profile == "analyst-lora":
        if req.get("adapter_name") != settings.adapter_name or req.get("adapter_revision") != settings.adapter_revision:
            raise EndpointError(400, "adapter not served")
        return settings.lora_module
    raise EndpointError(400, "unknown inference profile")


def check_messages(req: dict[str, Any]) -> list[dict[str, str]]:
    if req.get("prompt_version") not in KNOWN_PROMPTS:
        raise EndpointError(400, "unknown prompt version")
    messages = req.get("messages")
    if not isinstance(messages, list) or not 1 <= len(messages) <= 8:
        raise EndpointError(400, "messages missing")
    for m in messages:
        if (
            not isinstance(m, dict)
            or set(m) != {"role", "content"}
            or m["role"] not in ("system", "user")
            or not isinstance(m["content"], str)
            or not m["content"]
        ):
            raise EndpointError(400, "bad message")
    return messages


def is_special(token: str) -> bool:
    """A special or empty token is never the label: chat markers such as
    <|start|>, and whitespace-only tokens."""
    t = token.strip()
    return t == "" or (t.startswith("<") and t.endswith(">"))


def letter_of(token: str) -> str | None:
    """The letter a token spells, folding the variants a tokenizer gives
    it: "A", " A", "▁A", "ĠA"."""
    t = token.strip().lstrip("▁Ġ")
    return t if t in LETTERS else None


def letter_probabilities(content: list[dict[str, Any]]) -> tuple[dict[str, float], float]:
    """At the label position, the first non-special token, sums the
    probability of each letter over its variants and renormalises over
    A-G. Returns the distribution by label name and the letter mass."""
    for pos in content:
        if is_special(str(pos.get("token", ""))):
            continue
        candidates = list(pos.get("top_logprobs") or [])
        if not any(c.get("token") == pos.get("token") for c in candidates):
            candidates.append({"token": pos.get("token"), "logprob": pos.get("logprob")})
        mass: dict[str, float] = dict.fromkeys(LETTERS, 0.0)
        for c in candidates:
            letter = letter_of(str(c.get("token", "")))
            lp = c.get("logprob")
            if letter is None or not isinstance(lp, (int, float)) or math.isnan(lp) or lp > 0:
                continue
            mass[letter] += math.exp(lp)
        total = sum(mass.values())
        if total <= 0:
            return {}, 0.0
        return {LETTERS[k]: v / total for k, v in mass.items()}, min(total, 1.0)
    return {}, 0.0


def parse_evidence(text: str) -> list[str]:
    """What follows the first "|", split on ";": at most three phrases of
    1 to 120 characters, one line each."""
    _, sep, rest = text.partition("|")
    if not sep:
        return []
    out: list[str] = []
    for phrase in rest.split(";"):
        phrase = " ".join(phrase.split())
        if not phrase:
            continue
        out.append(phrase[:MAX_EVIDENCE_CHARS])
        if len(out) == MAX_EVIDENCE:
            break
    return out


def answer(settings: Settings, backend: ChatBackend, req: dict[str, Any]) -> dict[str, Any]:
    """Runs one validated request and builds the wire response."""
    model = select_model(settings, req)
    if req["contract"] != ANALYZE:
        raise EndpointError(501, "contract not served yet")
    messages = check_messages(req)
    body = {"model": model, "messages": messages, **CHAT_PARAMS}
    if settings.chat_template_kwargs:
        body["chat_template_kwargs"] = settings.chat_template_kwargs
    try:
        out = backend.chat(body)
        choice = out["choices"][0]
        content = choice["logprobs"]["content"]
        text = choice["message"]["content"] or ""
    except Exception:  # noqa: BLE001 - any backend failure is one fixed error
        raise EndpointError(502, "model call failed") from None
    probs, mass = letter_probabilities(content)
    if mass < MIN_LETTER_MASS or not probs:
        label = UNKNOWN
        if not probs:
            probs = {UNKNOWN: 1.0}
    else:
        label = max(LETTERS.values(), key=lambda name: probs[name])
    return {
        "label": label,
        "probabilities": probs,
        "evidence": parse_evidence(text),
        "request_id": "req-" + uuid.uuid4().hex,
        "runtime_version": settings.runtime_version[:50],
        "gpu": settings.gpu[:50],
    }


def handle(settings: Settings, backend: ChatBackend, authorization: str | None, body: bytes) -> tuple[int, bytes]:
    """One HTTP exchange: status and JSON body. Error bodies carry a fixed
    reason only."""
    if not authorised(settings, authorization):
        return 401, json.dumps({"error": "unauthorised"}).encode()
    try:
        resp = answer(settings, backend, parse_request(body))
    except EndpointError as e:
        return e.status, json.dumps({"error": e.reason}).encode()
    return 200, json.dumps(resp, ensure_ascii=False).encode()
