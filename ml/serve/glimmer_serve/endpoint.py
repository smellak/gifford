"""The thin Glimmer endpoint: the internal/gateway wire format in front of
a vLLM OpenAI-compatible server.

Pure logic, standard library only, so it is tested without a GPU, vLLM or
Modal. It never logs a payload, a prompt, a screen or an answer.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import math
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

ANALYZE = "ANALYZE_AGENT_STATE"
TRIAGE = "TRIAGE_MESSAGE"
CHOOSE = "CHOOSE_ORCHESTRATION_ACTION"
CONTRACTS = frozenset({ANALYZE, TRIAGE, CHOOSE})

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

# The labels of TRIAGE_MESSAGE, by the letter the v1 system message gives
# each, A to D: the contract's table order.
TRIAGE_LETTERS: dict[str, str] = {
    "A": "NORMAL",
    "B": "CONFLICT_OR_URGENT",
    "C": "LOW_PRIORITY",
    "D": "REQUIRES_HUMAN",
}

# A CHOOSE_ORCHESTRATION_ACTION request letters its own allowed_actions, in
# their order, A first, as the v1 prompt does: at least two actions, and at
# most one per letter.
CHOOSE_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
MIN_ACTIONS = 2

# The prompt version each contract's answers are read under.
PROMPTS: dict[str, str] = {
    ANALYZE: "analyze_agent_state.v1",
    TRIAGE: "triage_message.v1",
    CHOOSE: "choose_orchestration_action.v1",
}
KNOWN_PROMPTS = frozenset(PROMPTS.values())

# Below this much probability on its letters, an ANALYZE_AGENT_STATE answer
# is UNKNOWN. A TRIAGE_MESSAGE or CHOOSE_ORCHESTRATION_ACTION answer carries
# its letter mass instead, and the gateway refuses one below the same bar.
MIN_LETTER_MASS = 0.5

MAX_BODY_BYTES = 1 << 20
MAX_EVIDENCE = 3
MAX_EVIDENCE_CHARS = 120

# Generation parameters: the first token is the letter, read from its
# logprobs; the rest is the short evidence. Special tokens stay in the
# output, so the Muse Glimmer reasoning parser sees the tokens that frame
# any reasoning.
CHAT_PARAMS: dict[str, Any] = {
    "max_tokens": 40,
    "temperature": 0,
    "logprobs": True,
    "top_logprobs": 20,
    "skip_special_tokens": False,
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
    """What the endpoint serves. Read from configuration, never hard-coded.
    A deployment that serves the base model only leaves the adapter name,
    revision and LoRA module empty."""

    token: str
    base_model: str
    adapter_name: str
    adapter_revision: str
    lora_module: str
    runtime_version: str
    gpu: str
    chat_template_kwargs: dict[str, Any] = field(default_factory=lambda: {"enable_thinking": False})

    @property
    def serves_adapter(self) -> bool:
        return bool(self.adapter_name and self.adapter_revision and self.lora_module)


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
    """The served model for the request's inference profile. Without a
    served adapter, every analyst-lora request is refused, whatever names
    it carries, empty ones included."""
    profile = req.get("inference_profile")
    if profile == "base":
        if "adapter_name" in req or "adapter_revision" in req:
            raise EndpointError(400, "base takes no adapter")
        return settings.base_model
    if profile == "analyst-lora":
        if (
            not settings.serves_adapter
            or req.get("adapter_name") != settings.adapter_name
            or req.get("adapter_revision") != settings.adapter_revision
        ):
            raise EndpointError(400, "adapter not served")
        return settings.lora_module
    raise EndpointError(400, "unknown inference profile")


def check_messages(req: dict[str, Any]) -> list[dict[str, str]]:
    version = req.get("prompt_version")
    if version not in KNOWN_PROMPTS:
        raise EndpointError(400, "unknown prompt version")
    if version != PROMPTS[req["contract"]]:
        raise EndpointError(400, "prompt version does not match the contract")
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


def contract_letters(req: dict[str, Any]) -> dict[str, str]:
    """The letters a request's answer is read with, mapped to label names:
    fixed for ANALYZE_AGENT_STATE and TRIAGE_MESSAGE; for
    CHOOSE_ORCHESTRATION_ACTION, the payload's allowed_actions in their
    order, A first."""
    contract = req["contract"]
    if contract == ANALYZE:
        return LETTERS
    if contract == TRIAGE:
        return TRIAGE_LETTERS
    actions = req["payload"].get("allowed_actions")
    if (
        not isinstance(actions, list)
        or not MIN_ACTIONS <= len(actions) <= len(CHOOSE_LETTERS)
        or not all(isinstance(a, str) and a for a in actions)
        or len(set(actions)) != len(actions)
    ):
        raise EndpointError(400, "bad allowed actions")
    return dict(zip(CHOOSE_LETTERS, actions))


def is_special(token: str) -> bool:
    """A special or empty token is never the label: chat markers such as
    <|start|>, and whitespace-only tokens."""
    t = token.strip()
    return t == "" or (t.startswith("<") and t.endswith(">"))


def letter_of(token: str, letters: dict[str, str] = LETTERS) -> str | None:
    """The letter a token spells, folding the variants a tokenizer gives
    it: "A", " A", "▁A", "ĠA". Only the given letters count."""
    t = token.strip().lstrip("▁Ġ")
    return t if t in letters else None


def letter_probabilities(
    content: list[dict[str, Any]], letters: dict[str, str] = LETTERS
) -> tuple[dict[str, float], float]:
    """At the label position, the first non-special token, sums the
    probability of each of the given letters over its variants and
    renormalises over them. Returns the distribution by label name and the
    letter mass: the probability the letters held before renormalising,
    at most 1."""
    for pos in content:
        if is_special(str(pos.get("token", ""))):
            continue
        candidates = list(pos.get("top_logprobs") or [])
        if not any(c.get("token") == pos.get("token") for c in candidates):
            candidates.append({"token": pos.get("token"), "logprob": pos.get("logprob")})
        mass: dict[str, float] = dict.fromkeys(letters, 0.0)
        for c in candidates:
            letter = letter_of(str(c.get("token", "")), letters)
            lp = c.get("logprob")
            if letter is None or not isinstance(lp, (int, float)) or math.isnan(lp) or lp > 0:
                continue
            mass[letter] += math.exp(lp)
        total = sum(mass.values())
        if total <= 0:
            return {}, 0.0
        return {letters[k]: v / total for k, v in mass.items()}, min(total, 1.0)
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


def parse_reason(text: str) -> list[str]:
    """The one reason of a CHOOSE_ORCHESTRATION_ACTION answer: everything
    after the first "|", stripped. It is neither split nor cut, so the
    gateway sees it whole when it checks its length and drops it."""
    _, sep, rest = text.partition("|")
    reason = rest.strip()
    return [reason] if sep and reason else []


def _number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def model_output(out: Any) -> tuple[list[dict[str, Any]], str]:
    """Checks the shape of a chat completion and returns its first
    choice's logprob positions and text. Any other shape is refused: the
    model's output is remote input."""
    if not isinstance(out, dict) or not isinstance(out.get("choices"), list) or not out["choices"]:
        raise ValueError("no choices")
    choice = out["choices"][0]
    if not isinstance(choice, dict) or not isinstance(choice.get("message"), dict):
        raise ValueError("no message")
    text = choice["message"].get("content")
    if text is None:
        text = ""
    logprobs = choice.get("logprobs")
    if not isinstance(text, str) or not isinstance(logprobs, dict) or not isinstance(logprobs.get("content"), list):
        raise ValueError("no logprobs")
    content = logprobs["content"]
    for pos in content:
        if not isinstance(pos, dict) or not isinstance(pos.get("token"), str) or not _number(pos.get("logprob")):
            raise ValueError("bad position")
        top = pos.get("top_logprobs")
        if not isinstance(top, list):
            raise ValueError("bad top logprobs")
        for c in top:
            if not isinstance(c, dict) or not isinstance(c.get("token"), str) or not _number(c.get("logprob")):
                raise ValueError("bad candidate")
    return content, text


def chat_body(settings: Settings, req: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Validates one request and builds its chat completion body. Returns
    it with the letters its answer is read with."""
    model = select_model(settings, req)
    messages = check_messages(req)
    letters = contract_letters(req)
    body = {"model": model, "messages": messages, **CHAT_PARAMS}
    if settings.chat_template_kwargs:
        body["chat_template_kwargs"] = settings.chat_template_kwargs
    return body, letters


def wire_response(settings: Settings, out: Any, contract: str, letters: dict[str, str]) -> dict[str, Any]:
    """Reads one chat completion into the wire response, with the
    contract's letters. Every failure, of any kind, is the one fixed
    error: nothing remote reaches the caller as an exception or as text."""
    try:
        content, text = model_output(out)
        probs, mass = letter_probabilities(content, letters)
        evidence = parse_reason(text) if contract == CHOOSE else parse_evidence(text)
    except Exception:  # noqa: BLE001 - any malformed output is one fixed error
        raise EndpointError(502, "model call failed") from None
    if contract == ANALYZE:
        if mass < MIN_LETTER_MASS or not probs:
            label = UNKNOWN
            if not probs:
                probs = {UNKNOWN: 1.0}
        else:
            label = max(letters.values(), key=lambda name: probs[name])
    else:
        # The gateway judges the mass and the winner, so nothing is
        # invented here; with none of the letters there is no distribution.
        if not probs:
            raise EndpointError(502, "model call failed")
        label = max(letters.values(), key=lambda name: probs[name])
    return {
        "label": label,
        "probabilities": probs,
        "evidence": evidence,
        "letter_mass": mass,
        "request_id": "req-" + uuid.uuid4().hex,
        "runtime_version": settings.runtime_version[:50],
        "gpu": settings.gpu[:50],
    }


def _error(status: int, reason: str) -> tuple[int, bytes]:
    return status, json.dumps({"error": reason}).encode()


async def handle(settings: Settings, backend: ChatBackend, authorization: str | None, body: bytes) -> tuple[int, bytes]:
    """One HTTP exchange: status and JSON body. The model call runs in a
    worker thread, so concurrent requests overlap instead of blocking the
    event loop. Error bodies carry a fixed reason only."""
    if not authorised(settings, authorization):
        return _error(401, "unauthorised")
    try:
        req = parse_request(body)
        chat, letters = chat_body(settings, req)
        try:
            out = await asyncio.to_thread(backend.chat, chat)
        except Exception:  # noqa: BLE001 - any backend failure is one fixed error
            raise EndpointError(502, "model call failed") from None
        resp = wire_response(settings, out, req["contract"], letters)
    except EndpointError as e:
        return _error(e.status, e.reason)
    except Exception:  # noqa: BLE001 - never a stack or remote text to the caller
        return _error(500, "internal error")
    return 200, json.dumps(resp, ensure_ascii=False).encode()
