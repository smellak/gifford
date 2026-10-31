"""Unit tests of the thin endpoint against a fake vLLM. No GPU, no Modal,
no network; every value is invented."""

from __future__ import annotations

import asyncio
import json
import math
import time
import unittest

from glimmer_serve.asgi import make_app
from glimmer_serve.endpoint import (
    ANSWER_PREFILL,
    CHAT_PARAMS,
    LETTERS,
    TRIAGE_LETTERS,
    EndpointError,
    Settings,
    handle,
    letter_probabilities,
    parse_evidence,
    parse_reason,
    strip_markers,
    select_model,
)

TOKEN = "test-token-not-a-secret"
AUTH = "Bearer " + TOKEN

SETTINGS = Settings(
    token=TOKEN,
    base_model="glimmer-base",
    adapter_name="analyst",
    adapter_revision="r1",
    lora_module="analyst-lora",
    runtime_version="vllm-test",
    gpu="L4",
)
# A deployment that serves the base model only: no adapter is loaded.
BASE_ONLY = Settings(**{**SETTINGS.__dict__, "adapter_name": "", "adapter_revision": "", "lora_module": ""})

MESSAGES = [{"role": "system", "content": "classify"}, {"role": "user", "content": "screen (last 1 lines):\n<<<\n$ ls\n>>>"}]


def request(**over) -> dict:
    req = {
        "contract": "ANALYZE_AGENT_STATE",
        "inference_profile": "base",
        "prompt_version": "analyze_agent_state.v1",
        "messages": MESSAGES,
        "payload": {"screen_lines": ["$ ls"]},
    }
    req.update(over)
    return {k: v for k, v in req.items() if v is not None}


TRIAGE_MESSAGES = [{"role": "system", "content": "triage"}, {"role": "user", "content": "message:\n<<<\nplease review\n>>>"}]
CHOOSE_MESSAGES = [{"role": "system", "content": "choose"}, {"role": "user", "content": "options:\nA INJECT_PTY_PROMPT\nB DEFER"}]
INJECT, DEFER = "INJECT_PTY_PROMPT", "DEFER"
EVERY_ACTION = [
    "SHOW_HUMAN_NOTIFICATION", "DELIVER_NATIVE_EVENT", "INJECT_PTY_PROMPT", "QUEUE", "PRIORITIZE", "BATCH",
    "DISPLAY_ONLY", "REQUEST_HUMAN_DECISION", "RETRY", "DEFER", "EXPIRE", "DROP_DUPLICATE",
]


def triage_request(**over) -> dict:
    return request(**{
        "contract": "TRIAGE_MESSAGE",
        "prompt_version": "triage_message.v1",
        "messages": TRIAGE_MESSAGES,
        "payload": {"text": "please review the parser change"},
        **over,
    })


def choose_payload(actions) -> dict:
    """An orchestration payload as the gateway sends it, sanitised; the
    endpoint reads only its allowed_actions."""
    return {
        "task_type": "CHOOSE_ORCHESTRATION_ACTION",
        "agent": {"id": "agent-2", "adapter": "generic_pty", "capabilities": {"pty_capture": True}},
        "state": {"human_draft": "CLOSED", "agent_phase": "IDLE_AT_PROMPT", "p_idle": 0.97, "attention": "NONE"},
        "message": {"priority": "NORMAL", "triage": "NORMAL", "age_ms": 100},
        "channel": {"decision_id": "chd_01JB0000000000000000000010", "outcome": "ALLOW", "global": "ACTIVE", "link": "ALLOW"},
        "allowed_actions": list(actions),
    }


def choose_request(actions=(INJECT, DEFER), **over) -> dict:
    return request(**{
        "contract": "CHOOSE_ORCHESTRATION_ACTION",
        "prompt_version": "choose_orchestration_action.v1",
        "messages": CHOOSE_MESSAGES,
        "payload": choose_payload(actions),
        **over,
    })


def position(token: str, logprob: float, top: list[tuple[str, float]]) -> dict:
    return {"token": token, "logprob": logprob, "top_logprobs": [{"token": t, "logprob": lp} for t, lp in top]}


class FakeVLLM:
    """Answers with fixed logprobs and text, and keeps the bodies."""

    def __init__(self, content: list[dict] | None = None, text: str = "C|go test ./...", fail: bool = False) -> None:
        self.content = content if content is not None else [position("C", math.log(0.9), [("C", math.log(0.9)), ("A", math.log(0.05))])]
        self.text = text
        self.fail = fail
        self.bodies: list[dict] = []

    def chat(self, body: dict) -> dict:
        self.bodies.append(body)
        if self.fail:
            raise ConnectionError("sentinel backend detail")
        return {"choices": [{"message": {"content": self.text}, "logprobs": {"content": self.content}}]}


class RawVLLM:
    """Answers with one fixed raw completion, however broken."""

    def __init__(self, out) -> None:
        self.out = out

    def chat(self, body: dict):
        return self.out


class SlowVLLM(FakeVLLM):
    """A blocking backend that takes delay seconds per call, as urllib does."""

    def __init__(self, delay: float) -> None:
        super().__init__()
        self.delay = delay

    def chat(self, body: dict) -> dict:
        time.sleep(self.delay)
        return super().chat(body)


def call(req, backend=None, auth=AUTH, settings=SETTINGS):
    backend = backend or FakeVLLM()
    raw = json.dumps(req).encode() if not isinstance(req, bytes) else req
    status, body = asyncio.run(handle(settings, backend, auth, raw))
    return status, json.loads(body), backend


def assert_wire_response(t: unittest.TestCase, resp: dict, labels: set[str] | None = None) -> None:
    """The checks internal/gateway applies to an answer, over the
    contract's labels (ANALYZE_AGENT_STATE's by default)."""
    t.assertEqual(
        set(resp), {"label", "probabilities", "evidence", "letter_mass", "request_id", "runtime_version", "gpu", "quantization"}
    )
    t.assertTrue(0 <= resp["letter_mass"] <= 1)
    labels = labels if labels is not None else set(LETTERS.values())
    t.assertIn(resp["label"], labels)
    t.assertTrue(resp["probabilities"])
    t.assertTrue(set(resp["probabilities"]) <= labels)
    t.assertAlmostEqual(sum(resp["probabilities"].values()), 1.0, delta=0.001)
    t.assertLessEqual(len(resp["evidence"]), 3)
    for e in resp["evidence"]:
        t.assertTrue(1 <= len(e) <= 120)
    t.assertRegex(resp["request_id"], r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$")


class TestLetters(unittest.TestCase):
    def test_variants_are_summed(self):
        content = [position("C", math.log(0.5), [("C", math.log(0.5)), (" C", math.log(0.2)), ("▁C", math.log(0.1)), ("A", math.log(0.1))])]
        probs, mass = letter_probabilities(content)
        self.assertAlmostEqual(mass, 0.9)
        self.assertAlmostEqual(probs["RUNNING_TOOL"], 0.8 / 0.9)
        self.assertAlmostEqual(probs["IDLE_AT_PROMPT"], 0.1 / 0.9)
        self.assertEqual(set(probs), set(LETTERS.values()), "renormalised over A-G")

    def test_special_tokens_are_skipped(self):
        content = [
            position("<|channel|>", math.log(0.99), [("<|channel|>", math.log(0.99))]),
            position(" ", math.log(0.9), [(" ", math.log(0.9))]),
            position("D", math.log(0.7), [("D", math.log(0.7)), ("ĠE", math.log(0.2))]),
        ]
        probs, mass = letter_probabilities(content)
        self.assertAlmostEqual(mass, 0.9)
        self.assertAlmostEqual(probs["WAITING_APPROVAL"], 0.7 / 0.9)

    def test_non_letters_count_for_nothing(self):
        content = [position("C", math.log(0.3), [("C", math.log(0.3)), ("Ca", math.log(0.3)), ("c", math.log(0.2)), ("H", math.log(0.1))])]
        _, mass = letter_probabilities(content)
        self.assertAlmostEqual(mass, 0.3)

    def test_chosen_token_outside_top_counts(self):
        content = [position("B", math.log(0.6), [("A", math.log(0.1))])]
        probs, mass = letter_probabilities(content)
        self.assertAlmostEqual(mass, 0.7)
        self.assertAlmostEqual(probs["THINKING_STREAMING"], 0.6 / 0.7)

    def test_mass_is_at_most_one(self):
        # A backend whose logprobs add up past 1 still gives a mass the
        # gateway accepts.
        content = [position("A", math.log(0.8), [("A", math.log(0.8)), (" A", math.log(0.6)), ("B", math.log(0.2))])]
        probs, mass = letter_probabilities(content, TRIAGE_LETTERS)
        self.assertEqual(mass, 1.0)
        self.assertAlmostEqual(probs["NORMAL"], 1.4 / 1.6)

    def test_only_the_given_letters_count(self):
        content = [position("E", math.log(0.5), [("E", math.log(0.5)), ("A", math.log(0.2)), ("ĠD", math.log(0.1))])]
        probs, mass = letter_probabilities(content, TRIAGE_LETTERS)
        self.assertAlmostEqual(mass, 0.3, msg="E is an ANALYZE_AGENT_STATE letter, not a TRIAGE_MESSAGE one")
        self.assertEqual(set(probs), set(TRIAGE_LETTERS.values()), "renormalised over A-D")
        self.assertAlmostEqual(probs["NORMAL"], 0.2 / 0.3)
        self.assertAlmostEqual(probs["REQUIRES_HUMAN"], 0.1 / 0.3)


def tok(token: str) -> dict:
    """A position the model was sure of."""
    return position(token, math.log(0.99), [(token, math.log(0.99))])


def header(channel: str, to: str = "Ġto") -> list[dict]:
    """Muse Glimmer's channel header as its tokenizer splits it."""
    return [tok(to), tok(channel), tok("<|message|>")]


# The answer letter A, with its variant ĠA and the letter B below it.
LETTER_A = position("A", math.log(0.9), [("A", math.log(0.9)), ("ĠA", math.log(0.05)), ("B", math.log(0.03))])


class TestChannelHeader(unittest.TestCase):
    """The tokens Muse Glimmer answers with, measured with its tokenizer:
    "<|start|>assistant to=user<|message|>A|x" is '<|start|>', 'assistant',
    'Ġto', '=user', '<|message|>', 'A', '|x'. '=self' opens reasoning, which
    '<|eom|>' closes. The prompt ends with '<|start|>assistant', so the
    completion starts at 'Ġto'."""

    def test_the_label_follows_the_to_user_header(self):
        for to in ("Ġto", " to"):
            with self.subTest(to=to):
                probs, mass = letter_probabilities(header("=user", to) + [LETTER_A, tok("|x")])
                self.assertAlmostEqual(mass, 0.98)
                self.assertAlmostEqual(probs["IDLE_AT_PROMPT"], 0.95 / 0.98)
                self.assertAlmostEqual(probs["THINKING_STREAMING"], 0.03 / 0.98)

    def test_reasoning_is_skipped_up_to_the_answer(self):
        # A letter inside the reasoning is not the label.
        reasoning = header("=self") + [tok("A"), tok(" looks"), tok(" wrong"), tok("<|eom|>")]
        answer = [tok("<|start|>"), tok("assistant")] + header("=user")
        letter_b = position("B", math.log(0.8), [("B", math.log(0.8)), ("A", math.log(0.1))])
        probs, mass = letter_probabilities(reasoning + reasoning + answer + [letter_b, tok("|y")])
        self.assertAlmostEqual(mass, 0.9)
        self.assertAlmostEqual(probs["THINKING_STREAMING"], 0.8 / 0.9)

    def test_without_a_header_the_label_is_the_first_token(self):
        # The prompt carried the header, so the completion starts at the letter.
        probs, mass = letter_probabilities([LETTER_A, tok("|x")])
        self.assertAlmostEqual(mass, 0.98)
        self.assertAlmostEqual(probs["IDLE_AT_PROMPT"], 0.95 / 0.98)

    def test_no_answer_within_max_tokens_has_no_mass(self):
        for name, content in {
            "reasoning only": header("=self") + [tok("A"), tok(" so"), tok("<|eom|>")],
            "reasoning cut short": header("=self") + [tok("A"), tok(" and")],
            "header cut short": [tok("Ġto"), tok("=user")],
            "answer with no token": header("=user"),
            "reasoning, then no answer header": header("=self") + [tok("x"), tok("<|eom|>"), tok("<|start|>"), tok("assistant")],
            "another channel": header("=tool") + [LETTER_A],
        }.items():
            with self.subTest(name):
                self.assertEqual(letter_probabilities(content), ({}, 0.0))

    def test_through_the_endpoint(self):
        status, resp, _ = call(request(), FakeVLLM(content=header("=user") + [LETTER_A, tok("|x")], text="A|on screen"))
        self.assertEqual(status, 200)
        self.assertEqual(resp["label"], "IDLE_AT_PROMPT")
        self.assertAlmostEqual(resp["letter_mass"], 0.98)
        self.assertEqual(resp["evidence"], ["on screen"])

        reasoning_only = header("=self") + [tok("A"), tok("<|eom|>")]
        status, resp, _ = call(request(), FakeVLLM(content=reasoning_only, text=""))
        self.assertEqual((status, resp["label"], resp["letter_mass"]), (200, "UNKNOWN", 0.0))
        status, resp, _ = call(triage_request(), FakeVLLM(content=reasoning_only, text=""))
        self.assertEqual((status, resp), (502, {"error": "model call failed"}))


class TestAnswerPrefill(unittest.TestCase):
    """Every request ends in the answer channel, so the letter is the first
    token the model writes."""

    def test_the_exact_request_body_of_each_contract(self):
        for req, messages in ((request(), MESSAGES), (triage_request(), TRIAGE_MESSAGES), (choose_request(), CHOOSE_MESSAGES)):
            with self.subTest(req["contract"]):
                content = [position("A", math.log(0.9), [("A", math.log(0.9))]), tok("|x")]
                status, _, backend = call(req, FakeVLLM(content=content, text="A|x"), settings=BASE_ONLY)
                self.assertEqual(status, 200)
                self.assertEqual(backend.bodies, [{
                    "model": "glimmer-base",
                    "messages": messages + [{"role": "assistant", "content": ""}],
                    "max_tokens": 40,
                    "temperature": 0,
                    "logprobs": True,
                    "top_logprobs": 20,
                    "skip_special_tokens": False,
                    "add_generation_prompt": False,
                    "continue_final_message": True,
                    "chat_template_kwargs": {"reasoning_strength": "low"},
                }])

    def test_the_prefill_is_a_fresh_copy(self):
        _, _, backend = call(request())
        backend.bodies[0]["messages"][-1]["content"] = "sentinel"
        self.assertEqual(ANSWER_PREFILL, {"role": "assistant", "content": ""})

    def test_a_client_cannot_send_an_assistant_message(self):
        for name, messages in {
            "an empty one at the end": MESSAGES + [{"role": "assistant", "content": ""}],
            "an answer at the end": MESSAGES + [{"role": "assistant", "content": "A|sentinel"}],
            "one in the middle": [MESSAGES[0], {"role": "assistant", "content": "sentinel"}, MESSAGES[1]],
        }.items():
            for req in (request(messages=messages), triage_request(messages=messages), choose_request(messages=messages)):
                with self.subTest(name, contract=req["contract"]):
                    status, resp, backend = call(req)
                    self.assertEqual((status, resp), (400, {"error": "bad message"}))
                    self.assertEqual(backend.bodies, [], "nothing reaches the model")


class TestEndpoint(unittest.TestCase):
    def test_the_answer_names_the_quantization_served(self):
        for quantization in ("fp8", "", "q" * 60):
            with self.subTest(quantization=quantization[:8]):
                settings = Settings(**{**SETTINGS.__dict__, "quantization": quantization})
                status, resp, _ = call(request(), settings=settings)
                self.assertEqual(status, 200)
                self.assertEqual(resp["quantization"], quantization[:50], "at most 50 characters, as the gateway records")

    def test_ok(self):
        status, resp, backend = call(request())
        self.assertEqual(status, 200)
        assert_wire_response(self, resp)
        self.assertEqual(resp["label"], "RUNNING_TOOL")
        self.assertEqual(resp["evidence"], ["go test ./..."])
        self.assertAlmostEqual(resp["letter_mass"], 0.95)
        self.assertEqual(resp["gpu"], "L4")
        self.assertEqual(resp["quantization"], "", "the weights are served as published")
        body = backend.bodies[0]
        self.assertEqual(body["model"], "glimmer-base")
        self.assertEqual(body["messages"], MESSAGES + [ANSWER_PREFILL], "the messages pass as they are, then the prefill")
        for k, v in CHAT_PARAMS.items():
            self.assertEqual(body[k], v)
        self.assertEqual(body["max_tokens"], 40)
        self.assertEqual(body["top_logprobs"], 20)
        self.assertEqual(body["temperature"], 0)
        self.assertIs(body["skip_special_tokens"], False, "on every request, never a server flag")
        self.assertEqual(body["chat_template_kwargs"], {"reasoning_strength": "low"}, "the least reasoning")
        self.assertNotIn("payload", body, "the payload is never sent to the model on its own")

    def test_every_letter(self):
        for letter, label in LETTERS.items():
            content = [position(" " + letter, math.log(0.95), [(" " + letter, math.log(0.95))])]
            status, resp, _ = call(request(), FakeVLLM(content=content, text=letter + "|x"))
            self.assertEqual(status, 200)
            self.assertEqual(resp["label"], label)
            assert_wire_response(self, resp)

    def test_v1_and_v2_read_the_letters_alike(self):
        # A caller on either side of the prompt change reads the same answer:
        # both versions letter the seven states A to G.
        for version in ("analyze_agent_state.v1", "analyze_agent_state.v2"):
            for letter, label in LETTERS.items():
                content = [position(" " + letter, math.log(0.95), [(" " + letter, math.log(0.95))])]
                status, resp, backend = call(request(prompt_version=version), FakeVLLM(content=content, text=letter + "|x"))
                self.assertEqual(status, 200, (version, letter))
                self.assertEqual(resp["label"], label, (version, letter))
                assert_wire_response(self, resp)
                self.assertEqual(backend.bodies[0]["messages"], MESSAGES + [ANSWER_PREFILL])

    def test_low_mass_is_unknown(self):
        content = [position("Sure", math.log(0.6), [("Sure", math.log(0.6)), ("C", math.log(0.3)), ("A", math.log(0.1))])]
        status, resp, _ = call(request(), FakeVLLM(content=content, text="Sure, the agent"))
        self.assertEqual(status, 200)
        self.assertEqual(resp["label"], "UNKNOWN", "letter mass 0.4 < 0.5")
        self.assertAlmostEqual(resp["letter_mass"], 0.4)
        assert_wire_response(self, resp)

        content = [position("Sure", math.log(0.9), [("Sure", math.log(0.9))])]
        status, resp, _ = call(request(), FakeVLLM(content=content, text="Sure"))
        self.assertEqual(resp["label"], "UNKNOWN")
        self.assertEqual(resp["probabilities"], {"UNKNOWN": 1.0}, "no letter at all")
        self.assertEqual(resp["letter_mass"], 0.0)
        assert_wire_response(self, resp)

    def test_mass_at_threshold_keeps_the_label(self):
        content = [position("E", math.log(0.5), [("E", math.log(0.5))])]
        _, resp, _ = call(request(), FakeVLLM(content=content, text="E|"))
        self.assertEqual(resp["label"], "ASKING_QUESTION")
        self.assertEqual(resp["letter_mass"], 0.5)

    def test_analyst_lora(self):
        status, resp, backend = call(request(inference_profile="analyst-lora", adapter_name="analyst", adapter_revision="r1"))
        self.assertEqual(status, 200)
        self.assertEqual(backend.bodies[0]["model"], "analyst-lora")
        for over in (
            {"adapter_name": "other", "adapter_revision": "r1"},
            {"adapter_name": "analyst", "adapter_revision": "r2"},
            {"adapter_name": None, "adapter_revision": None},
        ):
            status, resp, backend = call(request(inference_profile="analyst-lora", **over))
            self.assertEqual(status, 400, over)
            self.assertEqual(backend.bodies, [])
        status, _, _ = call(request(adapter_name="analyst", adapter_revision="r1"))
        self.assertEqual(status, 400, "base takes no adapter")

    def test_refusals_send_nothing(self):
        cases = {
            "unknown prompt version": (request(prompt_version="analyze_agent_state.v3"), 400),
            "no prompt version": (request(prompt_version=None), 400),
            "no messages": (request(messages=None), 400),
            "assistant role": (request(messages=[{"role": "assistant", "content": "sentinel"}]), 400),
            "empty content": (request(messages=[{"role": "user", "content": ""}]), 400),
            "unknown field": ({**request(), "sentinel_field": 1}, 400),
            "unknown contract": (request(contract="CLASSIFY"), 400),
            "unknown profile": (request(inference_profile="sentinel"), 400),
            "triage under the analyze prompt": (request(contract="TRIAGE_MESSAGE"), 400),
            "orchestration under the analyze prompt": (choose_request(prompt_version="analyze_agent_state.v1"), 400),
            "analyze under the triage prompt": (request(prompt_version="triage_message.v1"), 400),
            "orchestration under the triage prompt": (choose_request(prompt_version="triage_message.v1"), 400),
            "triage under the orchestration prompt": (triage_request(prompt_version="choose_orchestration_action.v1"), 400),
            "unknown orchestration prompt version": (choose_request(prompt_version="choose_orchestration_action.v3"), 400),
            "analyze under the orchestration v2 prompt": (request(prompt_version="choose_orchestration_action.v2"), 400),
            "triage under the orchestration v2 prompt": (triage_request(prompt_version="choose_orchestration_action.v2"), 400),
            "triage under the analyze v2 prompt": (triage_request(prompt_version="analyze_agent_state.v2"), 400),
            "orchestration under the analyze v2 prompt": (choose_request(prompt_version="analyze_agent_state.v2"), 400),
            "not JSON": (b"{sentinel", 400),
            "too large": (b" " * ((1 << 20) + 1), 413),
        }
        for name, (req, want) in cases.items():
            with self.subTest(name):
                status, resp, backend = call(req)
                self.assertEqual(status, want)
                self.assertEqual(backend.bodies, [], "nothing reaches the model")
                self.assertNotIn("sentinel", json.dumps(resp), "errors carry no request content")

    def test_bearer(self):
        for auth in (None, "", "Bearer", "Bearer wrong", "Basic " + TOKEN, TOKEN, AUTH + "x"):
            with self.subTest(auth=auth):
                status, resp, backend = call(request(), auth=auth)
                self.assertEqual(status, 401)
                self.assertEqual(backend.bodies, [])
        no_token = Settings(**{**SETTINGS.__dict__, "token": ""})
        status, _ = asyncio.run(handle(no_token, FakeVLLM(), "Bearer ", json.dumps(request()).encode()))
        self.assertEqual(status, 401, "an empty configured token authorises nobody")

    def test_backend_failure_is_fixed_text(self):
        status, resp, _ = call(request(), FakeVLLM(fail=True))
        self.assertEqual(status, 502)
        self.assertEqual(resp, {"error": "model call failed"})
        self.assertNotIn("sentinel", json.dumps(resp))

    def test_malformed_output_is_fixed_502(self):
        good = position("C", math.log(0.9), [("C", math.log(0.9))])

        def completion(content=None, text="C|x", **choice_over):
            choice = {"message": {"content": text}, "logprobs": {"content": [good] if content is None else content}}
            choice.update(choice_over)
            return {"choices": [choice]}

        broken = {
            "logprobs.content null": completion(logprobs={"content": None}),
            "content [null]": completion(content=[None]),
            "top_logprobs [null]": completion(content=[{"token": "C", "logprob": -0.1, "top_logprobs": [None]}]),
            "top_logprobs null": completion(content=[{"token": "C", "logprob": -0.1, "top_logprobs": None}]),
            "position without token": completion(content=[{"bad": 1}]),
            "token not text": completion(content=[{"token": 7, "logprob": -0.1, "top_logprobs": []}]),
            "logprob not a number": completion(content=[{"token": "C", "logprob": "x", "top_logprobs": []}]),
            "logprob NaN": completion(content=[{"token": "C", "logprob": float("nan"), "top_logprobs": []}]),
            "candidate logprob bool": completion(content=[{"token": "C", "logprob": -0.1, "top_logprobs": [{"token": "C", "logprob": True}]}]),
            "logprobs null": completion(logprobs=None),
            "message null": completion(message=None),
            "text not a string": completion(text=["sentinel"]),
            "no choices": {"choices": []},
            "choices null": {"choices": None},
            "choice null": {"choices": [None]},
            "not an object": ["sentinel"],
            "null": None,
        }
        for name, out in broken.items():
            with self.subTest(name):
                status, resp, _ = call(request(), RawVLLM(out))
                self.assertEqual(status, 502)
                self.assertEqual(resp, {"error": "model call failed"}, "fixed text only")
        status, resp, _ = call(request(), RawVLLM(completion(text=None)))
        self.assertEqual(status, 200, "a null message text is no evidence, not an error")
        self.assertEqual(resp["evidence"], [])

    def test_concurrent_calls_overlap(self):
        backend = SlowVLLM(0.1)
        app = make_app(SETTINGS, backend)
        raw = json.dumps(request()).encode()

        async def one():
            sent = []
            chunks = [{"type": "http.request", "body": raw, "more_body": False}]

            async def receive():
                return chunks.pop(0)

            async def send(msg):
                sent.append(msg)

            scope = {"type": "http", "method": "POST", "path": "/infer", "headers": [(b"authorization", AUTH.encode())]}
            await app(scope, receive, send)
            return sent[0]["status"]

        async def both():
            return await asyncio.gather(one(), one())

        start = time.monotonic()
        statuses = asyncio.run(both())
        elapsed = time.monotonic() - start
        self.assertEqual(statuses, [200, 200])
        self.assertLess(elapsed, 0.18, "two 100 ms calls overlap instead of queueing on the event loop")


class TestBaseOnly(unittest.TestCase):
    def test_select_model(self):
        self.assertEqual(select_model(BASE_ONLY, request()), "glimmer-base")
        self.assertEqual(
            select_model(SETTINGS, request(inference_profile="analyst-lora", adapter_name="analyst", adapter_revision="r1")),
            "analyst-lora",
        )
        for names in (
            {"adapter_name": "", "adapter_revision": ""},
            {"adapter_name": "analyst", "adapter_revision": "r1"},
            {},
        ):
            with self.subTest(names):
                with self.assertRaises(EndpointError) as ctx:
                    select_model(BASE_ONLY, request(inference_profile="analyst-lora", **names))
                self.assertEqual((ctx.exception.status, ctx.exception.reason), (400, "adapter not served"))
        # An adapter is served only with its name, revision and LoRA module
        # all set, even when the request matches the empty one.
        for empty in ("adapter_name", "adapter_revision", "lora_module"):
            partial = Settings(**{**SETTINGS.__dict__, empty: ""})
            req = request(
                inference_profile="analyst-lora",
                adapter_name=partial.adapter_name,
                adapter_revision=partial.adapter_revision,
            )
            with self.subTest(empty=empty):
                with self.assertRaises(EndpointError) as ctx:
                    select_model(partial, req)
                self.assertEqual(ctx.exception.reason, "adapter not served")

    def test_analyst_lora_is_refused_before_vllm(self):
        for names in ({"adapter_name": "", "adapter_revision": ""}, {"adapter_name": "analyst", "adapter_revision": "r1"}):
            with self.subTest(names):
                status, resp, backend = call(request(inference_profile="analyst-lora", **names), settings=BASE_ONLY)
                self.assertEqual((status, resp), (400, {"error": "adapter not served"}))
                self.assertEqual(backend.bodies, [], "nothing reaches vLLM")

    def test_the_three_contracts_on_the_base_model(self):
        content = [position("A", math.log(0.9), [("A", math.log(0.9)), ("B", math.log(0.05))])]
        for req, labels, label in (
            (request(), set(LETTERS.values()), "IDLE_AT_PROMPT"),
            (triage_request(), set(TRIAGE_LETTERS.values()), "NORMAL"),
            (choose_request(), {INJECT, DEFER}, INJECT),
        ):
            with self.subTest(req["contract"]):
                status, resp, backend = call(req, FakeVLLM(content=content, text="A|on screen"), settings=BASE_ONLY)
                self.assertEqual(status, 200)
                assert_wire_response(self, resp, labels)
                self.assertEqual(resp["label"], label)
                self.assertEqual(len(backend.bodies), 1)
                self.assertEqual(backend.bodies[0]["model"], "glimmer-base")
                self.assertIs(backend.bodies[0]["skip_special_tokens"], False)


class TestTriage(unittest.TestCase):
    def test_ok(self):
        content = [position("B", math.log(0.8), [("B", math.log(0.8)), (" A", math.log(0.1)), ("E", math.log(0.05))])]
        status, resp, backend = call(triage_request(), FakeVLLM(content=content, text="B|blocks the release; asks to stop"))
        self.assertEqual(status, 200)
        assert_wire_response(self, resp, set(TRIAGE_LETTERS.values()))
        self.assertEqual(resp["label"], "CONFLICT_OR_URGENT")
        self.assertEqual(set(resp["probabilities"]), set(TRIAGE_LETTERS.values()), "renormalised over A-D")
        self.assertAlmostEqual(resp["probabilities"]["CONFLICT_OR_URGENT"], 0.8 / 0.9)
        self.assertAlmostEqual(resp["letter_mass"], 0.9, msg="E is no triage letter")
        self.assertEqual(resp["evidence"], ["blocks the release", "asks to stop"])
        self.assertEqual(backend.bodies[0]["messages"], TRIAGE_MESSAGES + [ANSWER_PREFILL], "the messages pass as they are")
        self.assertNotIn("payload", backend.bodies[0])

    def test_every_letter(self):
        for letter, label in TRIAGE_LETTERS.items():
            content = [position("▁" + letter, math.log(0.95), [("▁" + letter, math.log(0.95))])]
            status, resp, _ = call(triage_request(), FakeVLLM(content=content, text=letter + "|x"))
            self.assertEqual(status, 200)
            self.assertEqual(resp["label"], label)
            assert_wire_response(self, resp, set(TRIAGE_LETTERS.values()))

    def test_mass_is_reported_not_judged(self):
        # The gateway refuses a mass below 0.5 and a tie; the endpoint
        # reports them as they are and never falls back to another label.
        for mass in (0.5, 0.3):
            content = [position("C", math.log(mass), [("C", math.log(mass)), ("Sure", math.log(1 - mass))])]
            status, resp, _ = call(triage_request(), FakeVLLM(content=content, text="C|can wait"))
            self.assertEqual(status, 200)
            self.assertEqual(resp["label"], "LOW_PRIORITY")
            self.assertAlmostEqual(resp["letter_mass"], mass)
            self.assertEqual(resp["probabilities"]["LOW_PRIORITY"], 1.0)
        content = [position("A", math.log(0.4), [("A", math.log(0.4)), ("B", math.log(0.4))])]
        status, resp, _ = call(triage_request(), FakeVLLM(content=content, text="A|"))
        self.assertEqual(status, 200)
        self.assertEqual(resp["probabilities"]["NORMAL"], resp["probabilities"]["CONFLICT_OR_URGENT"], "a tie stays a tie")

    def test_no_letter_fails(self):
        content = [position("G", math.log(0.9), [("G", math.log(0.9)), ("Sure", math.log(0.1))])]
        status, resp, _ = call(triage_request(), FakeVLLM(content=content, text="G|x"))
        self.assertEqual(status, 502, "G is no triage letter, so there is no distribution")
        self.assertEqual(resp, {"error": "model call failed"})


class TestChoose(unittest.TestCase):
    def test_v1_and_v2_letter_the_options_alike(self):
        # A node on either side of the prompt change reads the same answer:
        # both versions letter allowed_actions in order, A first.
        for version in ("choose_orchestration_action.v1", "choose_orchestration_action.v2"):
            for actions, letter, other in (((INJECT, DEFER), "A", "B"), ((DEFER, INJECT), "B", "A")):
                content = [position(letter, math.log(0.8), [(letter, math.log(0.8)), (other, math.log(0.1))])]
                req = choose_request(actions, prompt_version=version)
                status, resp, backend = call(req, FakeVLLM(content=content, text=letter + "|idle at an empty prompt"))
                self.assertEqual(status, 200, (version, actions))
                assert_wire_response(self, resp, set(actions))
                self.assertEqual(resp["label"], INJECT, (version, actions))
                self.assertAlmostEqual(resp["probabilities"][INJECT], 0.8 / 0.9)
                self.assertEqual(backend.bodies[0]["messages"], CHOOSE_MESSAGES + [ANSWER_PREFILL])

    def test_letters_follow_the_request(self):
        # The model picks DEFER in both orders: B in the first, A in the
        # second. Its answer maps back by name.
        for actions, letter, other in (((INJECT, DEFER), "B", "A"), ((DEFER, INJECT), "A", "B")):
            content = [position(letter, math.log(0.7), [(letter, math.log(0.7)), (other, math.log(0.2))])]
            status, resp, backend = call(choose_request(actions), FakeVLLM(content=content, text=letter + "|the human may type"))
            self.assertEqual(status, 200, actions)
            assert_wire_response(self, resp, set(actions))
            self.assertEqual(resp["label"], DEFER)
            self.assertEqual(set(resp["probabilities"]), {INJECT, DEFER})
            self.assertAlmostEqual(resp["probabilities"][DEFER], 0.7 / 0.9)
            self.assertAlmostEqual(resp["probabilities"][INJECT], 0.2 / 0.9)
            self.assertAlmostEqual(resp["letter_mass"], 0.9)
            self.assertEqual(resp["evidence"], ["the human may type"])
            self.assertEqual(backend.bodies[0]["messages"], CHOOSE_MESSAGES + [ANSWER_PREFILL])
            self.assertNotIn("payload", backend.bodies[0])

    def test_variants_are_summed(self):
        content = [position("B", math.log(0.4), [("B", math.log(0.4)), (" B", math.log(0.2)), ("▁B", math.log(0.1)), ("ĠB", math.log(0.1)), ("A", math.log(0.1))])]
        _, resp, _ = call(choose_request(), FakeVLLM(content=content, text="B|wait"))
        self.assertEqual(resp["label"], DEFER)
        self.assertAlmostEqual(resp["letter_mass"], 0.9)
        self.assertAlmostEqual(resp["probabilities"][DEFER], 0.8 / 0.9)

    def test_letters_outside_the_request_count_for_nothing(self):
        content = [position("C", math.log(0.5), [("C", math.log(0.5)), ("A", math.log(0.3)), ("B", math.log(0.1))])]
        status, resp, _ = call(choose_request(), FakeVLLM(content=content, text="C|x"))
        self.assertEqual(status, 200)
        self.assertEqual(resp["label"], INJECT, "C is no letter of a two-action request")
        self.assertAlmostEqual(resp["letter_mass"], 0.4)
        self.assertEqual(set(resp["probabilities"]), {INJECT, DEFER})

    def test_every_action_has_a_letter(self):
        actions = EVERY_ACTION[5:] + EVERY_ACTION[:5]
        content = [position("L", math.log(0.9), [("L", math.log(0.9)), ("A", math.log(0.05))])]
        status, resp, _ = call(choose_request(actions), FakeVLLM(content=content, text="L|last"))
        self.assertEqual(status, 200)
        self.assertEqual(resp["label"], actions[11], "L is the twelfth action")
        self.assertEqual(set(resp["probabilities"]), set(actions))
        assert_wire_response(self, resp, set(actions))

    def test_the_reason_is_one_entry(self):
        long = "x" * 200
        content = [position("A", math.log(0.9), [("A", math.log(0.9))])]
        for text, want in (
            ("A|the agent is idle; the human left", ["the agent is idle; the human left"]),
            ("A|" + long, [long[:80]]),
            ("A|  spaced out  ", ["spaced out"]),
            ("A|a|b", ["a|b"]),
            ("A|   ", []),
            ("A", []),
        ):
            with self.subTest(text=text[:20]):
                status, resp, _ = call(choose_request(), FakeVLLM(content=content, text=text))
                self.assertEqual(status, 200)
                self.assertEqual(resp["evidence"], want)

    def test_the_reason_is_cut_to_80_characters(self):
        # The live answer the gateway refused, 83 characters long.
        live = "Agent idle at prompt with no human attention and pending messages; defer injection."
        self.assertEqual(len(live), 83)
        content = [position("A", math.log(0.9), [("A", math.log(0.9))])]
        for name, reason, want in (
            ("the live reason", live, live[:80]),
            ("80 exactly, intact", "z" * 80, "z" * 80),
            ("cut right after a space", "word " * 20, "word " * 15 + "word"),
        ):
            with self.subTest(name):
                status, resp, _ = call(choose_request(), FakeVLLM(content=content, text="A|" + reason))
                self.assertEqual(status, 200)
                [got] = resp["evidence"]
                self.assertEqual(got, want)
                self.assertLessEqual(len(got), 80)
                self.assertEqual(got, got.rstrip(), "no blank at the end")

    def test_bad_allowed_actions_send_nothing(self):
        cases = {
            "missing": {k: v for k, v in choose_payload([]).items() if k != "allowed_actions"},
            "not a list": {**choose_payload([]), "allowed_actions": "DEFER"},
            "one action": choose_payload([DEFER]),
            "27 actions": choose_payload(["SENTINEL_%d" % i for i in range(27)]),
            "repeated": choose_payload([DEFER, "sentinel", DEFER]),
            "not text": choose_payload([DEFER, 7]),
            "empty": choose_payload([DEFER, ""]),
        }
        for name, payload in cases.items():
            with self.subTest(name):
                status, resp, backend = call(choose_request(payload=payload))
                self.assertEqual(status, 400)
                self.assertEqual(resp, {"error": "bad allowed actions"})
                self.assertEqual(backend.bodies, [], "nothing reaches the model")

    def test_no_letter_fails(self):
        content = [position("Sure", math.log(0.9), [("Sure", math.log(0.9))])]
        status, resp, _ = call(choose_request(), FakeVLLM(content=content, text="Sure"))
        self.assertEqual(status, 502)
        self.assertEqual(resp, {"error": "model call failed"})


class TestEvidence(unittest.TestCase):
    def test_reason(self):
        self.assertEqual(parse_reason("A|idle; quiet"), ["idle; quiet"], "one entry, never split")
        self.assertEqual(parse_reason("A|" + "y" * 300), ["y" * 80], "cut to 80 characters")
        self.assertEqual(parse_reason("A|"), [])
        self.assertEqual(parse_reason("A"), [], "no bar, no reason")

    def test_parse(self):
        self.assertEqual(parse_evidence("C|go test ./...; 12 passed"), ["go test ./...", "12 passed"])
        self.assertEqual(parse_evidence("C| a ;; b ; c ; d"), ["a", "b", "c"], "at most three, empty dropped")
        self.assertEqual(parse_evidence("C|" + "x" * 200), ["x" * 120], "each at most 120 characters")
        self.assertEqual(parse_evidence("C|line one\nline two"), ["line one line two"], "one line each")
        self.assertEqual(parse_evidence("C"), [], "no bar, no evidence")
        self.assertEqual(parse_evidence("C|a|b"), ["a|b"], "only the first bar splits")

    def test_markers(self):
        self.assertEqual(strip_markers("C|on screen<|eot|>"), "C|on screen")
        self.assertEqual(strip_markers("C|a<|eom|>; b<|end|>"), "C|a; b")
        self.assertEqual(strip_markers("C|a <| b |> c; d|>e"), "C|a <| b |> c; d|>e", "only whole markers")

    def test_a_marker_never_reaches_the_evidence(self):
        content = [position("A", math.log(0.9), [("A", math.log(0.9))])]
        for req in (request(), triage_request(), choose_request()):
            with self.subTest(req["contract"]):
                status, resp, _ = call(req, FakeVLLM(content=content, text="A|on screen<|eot|>"))
                self.assertEqual(status, 200)
                self.assertEqual(resp["evidence"], ["on screen"])


class TestASGI(unittest.TestCase):
    def run_app(self, method="POST", path="/infer", body=b"", headers=()):
        app = make_app(SETTINGS, FakeVLLM())
        sent = []
        chunks = [{"type": "http.request", "body": body, "more_body": False}]

        async def receive():
            return chunks.pop(0)

        async def send(msg):
            sent.append(msg)

        scope = {"type": "http", "method": method, "path": path, "headers": list(headers)}
        asyncio.run(app(scope, receive, send))
        return sent[0]["status"], json.loads(sent[1]["body"])

    def test_route(self):
        status, resp = self.run_app(body=json.dumps(request()).encode(), headers=[(b"authorization", AUTH.encode())])
        self.assertEqual(status, 200)
        assert_wire_response(self, resp)
        self.assertEqual(self.run_app(method="GET")[0], 404)
        self.assertEqual(self.run_app(path="/other")[0], 404)
        self.assertEqual(self.run_app(body=json.dumps(request()).encode())[0], 401)


if __name__ == "__main__":
    unittest.main()
