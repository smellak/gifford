"""Unit tests of the thin endpoint against a fake vLLM. No GPU, no Modal,
no network; every value is invented."""

from __future__ import annotations

import asyncio
import json
import math
import unittest

from glimmer_serve.asgi import make_app
from glimmer_serve.endpoint import CHAT_PARAMS, LETTERS, Settings, handle, letter_probabilities, parse_evidence

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


def call(req, backend=None, auth=AUTH):
    backend = backend or FakeVLLM()
    status, body = handle(SETTINGS, backend, auth, json.dumps(req).encode() if not isinstance(req, bytes) else req)
    return status, json.loads(body), backend


def assert_wire_response(t: unittest.TestCase, resp: dict) -> None:
    """The checks internal/gateway applies to an answer."""
    t.assertEqual(set(resp), {"label", "probabilities", "evidence", "request_id", "runtime_version", "gpu"})
    labels = set(LETTERS.values())
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


class TestEndpoint(unittest.TestCase):
    def test_ok(self):
        status, resp, backend = call(request())
        self.assertEqual(status, 200)
        assert_wire_response(self, resp)
        self.assertEqual(resp["label"], "RUNNING_TOOL")
        self.assertEqual(resp["evidence"], ["go test ./..."])
        self.assertEqual(resp["gpu"], "L4")
        body = backend.bodies[0]
        self.assertEqual(body["model"], "glimmer-base")
        self.assertEqual(body["messages"], MESSAGES, "the messages pass as they are")
        for k, v in CHAT_PARAMS.items():
            self.assertEqual(body[k], v)
        self.assertEqual(body["max_tokens"], 40)
        self.assertEqual(body["top_logprobs"], 20)
        self.assertEqual(body["temperature"], 0)
        self.assertEqual(body["chat_template_kwargs"], {"enable_thinking": False}, "reasoning off")
        self.assertNotIn("payload", body, "the payload is never sent to the model on its own")

    def test_every_letter(self):
        for letter, label in LETTERS.items():
            content = [position(" " + letter, math.log(0.95), [(" " + letter, math.log(0.95))])]
            status, resp, _ = call(request(), FakeVLLM(content=content, text=letter + "|x"))
            self.assertEqual(status, 200)
            self.assertEqual(resp["label"], label)
            assert_wire_response(self, resp)

    def test_low_mass_is_unknown(self):
        content = [position("Sure", math.log(0.6), [("Sure", math.log(0.6)), ("C", math.log(0.3)), ("A", math.log(0.1))])]
        status, resp, _ = call(request(), FakeVLLM(content=content, text="Sure, the agent"))
        self.assertEqual(status, 200)
        self.assertEqual(resp["label"], "UNKNOWN", "letter mass 0.4 < 0.5")
        assert_wire_response(self, resp)

        content = [position("Sure", math.log(0.9), [("Sure", math.log(0.9))])]
        status, resp, _ = call(request(), FakeVLLM(content=content, text="Sure"))
        self.assertEqual(resp["label"], "UNKNOWN")
        self.assertEqual(resp["probabilities"], {"UNKNOWN": 1.0}, "no letter at all")
        assert_wire_response(self, resp)

    def test_mass_at_threshold_keeps_the_label(self):
        content = [position("E", math.log(0.5), [("E", math.log(0.5))])]
        _, resp, _ = call(request(), FakeVLLM(content=content, text="E|"))
        self.assertEqual(resp["label"], "ASKING_QUESTION")

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
            "unknown prompt version": (request(prompt_version="analyze_agent_state.v2"), 400),
            "no prompt version": (request(prompt_version=None), 400),
            "no messages": (request(messages=None), 400),
            "assistant role": (request(messages=[{"role": "assistant", "content": "sentinel"}]), 400),
            "empty content": (request(messages=[{"role": "user", "content": ""}]), 400),
            "unknown field": ({**request(), "sentinel_field": 1}, 400),
            "unknown contract": (request(contract="CLASSIFY"), 400),
            "unknown profile": (request(inference_profile="sentinel"), 400),
            "triage not served yet": (request(contract="TRIAGE_MESSAGE"), 501),
            "orchestration not served yet": (request(contract="CHOOSE_ORCHESTRATION_ACTION"), 501),
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
        status, _ = handle(no_token, FakeVLLM(), "Bearer ", json.dumps(request()).encode())
        self.assertEqual(status, 401, "an empty configured token authorises nobody")

    def test_backend_failure_is_fixed_text(self):
        status, resp, _ = call(request(), FakeVLLM(fail=True))
        self.assertEqual(status, 502)
        self.assertNotIn("sentinel", json.dumps(resp))
        status, resp, _ = call(request(), FakeVLLM(content=[{"bad": 1}]))
        self.assertEqual(status, 200)
        self.assertEqual(resp["label"], "UNKNOWN")


class TestEvidence(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse_evidence("C|go test ./...; 12 passed"), ["go test ./...", "12 passed"])
        self.assertEqual(parse_evidence("C| a ;; b ; c ; d"), ["a", "b", "c"], "at most three, empty dropped")
        self.assertEqual(parse_evidence("C|" + "x" * 200), ["x" * 120], "each at most 120 characters")
        self.assertEqual(parse_evidence("C|line one\nline two"), ["line one line two"], "one line each")
        self.assertEqual(parse_evidence("C"), [], "no bar, no evidence")
        self.assertEqual(parse_evidence("C|a|b"), ["a|b"], "only the first bar splits")


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
