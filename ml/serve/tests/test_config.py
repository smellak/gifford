"""Tests of the serving configuration. Every value is invented."""

from __future__ import annotations

import unittest
from pathlib import Path

from glimmer_serve.config import ConfigError, parse_config, vllm_args

EXAMPLE = (Path(__file__).resolve().parent.parent / "serve.example.toml").read_text()


class TestConfig(unittest.TestCase):
    def test_example_parses(self):
        cfg = parse_config(EXAMPLE)
        self.assertEqual(cfg.lora_module, "analyst-lora")
        self.assertEqual(cfg.chat_template_kwargs, {"enable_thinking": False})
        args = vllm_args(cfg, 8000)
        self.assertIn("--enable-lora", args)
        self.assertEqual(args[args.index("--lora-modules") + 1], "analyst-lora=/adapters/analyst/r1")
        self.assertEqual(args[args.index("--host") + 1], "127.0.0.1", "vLLM is never public")
        self.assertNotIn("--quantization", args, "empty quantization serves the weights as published")

    def test_quantization(self):
        cfg = parse_config(EXAMPLE.replace('quantization = ""', 'quantization = "fp8"'))
        args = vllm_args(cfg, 8000)
        self.assertEqual(args[args.index("--quantization") + 1], "fp8")

    def test_refusals(self):
        cases = {
            "not toml": "[model",
            "unknown table": EXAMPLE + "\n[extra]\nx = 1\n",
            "unknown key": EXAMPLE.replace("[serve]", "[serve]\ntoken = \"sentinel\""),
            "missing repo": EXAMPLE.replace('repo = "example-org/muse-glimmer"', ""),
            "bad max len": EXAMPLE.replace("max_model_len = 8192", "max_model_len = 10"),
            "missing gpu": EXAMPLE.replace('gpu = "L4"', ""),
        }
        for name, text in cases.items():
            with self.subTest(name):
                with self.assertRaises(ConfigError) as ctx:
                    parse_config(text)
                self.assertNotIn("sentinel", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
