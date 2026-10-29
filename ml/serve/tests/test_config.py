"""Tests of the serving configuration. Every value is invented."""

from __future__ import annotations

import unittest
from pathlib import Path

import tempfile

from glimmer_serve.config import CONFIG_ENV, CONFIG_TEXT_ENV, ConfigError, config_text, parse_config, vllm_args

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
        self.assertIn("--enable-prefix-caching", args, "PRD 12.5: the fixed system prompt is cached")

    def test_config_reaches_the_worker(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as f:
            f.write(EXAMPLE)
        deploying = config_text({CONFIG_ENV: f.name})
        self.assertEqual(deploying, EXAMPLE, "the deploying machine reads the file")
        # The worker has no local file and no CONFIG_ENV: only the image
        # environment the deploy set.
        worker = config_text({CONFIG_TEXT_ENV: deploying})
        self.assertEqual(parse_config(worker), parse_config(deploying))
        for env in ({}, {CONFIG_ENV: "/nonexistent/serve.toml"}):
            with self.assertRaises(ConfigError):
                config_text(env)
        Path(f.name).unlink()

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
