"""Tests of the serving configuration. Every value is invented."""

from __future__ import annotations

import itertools
import unittest
from pathlib import Path

import tempfile

from glimmer_serve.config import (
    ADAPTER_KEYS,
    CONFIG_ENV,
    CONFIG_TEXT_ENV,
    HF_CACHE,
    ConfigError,
    config_text,
    parse_config,
    serve_options,
    vllm_args,
    vllm_env,
    volumes,
)

EXAMPLE = (Path(__file__).resolve().parent.parent / "serve.example.toml").read_text()
BASE_ONLY = (Path(__file__).resolve().parent.parent / "serve.base-only.example.toml").read_text()
TIMEOUT_LINE = "startup_timeout_s = 900"


def without_adapter(text: str) -> str:
    """The configuration with its [adapter] table taken out."""
    lines = text.splitlines(keepends=True)
    start = next(i for i, line in enumerate(lines) if line.startswith("[adapter]"))
    end = next(i for i in range(start + 1, len(lines)) if lines[i].startswith("["))
    return "".join(lines[:start] + lines[end:])


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


class TestBaseOnly(unittest.TestCase):
    def test_the_base_only_example_serves_no_adapter(self):
        cfg = parse_config(BASE_ONLY)
        self.assertTrue(cfg.base_only)
        self.assertEqual((cfg.adapter_name, cfg.adapter_revision, cfg.lora_module, cfg.adapter_path), ("", "", "", ""))
        args = vllm_args(cfg, 8000)
        self.assertNotIn("--enable-lora", args)
        self.assertNotIn("--lora-modules", args)

    def test_without_the_adapter_only_the_lora_flags_go(self):
        full = vllm_args(parse_config(EXAMPLE), 8000)
        cfg = parse_config(without_adapter(EXAMPLE))
        self.assertTrue(cfg.base_only)
        i = full.index("--enable-lora")
        self.assertEqual(full[i : i + 3], ["--enable-lora", "--lora-modules", "analyst-lora=/adapters/analyst/r1"])
        self.assertEqual(vllm_args(cfg, 8000), full[:i] + full[i + 3 :], "the rest of the line is unchanged")

    def test_with_the_adapter_the_line_is_as_before(self):
        cfg = parse_config(EXAMPLE)
        self.assertFalse(cfg.base_only)
        self.assertEqual(
            vllm_args(cfg, 8000),
            [
                "vllm", "serve", "example-org/muse-glimmer",
                "--revision", "REVISION",
                "--served-model-name", "glimmer-base",
                "--host", "127.0.0.1",
                "--port", "8000",
                "--max-model-len", "8192",
                "--enable-lora",
                "--lora-modules", "analyst-lora=/adapters/analyst/r1",
                "--max-logprobs", "20",
                "--enable-prefix-caching",
                "--disable-log-requests",
            ],
        )

    def test_an_empty_or_partial_adapter_table_is_refused(self):
        values = {"name": "sentinel-name", "revision": "sentinel-rev", "module": "sentinel-module", "path": "/sentinel"}
        base = without_adapter(EXAMPLE)
        for count in range(len(ADAPTER_KEYS)):
            for present in itertools.combinations(ADAPTER_KEYS, count):
                missing = next(k for k in ADAPTER_KEYS if k not in present)
                table = "[adapter]\n" + "".join(f'{k} = "{values[k]}"\n' for k in present)
                with self.subTest(present=present):
                    with self.assertRaises(ConfigError) as ctx:
                        parse_config(base + table)
                    self.assertIn(f"adapter.{missing}", str(ctx.exception))
                    self.assertNotIn("sentinel", str(ctx.exception))
        empty_value = EXAMPLE.replace('name = "analyst"', 'name = ""')
        with self.assertRaises(ConfigError) as ctx:
            parse_config(empty_value)
        self.assertIn("adapter.name", str(ctx.exception))
        with self.assertRaises(ConfigError):
            parse_config('adapter = "sentinel"\n' + base)

    def test_the_adapters_volume_only_with_an_adapter(self):
        weights = {"/root/.cache/huggingface": "glimmer-weights"}
        self.assertEqual(volumes(parse_config(EXAMPLE)), {**weights, "/adapters": "glimmer-adapters"})
        self.assertEqual(volumes(parse_config(BASE_ONLY)), weights)


class TestServeOptions(unittest.TestCase):
    def test_the_limits_are_fixed_and_the_startup_timeout_configured(self):
        fixed = {"max_containers": 1, "min_containers": 0, "scaledown_window": 300}
        for text in (EXAMPLE, BASE_ONLY):
            self.assertEqual(serve_options(parse_config(text)), {**fixed, "startup_timeout": 900})
        for value in (60, 1800):
            cfg = parse_config(EXAMPLE.replace(TIMEOUT_LINE, f"startup_timeout_s = {value}"))
            self.assertEqual(cfg.startup_timeout_s, value)
            self.assertEqual(serve_options(cfg), {**fixed, "startup_timeout": value})

    def test_the_startup_timeout_is_required_and_bounded(self):
        for name, line in {
            "missing": "",
            "below": "startup_timeout_s = 59",
            "above": "startup_timeout_s = 1801",
            "zero": "startup_timeout_s = 0",
            "negative": "startup_timeout_s = -900",
            "text": 'startup_timeout_s = "900"',
            "float": "startup_timeout_s = 900.0",
            "bool": "startup_timeout_s = true",
        }.items():
            with self.subTest(name):
                with self.assertRaises(ConfigError) as ctx:
                    parse_config(EXAMPLE.replace(TIMEOUT_LINE, line))
                self.assertIn("startup_timeout_s", str(ctx.exception))

    def test_no_limit_is_raised_by_configuration(self):
        for key in ("max_containers", "min_containers", "scaledown_window", "startup_timeout"):
            with self.subTest(key):
                with self.assertRaises(ConfigError):
                    parse_config(EXAMPLE.replace("[serve]", f"[serve]\n{key} = 4"))

    def test_vllm_runs_offline_on_the_downloaded_weights(self):
        env = vllm_env({"PATH": "/usr/bin", "HF_HUB_OFFLINE": "0"})
        self.assertEqual(env["HF_HUB_OFFLINE"], "1", "a skipped download fails fast, never downloads on the GPU")
        self.assertEqual(f"{env['HF_HOME']}/hub", HF_CACHE, "download_weights writes where vLLM reads")
        self.assertEqual(env["PATH"], "/usr/bin")

    def test_the_hugging_face_secret_is_optional(self):
        self.assertEqual(parse_config(EXAMPLE).hf_secret_name, "")
        named = EXAMPLE.replace('# hf_secret_name = "huggingface"', 'hf_secret_name = "huggingface"')
        self.assertEqual(parse_config(named).hf_secret_name, "huggingface")


if __name__ == "__main__":
    unittest.main()
