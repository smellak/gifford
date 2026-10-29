"""Serving configuration. Model repo, revision, quantization, GPU and the
adapter come from a TOML file named by GLIMMER_SERVE_CONFIG, never from
code; the Bearer token comes from a Modal secret, never from the file."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from typing import Any

TOKEN_ENV = "GLIMMER_TOKEN"
CONFIG_ENV = "GLIMMER_SERVE_CONFIG"


class ConfigError(Exception):
    """A configuration refused. Names the key, never its value."""


@dataclass(frozen=True)
class ServeConfig:
    base_repo: str
    base_revision: str
    served_name: str
    quantization: str
    gpu: str
    max_model_len: int
    adapter_name: str
    adapter_revision: str
    lora_module: str
    adapter_path: str
    secret_name: str
    vllm_version: str
    chat_template_kwargs: dict[str, Any]


def _str(table: dict[str, Any], key: str, *, optional: bool = False, limit: int = 200) -> str:
    v = table.get(key, "" if optional else None)
    if not isinstance(v, str) or (not v and not optional) or len(v) > limit:
        raise ConfigError(f"{key}: required text of at most {limit} characters")
    return v


def parse_config(text: str) -> ServeConfig:
    try:
        doc = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        raise ConfigError("document: not valid TOML") from None
    allowed = {"model", "adapter", "serve"}
    if set(doc) - allowed or not all(isinstance(doc.get(k), dict) for k in allowed):
        raise ConfigError("document: needs exactly [model], [adapter] and [serve]")
    model, adapter, serve = doc["model"], doc["adapter"], doc["serve"]
    for name, table, keys in (
        ("model", model, {"repo", "revision", "served_name", "quantization", "max_model_len"}),
        ("adapter", adapter, {"name", "revision", "module", "path"}),
        ("serve", serve, {"gpu", "secret_name", "vllm_version", "chat_template_kwargs"}),
    ):
        if set(table) - keys:
            raise ConfigError(f"{name}: unknown key")
    max_len = model.get("max_model_len", 8192)
    if not isinstance(max_len, int) or not 512 <= max_len <= 262144:
        raise ConfigError("max_model_len: integer in 512..262144")
    kwargs = serve.get("chat_template_kwargs", {"enable_thinking": False})
    if not isinstance(kwargs, dict):
        raise ConfigError("chat_template_kwargs: table")
    return ServeConfig(
        base_repo=_str(model, "repo"),
        base_revision=_str(model, "revision", limit=100),
        served_name=_str(model, "served_name", limit=100),
        quantization=_str(model, "quantization", optional=True, limit=50),
        gpu=_str(serve, "gpu", limit=50),
        max_model_len=max_len,
        adapter_name=_str(adapter, "name", limit=100),
        adapter_revision=_str(adapter, "revision", limit=100),
        lora_module=_str(adapter, "module", limit=100),
        adapter_path=_str(adapter, "path"),
        secret_name=_str(serve, "secret_name", limit=100),
        vllm_version=_str(serve, "vllm_version", limit=50),
        chat_template_kwargs=kwargs,
    )


def vllm_args(cfg: ServeConfig, port: int) -> list[str]:
    """The vLLM OpenAI server command line: one base model, the Analyst
    LoRA as an adapter, bound to localhost only."""
    args = [
        "vllm", "serve", cfg.base_repo,
        "--revision", cfg.base_revision,
        "--served-model-name", cfg.served_name,
        "--host", "127.0.0.1",
        "--port", str(port),
        "--max-model-len", str(cfg.max_model_len),
        "--enable-lora",
        "--lora-modules", f"{cfg.lora_module}={cfg.adapter_path}",
        "--max-logprobs", "20",
        "--disable-log-requests",
    ]
    if cfg.quantization:
        args += ["--quantization", cfg.quantization]
    return args
