"""Serving configuration. Model repo, revision, quantization, GPU, the
deployment limits and the optional adapter come from a TOML file named by
GLIMMER_SERVE_CONFIG, never from code; the Bearer token and any Hugging
Face token come from Modal secrets, never from the file."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TOKEN_ENV = "GLIMMER_TOKEN"
# CONFIG_ENV names the TOML file on the deploying machine. CONFIG_TEXT_ENV
# carries its text into the image, so the worker, which re-imports the
# app module and has no local files, reads the very same configuration.
CONFIG_ENV = "GLIMMER_SERVE_CONFIG"
CONFIG_TEXT_ENV = "GLIMMER_SERVE_CONFIG_TOML"

# The Hugging Face cache vLLM reads lives on the weights volume, and
# download_weights fills it there, so the GPU container finds the weights
# in place. The adapters volume is mounted only when an adapter is served.
WEIGHTS_MOUNT = "/root/.cache/huggingface"
HF_CACHE = WEIGHTS_MOUNT + "/hub"
WEIGHTS_VOLUME = "glimmer-weights"
ADAPTERS_MOUNT = "/adapters"
ADAPTERS_VOLUME = "glimmer-adapters"

# The deployment's limits come from [serve], each within its bounds. By
# default there is at most one container, none is kept warm, and an idle
# one stops after five minutes. One container is also the most allowed:
# one GPU. Recording mode is min_containers = 1, which keeps that container
# warm and billed while idle. Modal's idle window is 2 s to 20 min.
MAX_CONTAINERS = 1
MIN_CONTAINERS = 0
SCALEDOWN_WINDOW_S = 300
SCALEDOWN_WINDOW_MIN_S = 2
SCALEDOWN_WINDOW_MAX_S = 1200
STARTUP_TIMEOUT_MIN_S = 60
STARTUP_TIMEOUT_MAX_S = 1800

ADAPTER_KEYS = ("name", "revision", "module", "path")


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
    # A deployment without an [adapter] table serves the base model only;
    # its adapter fields are then empty.
    base_only: bool
    adapter_name: str
    adapter_revision: str
    lora_module: str
    adapter_path: str
    secret_name: str
    hf_secret_name: str
    vllm_version: str
    max_containers: int
    min_containers: int
    scaledown_window_s: int
    startup_timeout_s: int
    chat_template_kwargs: dict[str, Any]


def _str(table: dict[str, Any], key: str, *, optional: bool = False, limit: int = 200, where: str = "") -> str:
    v = table.get(key, "" if optional else None)
    if not isinstance(v, str) or (not v and not optional) or len(v) > limit:
        raise ConfigError(f"{where}{key}: required text of at most {limit} characters")
    return v


def _int(table: dict[str, Any], key: str, default: int | None, low: int, high: int) -> int:
    v = table.get(key, default)
    if isinstance(v, bool) or not isinstance(v, int) or not low <= v <= high:
        raise ConfigError(f"{key}: integer in {low}..{high}")
    return v


def config_text(environ: Mapping[str, str]) -> str:
    """The configuration text: from the image environment on the worker,
    from the file CONFIG_ENV names on the deploying machine."""
    if environ.get(CONFIG_TEXT_ENV):
        return environ[CONFIG_TEXT_ENV]
    path = environ.get(CONFIG_ENV)
    if not path:
        raise ConfigError(f"{CONFIG_ENV}: required to deploy")
    try:
        return Path(path).read_text()
    except OSError:
        raise ConfigError(f"{CONFIG_ENV}: unreadable") from None


def parse_config(text: str) -> ServeConfig:
    try:
        doc = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        raise ConfigError("document: not valid TOML") from None
    # [adapter] is optional: without it the deployment serves the base
    # model only. A table that is there needs all four keys, so an empty or
    # partial one is refused rather than read as base only.
    if (
        set(doc) - {"model", "adapter", "serve"}
        or not all(isinstance(doc.get(k), dict) for k in ("model", "serve"))
        or not isinstance(doc.get("adapter", {}), dict)
    ):
        raise ConfigError("document: needs [model] and [serve], and an optional [adapter]")
    model, serve = doc["model"], doc["serve"]
    adapter = doc.get("adapter")
    for name, table, keys in (
        ("model", model, {"repo", "revision", "served_name", "quantization", "max_model_len"}),
        ("adapter", adapter or {}, set(ADAPTER_KEYS)),
        (
            "serve",
            serve,
            {
                "gpu",
                "secret_name",
                "hf_secret_name",
                "vllm_version",
                "max_containers",
                "min_containers",
                "scaledown_window_s",
                "startup_timeout_s",
                "chat_template_kwargs",
            },
        ),
    ):
        if set(table) - keys:
            raise ConfigError(f"{name}: unknown key")
    max_len = _int(model, "max_model_len", 8192, 512, 262144)
    max_containers = _int(serve, "max_containers", MAX_CONTAINERS, 1, MAX_CONTAINERS)
    min_containers = _int(serve, "min_containers", MIN_CONTAINERS, 0, max_containers)
    scaledown = _int(serve, "scaledown_window_s", SCALEDOWN_WINDOW_S, SCALEDOWN_WINDOW_MIN_S, SCALEDOWN_WINDOW_MAX_S)
    startup = _int(serve, "startup_timeout_s", None, STARTUP_TIMEOUT_MIN_S, STARTUP_TIMEOUT_MAX_S)
    kwargs = serve.get("chat_template_kwargs", {"reasoning_strength": "low"})
    if not isinstance(kwargs, dict):
        raise ConfigError("chat_template_kwargs: table")
    base_only = adapter is None
    if base_only:
        adapter_name = adapter_revision = lora_module = adapter_path = ""
    else:
        adapter_name = _str(adapter, "name", limit=100, where="adapter.")
        adapter_revision = _str(adapter, "revision", limit=100, where="adapter.")
        lora_module = _str(adapter, "module", limit=100, where="adapter.")
        adapter_path = _str(adapter, "path", where="adapter.")
    return ServeConfig(
        base_repo=_str(model, "repo"),
        base_revision=_str(model, "revision", limit=100),
        served_name=_str(model, "served_name", limit=100),
        quantization=_str(model, "quantization", optional=True, limit=50),
        gpu=_str(serve, "gpu", limit=50),
        max_model_len=max_len,
        base_only=base_only,
        adapter_name=adapter_name,
        adapter_revision=adapter_revision,
        lora_module=lora_module,
        adapter_path=adapter_path,
        secret_name=_str(serve, "secret_name", limit=100),
        hf_secret_name=_str(serve, "hf_secret_name", optional=True, limit=100),
        vllm_version=_str(serve, "vllm_version", limit=50),
        max_containers=max_containers,
        min_containers=min_containers,
        scaledown_window_s=scaledown,
        startup_timeout_s=startup,
        chat_template_kwargs=kwargs,
    )


def vllm_args(cfg: ServeConfig, port: int) -> list[str]:
    """The vLLM OpenAI server command line: one base model, bound to
    localhost only, with the Analyst LoRA as an adapter unless the
    deployment serves the base model only."""
    args = [
        "vllm", "serve", cfg.base_repo,
        "--revision", cfg.base_revision,
        "--served-model-name", cfg.served_name,
        "--host", "127.0.0.1",
        "--port", str(port),
        "--max-model-len", str(cfg.max_model_len),
    ]
    if not cfg.base_only:
        args += ["--enable-lora", "--lora-modules", f"{cfg.lora_module}={cfg.adapter_path}"]
    args += [
        "--max-logprobs", "20",
        "--enable-prefix-caching",
        # Request logging stays off. vLLM has --no-enable-log-requests from
        # 0.10.1 and no --disable-log-requests from 0.17.0; an older vLLM
        # rejects the line, so its start fails at once.
        "--no-enable-log-requests",
        # Muse Glimmer's tool-call and reasoning parsers, by the names vLLM
        # 0.28.0 registers them under, and the model's own generation
        # defaults. A request's parameters still win over those defaults.
        "--enable-auto-tool-choice",
        "--tool-call-parser", "muse_glimmer",
        "--reasoning-parser", "muse_glimmer",
        "--generation-config", "auto",
    ]
    if cfg.quantization:
        args += ["--quantization", cfg.quantization]
    return args


def vllm_env(environ: Mapping[str, str]) -> dict[str, str]:
    """The vLLM server's environment: the container's, with the Hugging
    Face cache on the weights volume and offline. vLLM reads the weights
    download_weights put there and never downloads, so a skipped download
    fails the start fast instead of fetching the weights on the GPU.

    It also turns off FlashInfer's top-k/top-p sampler. That sampler is
    compiled at start-up and needs nvcc, which the image does not have, so
    the start failed while warming it up. vLLM then uses its PyTorch
    sampler; with temperature 0 the answer is the same."""
    return {**environ, "HF_HOME": WEIGHTS_MOUNT, "HF_HUB_OFFLINE": "1", "VLLM_USE_FLASHINFER_SAMPLER": "0"}


def serve_options(cfg: ServeConfig) -> dict[str, int]:
    """The serve function's Modal limits, by their Modal 1.1.4 names, as
    configured: the container cap, the containers kept warm (one in
    recording mode), the idle window, and the startup timeout, the same one
    the vLLM readiness wait uses so the two never disagree."""
    return {
        "max_containers": cfg.max_containers,
        "min_containers": cfg.min_containers,
        "scaledown_window": cfg.scaledown_window_s,
        "startup_timeout": cfg.startup_timeout_s,
    }


def volumes(cfg: ServeConfig) -> dict[str, str]:
    """The serve function's volumes, by mount path: the weights always,
    the adapters only when an adapter is served."""
    mounts = {WEIGHTS_MOUNT: WEIGHTS_VOLUME}
    if not cfg.base_only:
        mounts[ADAPTERS_MOUNT] = ADAPTERS_VOLUME
    return mounts
