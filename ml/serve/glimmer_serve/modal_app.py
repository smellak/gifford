"""The Modal app: vLLM's OpenAI-compatible server on one GPU, bound to
localhost, with the thin endpoint in front of it as the only public route,
and a GPU-free function that downloads the base weights before any GPU
starts.

Deploy with `modal deploy -m glimmer_serve.modal_app` (see README). Not
imported by tests: it needs Modal and a GPU. Every decision it applies,
the vLLM command line and environment, the volumes and the limits, comes
from config.py, which the tests cover.

Modal imports this module again on the worker, where neither the
deploying machine's files nor its environment exist. The configuration
text therefore travels in the image environment (config.CONFIG_TEXT_ENV):
the deploying machine reads it from the file GLIMMER_SERVE_CONFIG names,
the worker reads the same text back from its image. It holds no secret;
the token comes from the Modal secret only.
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request

import modal

from .asgi import make_app
from .config import (
    CONFIG_TEXT_ENV,
    HF_CACHE,
    TOKEN_ENV,
    WEIGHTS_MOUNT,
    config_text,
    parse_config,
    serve_options,
    vllm_args,
    vllm_env,
    volumes,
)
from .endpoint import Settings
from .startup import health_probe, wait_ready

VLLM_PORT = 8000
# Modal's default execution timeout, 300 s, is too short to download large
# weights; the download runs without a GPU, so a long one costs little.
DOWNLOAD_TIMEOUT_S = 3600
CFG_TEXT = config_text(os.environ)
cfg = parse_config(CFG_TEXT)

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(f"vllm=={cfg.vllm_version}")
    .env({CONFIG_TEXT_ENV: CFG_TEXT})
    .add_local_python_source("glimmer_serve")
)
# A base-only deployment neither creates nor mounts the adapters volume.
mounted = {path: modal.Volume.from_name(name, create_if_missing=True) for path, name in volumes(cfg).items()}
weights = mounted[WEIGHTS_MOUNT]
# The Hugging Face token, only if the model needs one: from the Modal secret
# hf_secret_name names, never from the configuration.
hf_secrets = [modal.Secret.from_name(cfg.hf_secret_name)] if cfg.hf_secret_name else []
app = modal.App("gifford-glimmer")


class VLLMBackend:
    """Chat completions against the local vLLM server. Called from a
    worker thread by the endpoint, so it may block."""

    def __init__(self, port: int) -> None:
        self.url = f"http://127.0.0.1:{port}/v1/chat/completions"

    def chat(self, body: dict) -> dict:
        req = urllib.request.Request(
            self.url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - fixed localhost URL
            return json.loads(resp.read())


@app.function(image=image)
def check_config() -> dict:
    """A GPU-free remote check: the worker imports this module, reads the
    configuration from its image and returns what serve would run with:
    whether it serves the base model only, its limits, its volumes and
    the vLLM command line. Run with
    `modal run -m glimmer_serve.modal_app::check_config`."""
    remote = parse_config(os.environ[CONFIG_TEXT_ENV])
    return {
        "served_name": remote.served_name,
        "gpu": remote.gpu,
        "quantization": remote.quantization,
        "base_only": remote.base_only,
        "serve_options": serve_options(remote),
        "volumes": volumes(remote),
        "argv": vllm_args(remote, VLLM_PORT),
    }


@app.function(image=image, volumes={WEIGHTS_MOUNT: weights}, secrets=hf_secrets, timeout=DOWNLOAD_TIMEOUT_S)
def download_weights() -> dict:
    """Downloads the configured base weights, the exact repo and revision,
    into the weights volume's Hugging Face cache, the one vLLM reads, and
    commits the volume. It runs on Modal without a GPU: nothing downloads
    on the deploying machine or with a GPU on. Run with
    `modal run -m glimmer_serve.modal_app::download_weights` before the
    deploy."""
    from huggingface_hub import snapshot_download  # installed with vLLM

    remote = parse_config(os.environ[CONFIG_TEXT_ENV])
    snapshot_download(repo_id=remote.base_repo, revision=remote.base_revision, cache_dir=HF_CACHE)
    weights.commit()
    return {"repo": remote.base_repo, "revision": remote.base_revision}


@app.function(
    image=image,
    gpu=cfg.gpu,
    secrets=[modal.Secret.from_name(cfg.secret_name)],
    volumes=mounted,
    **serve_options(cfg),
)
@modal.concurrent(max_inputs=16)
@modal.asgi_app()
def serve():
    # Offline: the weights come from the volume download_weights filled,
    # never from a download with the GPU on. If vLLM exits, missing
    # weights for one, the start fails at once instead of waiting out the
    # startup timeout.
    vllm = subprocess.Popen(vllm_args(cfg, VLLM_PORT), env=vllm_env(os.environ))  # noqa: S603 - fixed argv from validated config
    wait_ready(vllm, health_probe(VLLM_PORT), cfg.startup_timeout_s)
    settings = Settings(
        token=os.environ.get(TOKEN_ENV, ""),
        base_model=cfg.served_name,
        adapter_name=cfg.adapter_name,
        adapter_revision=cfg.adapter_revision,
        lora_module=cfg.lora_module,
        runtime_version=f"vllm-{cfg.vllm_version}",
        gpu=cfg.gpu,
        quantization=cfg.quantization,
        chat_template_kwargs=cfg.chat_template_kwargs,
    )
    return make_app(settings, VLLMBackend(VLLM_PORT))
