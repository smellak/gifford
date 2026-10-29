"""The Modal app: vLLM's OpenAI-compatible server on one GPU, bound to
localhost, with the thin endpoint in front of it as the only public route.

Deploy with `modal deploy glimmer_serve/modal_app.py` (see README). Not
imported by tests: it needs Modal and a GPU.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

import modal

from .asgi import make_app
from .config import CONFIG_ENV, TOKEN_ENV, parse_config, vllm_args
from .endpoint import Settings

VLLM_PORT = 8000
cfg = parse_config(Path(os.environ[CONFIG_ENV]).read_text())

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(f"vllm=={cfg.vllm_version}")
    .add_local_python_source("glimmer_serve")
)
weights = modal.Volume.from_name("glimmer-weights", create_if_missing=True)
adapters = modal.Volume.from_name("glimmer-adapters", create_if_missing=True)
app = modal.App("gifford-glimmer")


class VLLMBackend:
    """Chat completions against the local vLLM server."""

    def __init__(self, port: int) -> None:
        self.url = f"http://127.0.0.1:{port}/v1/chat/completions"

    def chat(self, body: dict) -> dict:
        req = urllib.request.Request(
            self.url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - fixed localhost URL
            return json.loads(resp.read())


def _wait_ready(port: int, deadline_s: float = 900) -> None:
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5):  # noqa: S310
                return
        except OSError:
            time.sleep(2)
    raise RuntimeError("vLLM did not become ready")


@app.function(
    image=image,
    gpu=cfg.gpu,
    secrets=[modal.Secret.from_name(cfg.secret_name)],
    volumes={"/root/.cache/huggingface": weights, "/adapters": adapters},
    scaledown_window=300,
)
@modal.concurrent(max_inputs=16)
@modal.asgi_app()
def serve():
    subprocess.Popen(vllm_args(cfg, VLLM_PORT))  # noqa: S603 - fixed argv from validated config
    _wait_ready(VLLM_PORT)
    settings = Settings(
        token=os.environ.get(TOKEN_ENV, ""),
        base_model=cfg.served_name,
        adapter_name=cfg.adapter_name,
        adapter_revision=cfg.adapter_revision,
        lora_module=cfg.lora_module,
        runtime_version=f"vllm-{cfg.vllm_version}",
        gpu=cfg.gpu,
        chat_template_kwargs=cfg.chat_template_kwargs,
    )
    return make_app(settings, VLLMBackend(VLLM_PORT))
