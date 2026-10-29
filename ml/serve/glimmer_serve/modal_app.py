"""The Modal app: vLLM's OpenAI-compatible server on one GPU, bound to
localhost, with the thin endpoint in front of it as the only public route.

Deploy with `modal deploy -m glimmer_serve.modal_app` (see README). Not
imported by tests: it needs Modal and a GPU.

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
import time
import urllib.request

import modal

from .asgi import make_app
from .config import CONFIG_TEXT_ENV, TOKEN_ENV, config_text, parse_config, vllm_args
from .endpoint import Settings

VLLM_PORT = 8000
CFG_TEXT = config_text(os.environ)
cfg = parse_config(CFG_TEXT)

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(f"vllm=={cfg.vllm_version}")
    .env({CONFIG_TEXT_ENV: CFG_TEXT})
    .add_local_python_source("glimmer_serve")
)
weights = modal.Volume.from_name("glimmer-weights", create_if_missing=True)
adapters = modal.Volume.from_name("glimmer-adapters", create_if_missing=True)
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


def _wait_ready(port: int, deadline_s: float = 900) -> None:
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5):  # noqa: S310
                return
        except OSError:
            time.sleep(2)
    raise RuntimeError("vLLM did not become ready")


@app.function(image=image)
def check_config() -> dict:
    """A GPU-free remote check: the worker imports this module, reads the
    configuration from its image and builds the vLLM command line. Run
    with `modal run -m glimmer_serve.modal_app::check_config`."""
    remote = parse_config(os.environ[CONFIG_TEXT_ENV])
    return {"served_name": remote.served_name, "gpu": remote.gpu, "argv": vllm_args(remote, VLLM_PORT)}


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
