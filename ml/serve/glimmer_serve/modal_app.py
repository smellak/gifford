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
from pathlib import Path

import modal

from .asgi import make_app
from .config import (
    CONFIG_TEXT_ENV,
    ADAPTERS_MOUNT,
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
from .endpoint import Settings, serving_runtime_version
from .publishing import validate_repo_pair, verify_adapter_bundle
from .startup import health_probe, wait_ready

VLLM_PORT = 8000
# Keep the historical production name when no override is present. A separate
# deployment (for example the judge endpoint) can opt into another Modal app
# without editing this module or replacing the production app.
APP_NAME_ENV = "GLIMMER_MODAL_APP_NAME"
DEFAULT_APP_NAME = "gifford-glimmer"
# Modal's default execution timeout, 300 s, is too short to download large
# weights; the download runs without a GPU, so a long one costs little.
DOWNLOAD_TIMEOUT_S = 3600
CFG_TEXT = config_text(os.environ)
cfg = parse_config(CFG_TEXT)
app_name = os.environ.get(APP_NAME_ENV, DEFAULT_APP_NAME)
if not app_name or len(app_name) > 64 or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in app_name):
    raise ValueError(f"{APP_NAME_ENV}: expected 1..64 lowercase letters, digits or hyphens")
module_path = Path(__file__).resolve()
# On the deploying machine the module lives below <repo>/ml/serve and the
# source documents are added to the image. Modal later reimports it from
# /root/glimmer_serve, where that repository tree intentionally does not
# exist; the already-built image then needs no local-file declarations.
repo_root = module_path.parents[3] if len(module_path.parents) > 3 else None
publication_files = (
    {
        repo_root / "docs/huggingface/MODEL_CARD.md": "/opt/gifford-hf/README.md",
        repo_root / "docs/huggingface/DATASET_CARD.md": "/opt/gifford-hf/DATASET_CARD.md",
        repo_root / "docs/huggingface/PUBLISHING.md": "/opt/gifford-hf/PUBLISHING.md",
        repo_root / "docs/ML_CONTRACT.md": "/opt/gifford-hf/ML_CONTRACT.md",
        repo_root / "docs/RESULTS.md": "/opt/gifford-hf/RESULTS.md",
        repo_root / "LICENSE": "/opt/gifford-hf/LICENSE",
        repo_root / "NOTICE": "/opt/gifford-hf/NOTICE",
    }
    if repo_root is not None
    else {}
)

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(f"vllm=={cfg.vllm_version}")
    .env({CONFIG_TEXT_ENV: CFG_TEXT, APP_NAME_ENV: app_name})
    .add_local_python_source("glimmer_serve")
)
for local_path, remote_path in publication_files.items():
    image = image.add_local_file(local_path, remote_path)
# A base-only deployment neither creates nor mounts the adapters volume.
mounted = {path: modal.Volume.from_name(name, create_if_missing=True) for path, name in volumes(cfg).items()}
weights = mounted[WEIGHTS_MOUNT]
publication_volumes = {ADAPTERS_MOUNT: mounted[ADAPTERS_MOUNT]} if not cfg.base_only else {}
# The Hugging Face token, only if the model needs one: from the Modal secret
# hf_secret_name names, never from the configuration.
hf_secrets = [modal.Secret.from_name(cfg.hf_secret_name)] if cfg.hf_secret_name else []
app = modal.App(app_name)


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
    its service/runtime identity, whether it serves the base model only,
    its limits, its volumes and the vLLM command line. Run with
    `modal run -m glimmer_serve.modal_app::check_config`."""
    remote = parse_config(os.environ[CONFIG_TEXT_ENV])
    return {
        "served_name": remote.served_name,
        "gpu": remote.gpu,
        "quantization": remote.quantization,
        "runtime_version": serving_runtime_version(remote.vllm_version),
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
    volumes=publication_volumes,
    secrets=hf_secrets,
    timeout=DOWNLOAD_TIMEOUT_S,
)
def publish_adapter(
    model_repo_id: str,
    dataset_repo_id: str,
    expected_sha256: str,
    private: bool = False,
) -> dict:
    """Publish the configured adapter directly from its Modal volume.

    The worker verifies the tensor SHA-256 and checkpoint identity before it
    creates or updates the Hub repository. No adapter byte reaches the machine
    that invoked this function.
    """
    from huggingface_hub import CommitOperationAdd, HfApi  # installed with vLLM

    remote = parse_config(os.environ[CONFIG_TEXT_ENV])
    if remote.base_only:
        raise ValueError("publication requires a configured adapter")
    token = os.environ.get("HF_TOKEN")
    if not token:
        raise ValueError("HF_TOKEN is not configured for the publication function")
    model_target, dataset_target = validate_repo_pair(model_repo_id, dataset_repo_id)
    bundle = verify_adapter_bundle(
        remote.adapter_path,
        mount_path=ADAPTERS_MOUNT,
        expected_name=remote.adapter_name,
        expected_revision=remote.adapter_revision,
        expected_sha256=expected_sha256,
    )

    api = HfApi(token=token)
    api.create_repo(repo_id=dataset_target, repo_type="dataset", private=private, exist_ok=True)
    dataset_commit = api.create_commit(
        repo_id=dataset_target,
        repo_type="dataset",
        operations=[CommitOperationAdd(path_in_repo="README.md", path_or_fileobj="/opt/gifford-hf/DATASET_CARD.md")],
        commit_message="Publish the S1 aggregate dataset card",
    )

    api.create_repo(repo_id=model_target, repo_type="model", private=private, exist_ok=True)
    model_operations = [
        CommitOperationAdd(path_in_repo=filename, path_or_fileobj=str(bundle.path / filename))
        for filename in ("adapter_model.safetensors", "adapter_config.json", "gifford_checkpoint_manifest.json")
    ]
    model_operations.extend(
        CommitOperationAdd(path_in_repo=filename, path_or_fileobj=f"/opt/gifford-hf/{filename}")
        for filename in ("README.md", "DATASET_CARD.md", "PUBLISHING.md", "ML_CONTRACT.md", "RESULTS.md", "LICENSE", "NOTICE")
    )
    model_commit = api.create_commit(
        repo_id=model_target,
        repo_type="model",
        operations=model_operations,
        commit_message=f"Publish {bundle.name} {bundle.revision}",
    )
    return {
        "model_repo_id": model_target,
        "dataset_repo_id": dataset_target,
        "private": private,
        "adapter_name": bundle.name,
        "adapter_revision": bundle.revision,
        "adapter_sha256": bundle.sha256,
        "model_commit_oid": getattr(model_commit, "oid", ""),
        "dataset_commit_oid": getattr(dataset_commit, "oid", ""),
    }


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
        runtime_version=serving_runtime_version(cfg.vllm_version),
        gpu=cfg.gpu,
        quantization=cfg.quantization,
        chat_template_kwargs=cfg.chat_template_kwargs,
    )
    return make_app(settings, VLLMBackend(VLLM_PORT))
