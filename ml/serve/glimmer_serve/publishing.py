"""Fail-closed validation for publishing one adapter from its Modal volume."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

_REPO_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}/[A-Za-z0-9][A-Za-z0-9._-]{0,95}")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class PublishError(Exception):
    """A publication input or adapter bundle was refused."""


@dataclass(frozen=True)
class AdapterBundle:
    path: Path
    name: str
    revision: str
    sha256: str


def validate_repo_id(value: str) -> str:
    """Require an explicit Hugging Face namespace and repository name."""
    if not _REPO_ID.fullmatch(value) or ".." in value or "--" in value:
        raise PublishError("repo_id: expected namespace/repository")
    return value


def validate_repo_pair(model_repo_id: str, dataset_repo_id: str) -> tuple[str, str]:
    """Validate distinct model and card-only dataset destinations."""
    model = validate_repo_id(model_repo_id)
    dataset = validate_repo_id(dataset_repo_id)
    if model == dataset:
        raise PublishError("model and dataset repositories must be distinct")
    return model, dataset


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bundle_file(adapter: Path, name: str) -> Path:
    """Resolve one required regular file without following it out of the bundle."""
    try:
        path = (adapter / name).resolve(strict=True)
    except OSError as exc:
        raise PublishError("adapter bundle is incomplete") from exc
    if not path.is_file() or not path.is_relative_to(adapter):
        raise PublishError("adapter bundle contains an unsafe file path")
    return path


def verify_adapter_bundle(
    configured_path: str,
    *,
    mount_path: str,
    expected_name: str,
    expected_revision: str,
    expected_sha256: str,
) -> AdapterBundle:
    """Resolve and verify the exact configured checkpoint inside its volume."""
    if not _SHA256.fullmatch(expected_sha256):
        raise PublishError("expected_sha256: expected 64 lowercase hex characters")
    try:
        mount = Path(mount_path).resolve(strict=True)
        adapter = Path(configured_path).resolve(strict=True)
    except OSError as exc:
        raise PublishError("adapter volume or configured path is unavailable") from exc
    if not adapter.is_dir() or not adapter.is_relative_to(mount) or adapter == mount:
        raise PublishError("adapter path is outside the adapter volume")

    weights = _bundle_file(adapter, "adapter_model.safetensors")
    config = _bundle_file(adapter, "adapter_config.json")
    manifest_path = _bundle_file(adapter, "gifford_checkpoint_manifest.json")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        json.loads(config.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublishError("adapter metadata is unreadable") from exc

    if manifest.get("adapter_name") != expected_name:
        raise PublishError("adapter name does not match the serving configuration")
    if manifest.get("adapter_revision") != expected_revision:
        raise PublishError("adapter revision does not match the serving configuration")
    if manifest.get("adapter_sha256") != expected_sha256:
        raise PublishError("manifest adapter SHA-256 does not match the expected seal")
    if _sha256_file(weights) != expected_sha256:
        raise PublishError("adapter weights do not match the expected SHA-256")
    return AdapterBundle(adapter, expected_name, expected_revision, expected_sha256)
