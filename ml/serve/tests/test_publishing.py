import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from glimmer_serve.publishing import (
    PublishError,
    validate_repo_id,
    validate_repo_pair,
    verify_adapter_bundle,
)


class PublishingTest(unittest.TestCase):
    def bundle(self, root: Path) -> tuple[Path, str]:
        adapter = root / "adapters" / "analyst" / "r1"
        adapter.mkdir(parents=True)
        raw = b"sealed adapter"
        sha = hashlib.sha256(raw).hexdigest()
        (adapter / "adapter_model.safetensors").write_bytes(raw)
        (adapter / "adapter_config.json").write_text("{}", encoding="utf-8")
        (adapter / "gifford_checkpoint_manifest.json").write_text(
            json.dumps({"adapter_name": "analyst", "adapter_revision": "r1", "adapter_sha256": sha}),
            encoding="utf-8",
        )
        return adapter, sha

    def test_verifies_the_sealed_bundle_inside_the_mount(self) -> None:
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            adapter, sha = self.bundle(root)
            got = verify_adapter_bundle(
                str(adapter), mount_path=str(root / "adapters"),
                expected_name="analyst", expected_revision="r1", expected_sha256=sha,
            )
            self.assertEqual(sha, got.sha256)

    def test_rejects_escape_identity_and_weight_changes(self) -> None:
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            adapter, sha = self.bundle(root)
            outside = root / "outside"
            outside.mkdir()
            with self.assertRaises(PublishError):
                verify_adapter_bundle(
                    str(outside), mount_path=str(root / "adapters"),
                    expected_name="analyst", expected_revision="r1", expected_sha256=sha,
                )
            (adapter / "adapter_model.safetensors").write_bytes(b"changed")
            with self.assertRaises(PublishError):
                verify_adapter_bundle(
                    str(adapter), mount_path=str(root / "adapters"),
                    expected_name="analyst", expected_revision="r1", expected_sha256=sha,
                )

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_rejects_a_required_file_symlinked_outside_the_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            adapter, sha = self.bundle(root)
            external = root / "external.json"
            external.write_text("{}", encoding="utf-8")
            (adapter / "adapter_config.json").unlink()
            (adapter / "adapter_config.json").symlink_to(external)
            with self.assertRaises(PublishError):
                verify_adapter_bundle(
                    str(adapter), mount_path=str(root / "adapters"),
                    expected_name="analyst", expected_revision="r1", expected_sha256=sha,
                )

    def test_requires_an_explicit_safe_repo_id(self) -> None:
        self.assertEqual("gifford/analyst-v2", validate_repo_id("gifford/analyst-v2"))
        for value in ("analyst-v2", "../escape/repo", "org/repo--name", "org/repo/extra"):
            with self.subTest(value=value), self.assertRaises(PublishError):
                validate_repo_id(value)

    def test_requires_distinct_model_and_dataset_repositories(self) -> None:
        self.assertEqual(
            ("smellak/analyst", "smellak/screens"),
            validate_repo_pair("smellak/analyst", "smellak/screens"),
        )
        with self.assertRaises(PublishError):
            validate_repo_pair("smellak/analyst", "smellak/analyst")


if __name__ == "__main__":
    unittest.main()
