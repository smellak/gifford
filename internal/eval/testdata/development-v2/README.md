# Adversarial development corpus v2

This public synthetic corpus is for selecting a v2 checkpoint. It is not a
training set, calibration set, readiness input, adapter-gate input, or fixed
safety regression. No fixed safety fixture and no synthetic training example
was inspected, copied, paraphrased, or reused to make it.

The corpus is derived only from the state contract in ML_CONTRACT sections 9
and 10, its permission-dialog v2 policy, and the standard window contract. Its
base commit is 4bbc6ec82e3391812be9fa26fea0b2a1d599c5a6. The dataset seal is sha256:b0284d78398a164474d6b8f4d5d96c5fddc1729b3f7cbd48133498b1a3cff9c5; SHA256SUMS seals every public
file in this directory other than itself.

## Shape

- 120 schema-1 windows: 10 families × 6 harnesses × 2 variants.
- 36 A controls (30%), 12 B, 12 C, 24 D, 24 E, and 12 F.
- Harnesses: Claude Code, Muse Code, OpenCode, Antigravity, Codex, and generic.
- Every window has source_quality=synthetic, target.source=synthetic, and a
  standard manifest entry.
- Every window is routed through the manifest's validation split only so
  gifford eval predict reads it. This split is development routing, not the
  calibration validation split.

families.json freezes each family's state, harness matrix, variants,
provenance, and window ids. The canonical [permission-dialog] examples are
synthetic model inputs only: they carry no live observation seal and grant no
permission or D-truth authority.

## Use

Run prediction or a non-training model-input export with the standard
manifest.json and windows/ paths. Rank already-defined checkpoints from
those development predictions. Do not pass this manifest to gifford eval,
fit temperature or θ from it, add its windows to train, or use it for any gate.

To reproduce into a new directory from the repository root:

    go run ./internal/eval/testdata/development-v2-generator --out /tmp/gifford-development-v2
    cd internal/eval/testdata/development-v2 && sha256sum -c SHA256SUMS
