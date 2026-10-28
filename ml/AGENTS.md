# ml/ — rules for agents

- Python here runs offline only. Nothing in ml/ is imported by the gifford binary.
- Training starts only after the dataset readiness gate passes.
- Raw captures, datasets and adapter weights never enter the repository.
- Every dataset row has passed internal/redact; the manifest records
  `redaction_policy_version`.
- Base model: Muse Glimmer (Meta, open weights). The adapter is a LoRA on it.
