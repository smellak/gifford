# Publishing the Analyst adapter

The CTO is the publisher of record. Publication must preserve the adapter
identity in [MODEL_CARD.md](MODEL_CARD.md) and the evidence boundary in
[DATASET_CARD.md](DATASET_CARD.md).

Published destinations:

- model: [`smellak/gifford-glimmer-analyst-v2`](https://huggingface.co/smellak/gifford-glimmer-analyst-v2);
- card-only dataset: [`smellak/gifford-screens-s1`](https://huggingface.co/datasets/smellak/gifford-screens-s1).

## Publication record

| Item | Value |
|---|---|
| Adapter repository | `smellak/gifford-glimmer-analyst-v2` |
| Weights revision | `d53e64f5` |
| Card and `evidence/` revision | `abe91b8fa61ec1f423f2a710291f33c17b093537` |
| Adapter SHA-256 | `dd28393cf156cd62290b31f24bea795c9d4eed38832c88a6330d0863a10a4c34` |
| Published (UTC) | 2026-11-04T19:24Z |
| Dataset repository | `smellak/gifford-screens-s1`, card only, no rows |
| Visibility | Public since 2026-11-04T20:32Z; both repositories answer anonymous reads |

## Fixed rules

1. Adapter weights upload directly from the Modal adapter volume to Hugging
   Face. They are never downloaded to an operator, build or coordination
   server.
2. The token is loaded from an owner-only (`0600`) file into a named Modal
   secret without printing it. It is never placed in Git, shell tracing,
   command output or a deployment configuration.
3. Raw recordings, captures, dataset rows and private infrastructure values
   are never uploaded.
4. Before upload, verify the adapter directory hash, revision and base-model
   pin against the cards. After upload, record the returned Hub commit and
   verify the public files and license without exposing the token.

## Contents

Publish:

- LoRA tensor and adapter configuration for `glimmer-analyst-v2` revision
  `l56-s1-paso-2055`;
- the model card as the repository `README.md`;
- the Apache-2.0 `LICENSE` and project `NOTICE`;
- aggregate evaluation/evidence files that contain no private rows or paths.

Do not publish:

- base-model weights (the card links to the pinned upstream checkpoint);
- raw or projected dataset rows, captures, masks, timelines or sidecars;
- private URLs, application identifiers, hostnames, accounts, tokens or local
  paths.

The Modal function and operator command are documented in
[`ml/serve/README.md`](https://github.com/smellak/gifford/blob/main/ml/serve/README.md). The function resolves the
configured adapter path inside the mounted adapter volume, rejects paths
outside that volume, and uploads from the Modal worker directly.
