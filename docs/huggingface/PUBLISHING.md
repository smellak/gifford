# Publishing the Analyst adapter — proposal (draft)

> Proposal only. The final publish / no-publish decision for every item is
> the maintainers'. This file lists what is proposed for
> publication on Hugging Face at the end, and what must never be published.
> Two rules are fixed and non-negotiable.

## Fixed rules

1. **Weights never pass through an intermediate server.** They are
   uploaded directly from where they are trained to the Hub. No staging
   copy on shared infrastructure, no relay through chat, mail, or
   file-transfer tools.
2. **Raw recordings and captures are never published.** No raw terminal
   recordings, no replay captures, no unprojected screens — not on the Hub,
   not in the repository, not anywhere public.

## Proposed: publish

- **The adapter weights** (LoRA: adapter tensors plus metadata naming the
  base checkpoint, revision, digest, and training run), uploaded per rule 1
  above, once the adapter gate passes and its report is published.
- **`MODEL_CARD.md`** (this directory): the adapter's contract, serving,
  uses, limitations, and gate criteria, with figures filled from the gate
  report at publish time.
- **`DATASET_CARD.md`** (this directory): the window format, labels,
  splits, and the redaction and privacy process. No windows travel with it.
- **Evaluation reports**: the frozen gate report (metrics, criteria
  verdicts, calibration artifact references) and the already-public
  `docs/RESULTS.md` figures they build on.
- **Serving code and configuration shapes** already public in the
  repository (`ml/serve/`, example configurations with placeholders).
  Placeholders only: never a filled-in configuration.

## Proposed: never publish

- Raw recordings and captures, in any form.
- Unprojected screens.
- Mask files, workspace names, machine, account, or domain names.
- Driver timelines, hook event spans, label sidecars, few-shot example
  pools, tuning-half predictions used to fit thresholds.
- Deployment configurations with values (URLs, tokens, revisions tied to
  private infrastructure), gateway profiles with secrets, Modal secrets.
- Anything that entered the repository's ignore list (captures,
  recordings, datasets, keys, local databases).

## Privacy and repository rules (public repository, Apache-2.0)

- No private data in any published file: no host names, no operator
  account names, no application identifiers, no URLs or tokens, no message
  identifiers from the coordination hub.
- No internal milestone or phase codes: everything in plain words.
- Only what is already in `docs/` and `ml/` on the main branch is stated.
  Missing data stays visibly marked **PENDING** and goes back to the
  maintainers as a question instead of being filled in.
- The dataset license itself is **PENDING** until the publication decision
  (see the dataset card front matter).

## Open questions for the maintainers

1. Base checkpoint pinning: which exact base revision the published
   adapter targets.
2. Dataset license for the Hub (code is Apache-2.0; data needs its own
   decision).
3. Whether the blind-test session list digests are published with the gate
   report, or kept private.
4. Hub namespace and repository names for the adapter.
5. Whether the projected dataset windows are published at the end, after
   a privacy review and subject to the providers' terms, and under which
   license. Still never published either way: raw recordings and captures,
   unprojected screens, masks, timelines, sidecars, few-shot pools,
   configurations with values, and ignored files.
