# Plan versus delivered product

The PRD and implementation plan preserve the project's historical intent.
They are not a checklist of claims about the judge build. This page is the
short reconciliation for the final release.

| Planned intent | Delivered in the judge build |
|---|---|
| Harness-agnostic communication | MCP messaging, a generic managed PTY path and measured profiles for five harness families. Unknown versions fail closed. |
| Adaptive last mile | Managed PTY delivery is the proven path. Controlled tmux delivery has a documented residual race. Native hooks/plugins/mods observe only; native delivery is not shipped. |
| Model-assisted timing | A trained LoRA can advise the Glimmer route. It is corroborated by a deterministic detector and bounded by the same guards as the model-free route. |
| Model-free operation | The deterministic detector route can deliver without Glimmer when its measured evidence and all guards allow it. |
| Federation | Two directly reachable Linux nodes can enroll, authenticate with mTLS and exchange signed, acknowledged messages under explicit policy. No hosted relay or NAT traversal is shipped. |
| Secure files | Local artifacts are encrypted end to end and acknowledged. Cross-node artifacts are implemented and automatically tested (`TestFederatedArtifactRoundTripOverMTLS`), not externally accepted, so the judge build does not claim them. |
| Human governance | Approvals, pause/resume, scoped controls, budgets and a global off switch are shipped. Several planned convenience controls remain outside the judge build. |
| Broad platform support | The runtime release is Linux-only. Other operating systems are compile checks, not supported runtime targets. |

The authoritative delivery boundary is
[JUDGE_BUILD.md](JUDGE_BUILD.md); architecture is in
[DESIGN.md](DESIGN.md); per-version support is generated in
[COMPATIBILITY.md](COMPATIBILITY.md); and measured evidence is in
[RESULTS.md](RESULTS.md).
