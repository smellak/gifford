# Internal communication stress campaign

Evidence level: **📝 recorded in an execution outside the automated suite**.

The release act (record revision `b8af8064`) records a campaign with 1,000
unique communication marks whose final ledger row was `VALIDATED`. The ledger
also retained every stop, substitution, retry and annulment instead of erasing
failed attempts. This page is the public aggregate record; raw panes, ledgers
and event stores are retained privately because they contain operational
identifiers and terminal content.

## What the count means

- The unit is an internal coordination message from agent A to agent B, a
  committed Gifford last-mile notice, B's recorded read and marked reply, and
  delivery/read of that reply by A. Runner instructions did not count.
- “Validated” required durable delivery records plus timestamped terminal
  traces for both participants. Missing trace stopped the campaign even when
  the databases showed delivery.
- The blocks covered Claude Code, Codex and OpenCode across two directly
  connected nodes. Muse Code was excluded because its provider quota was
  unavailable.
- An independent internal verifier recomputed the durable conditions for the
  runner records, manually audited a sample of terminal traces and signed the
  block closures. This remains internal evidence (`📝`), not a second external
  reproduction (`✅`).
- The count is an internal resistance result, not an independent
  reproduction.

## Execution caveats

- The final 1,000 are eventual validations, not 1,000 first-attempt passes.
  Provider failures, wrong recipients, incomplete traces and runner criteria
  produced retained non-validating history rows before a mark was repeated or
  substituted under the runbook.
- The campaign crossed product repairs, release-candidate changes and a node
  restart. It therefore demonstrates a sustained development campaign, not
  one immutable judge build.
- A block labelled “Glimmer active” did not mean the LoRA served every call.
  After one adapter timeout the node selected the base model for most of that
  interval and the LoRA was reactivated near the end. Model identity in the
  per-call record, not the block label, is authoritative.
- Native hooks/plugins observed only; every last-mile delivery in the campaign
  used the PTY path.

## What it does not mean

- a 100% first-attempt success rate or 1,000 independent runs;
- 1,000 autonomous agent reactions attributable to Gifford alone;
- 1,000 terminal notices or 1,000 prompt injections;
- 1,000 independent agents, sessions or harness versions;
- native MCP message-body transport (the campaign's bodies lived in the
  internal coordination system while Gifford handled the last mile);
- a success rate or accuracy measure for the Analyst, the LoRA, the
  deterministic detector or a single immutable release.

For those capabilities, use the separate per-version rows and externally
recorded paths in [RESULTS.md](../RESULTS.md). No broader product claim may be
derived from this aggregate.
