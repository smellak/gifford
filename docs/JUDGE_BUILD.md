# Judge build

This page identifies the single build that judges should install and the
claims that belong to it. The release owner fills both placeholders only after
the final tag points at the reviewed commit; a tag or commit copied from a
different build invalidates this page.

| Field | Final value |
|---|---|
| Judge tag | `<JUDGE_TAG_TO_BE_FILLED_BY_MAPPER>` |
| Judge commit | `<JUDGE_COMMIT_TO_BE_FILLED_BY_MAPPER>` |
| Supported platform | Linux amd64 and arm64 |

Before publication, verify that the tag resolves to the commit above and that
the binary reports the same identity:

```sh
git rev-list -n 1 <JUDGE_TAG_TO_BE_FILLED_BY_MAPPER>
gifford version
sha256sum ./gifford ./gifford-tmux
```

Record the two binary hashes with the release assets. Do not replace either
placeholder with a moving branch such as `main`.

## Included in the judge build

- Local MCP messaging: send, list/read, acknowledge and reply.
- The deterministic PTY route and the optional Glimmer-assisted route defined
  in [DESIGN.md](DESIGN.md). Both retain deterministic channel, identity,
  freshness and human-input guards.
- Managed-PTY delivery for the measured profiles in
  [COMPATIBILITY.md](COMPATIBILITY.md), including prepare/commit and the Human
  Draft Guard.
- Observe-only hooks, plugins and mods listed in the compatibility matrix.
  They supply evidence; they are not native delivery transports.
- Human controls: approvals, pause/resume and the global off switch, with
  durable decisions and incident visibility.
- Signed message envelopes and two-node federation over TLS 1.3 with mutual
  certificates and explicit policies.
- Local encrypted artifact transfer, loop budgets, model lifecycle controls,
  breaker fallback and adapter reactivation probes.

## Explicitly not included

- Native in-harness message delivery. Every supported native integration is
  observe-only in this build.
- A hosted relay or automatic NAT traversal. Two nodes must be mutually
  reachable, for example through their own private network or VPN.
- End-to-end encryption for message bodies. Federation is protected in
  transit with mTLS and messages are signed; only artifacts use the separate
  end-to-end encrypted path.
- Cross-node artifact delivery, macOS/Windows runtime support, and support for
  unmeasured harness versions.
- A claim that every automatically delivered notice caused an autonomous agent
  reaction. Delivery, read/acknowledgement and autonomous reaction are
  separate evidence rows in [RESULTS.md](RESULTS.md).

## Evidence boundary

The tag and commit identify code, not a successful live run. Public claims
must cite [RESULTS.md](RESULTS.md), whose symbols distinguish implementation,
automated tests, one recorded external execution and independent
reproduction. The judge build must not inherit stronger claims from another
commit, harness version or model revision.
