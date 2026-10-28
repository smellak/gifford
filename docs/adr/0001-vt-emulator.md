# ADR 0001 — VT emulator

- **Status:** pending (opened 2026-10-28)
- **Owner:** A-PROXY

## Context

The Proxy and capture need to reconstruct the screen of five different TUIs from
the raw byte stream, take snapshots and diffs, compute a hash that is stable
against clocks and spinners, and extract the input line.

## Options

1. `github.com/hinshun/vt10x`
2. the `vt` package of `github.com/charmbracelet/x`

## Decision

Pending. To be decided by reconstructing the initial screen of each harness with
both options and comparing.

## Consequences

To be written with the decision.
