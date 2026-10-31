# ADR 0002 — Unified node process

- **Status:** accepted
- **Owner:** A-CORE

## Context

The local Hub, control API, delivery worker, Orchestrator, Proxy-session
registry, and federation services share one SQLite database. Splitting those
components into independently supervised processes would create multiple
writers, duplicate recovery and reconciliation loops, and ambiguous ownership
of `hub.sock`, `daemon.sock`, and in-flight delivery evidence.

Delivery recovery must finish before operators can resolve approvals through
`daemon.sock`. Shutdown must also drain the worker and Proxy result consumers
before closing the database they persist into.

## Decision

Run all node services in one operating-system process with one event-store
writer pool. A separate SQLite pool is permitted for metadata reads only when
every connection has `query_only` enabled.

`gifford hub` and `gifford daemon` are aliases for this same foreground node
entry point. They acquire the same `hub.lock`, create the same sockets, run the
same recovery, and start the same loops. Starting either alias while a node is
already running refuses; the commands never divide ownership or cooperate as
two processes.

`gifford up --dev` also invokes the same foreground entry point directly. It
does not spawn children, supervise two aliases, or implement a restart loop.
`gifford up` without `--dev` remains explicitly unimplemented until a service
manager integration is defined.

Offline `gifford db backup` and `gifford db restore` maintenance acquires the
same `hub.lock`. It refuses while either live alias owns the node and never
forms a second writer pool alongside the running service.

## Consequences

- Startup migrations, durable delivery recovery, socket publication, and
  background loops have one ordered lifecycle.
- Shutdown stops and waits for the worker loop, drains the Proxy registry and
  result consumers, closes `daemon.sock`, closes the read pool, and closes the
  event-store writer last.
- `hub.lock` is the process-wide singleton claim for every alias and offline
  database maintenance command.
- Restore migrates a staging copy, preserves the prior database as a private
  recovery file, and publishes a durable swap marker. Startup resolves that
  marker or refuses before it can open or create the canonical database.
- A future `gifford install-systemd` implementation must generate one node
  unit invoking one alias. It must not generate separate Hub and daemon units.
- External supervisors may restart that one process. In-process restart loops
  are intentionally excluded so durable recovery remains the sole restart
  boundary.
