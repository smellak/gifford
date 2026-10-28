# Testing

| Command | What it runs |
|---|---|
| `task test` | unit tests, fast, `-failfast` |
| `task test:race` | unit tests with the race detector (what CI runs) |
| `task test:update` | regenerate `.golden` files (explain them in the PR) |
| `task test:record` | re-record model cassettes; needs a live endpoint; never in CI |
| `task fuzz -- --scenarios N` | keyboard-versus-injection race fuzzer |
| `task conformance` | conformance suite against installed harnesses |

Rules: table tests; `t.Parallel()` unless a real shared resource such as a PTY;
`t.TempDir()` and `t.Setenv()`; testify `require`. Tests never call a real model
endpoint (cassettes) and never drive a real harness (`test/fakeharness`), except
the suites under `test/`, which run behind build tags. A test that takes more than
ten seconds moves to `test/`.

Fixtures with deliberately seeded secrets are named `testdata/redact/seeded_*` so
gitleaks ignores them; any other secret in the repository is an incident.
