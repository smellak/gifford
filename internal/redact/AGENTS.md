# internal/redact — rules for implementing agents

- No secret-shaped literal in any committed file, tests and testdata included: seeded values come from `redacttest.Value` at run time; templates hold `{{kind:name}}` slots; PEM markers are built from parts, in tests too. gitleaks runs in CI and in hooks.
- Fixture content is invented: no real paths, user names, hostnames or pasted session output; hosts end in `example.invalid`.
- A change to a rule, the placeholder, `CellWidth` or the wrap rules bumps `PolicyVersion` and updates the fixture in the same PR; after `ml-contract-v1` it is an ML contract change.
- Never log input, nor put input or option text in an error.
- Production code never imports `redacttest`. A fuzz failure is reduced to a reproducer with no secret shape before it is committed.
