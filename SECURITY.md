# Security policy

## Reporting a vulnerability

Please do **not** open a public issue. Use GitHub's private vulnerability
reporting: **Security → Report a vulnerability** on this repository. You will get
an answer within 72 hours during the hackathon window.

## Scope

Gifford sits between a human keyboard and the agents it drives, so we treat as
security issues in particular:

- any path that writes to an agent's terminal outside the single writer, or
  while the human is typing;
- any path that delivers a message without a channel decision;
- any screen or log that leaves the machine without redaction;
- secrets committed to the repository.

## Supported versions

Only the latest `main` and the latest tagged release.
