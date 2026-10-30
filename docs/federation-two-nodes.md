# Two-node federation runbook

This runbook validates one real message and acknowledgement across two machines,
then exercises the live federation boundary. Use disposable nodes. The examples
use only `node-a.example.invalid` and `node-b.example.invalid`; make those names
resolve through the test network without recording addresses in the repository.

## Topology and prerequisites

| Machine | Node | Organization | Agent | Federation origin |
|---|---|---|---|---|
| A | `node-a` | `org-a` | `codex` | `https://node-a.example.invalid:7443` |
| B | `node-b` | `org-b` after enrollment | `claude` | `https://node-b.example.invalid:7443` |

Both machines need the same Gifford revision, Go, Task, OpenSSL, `jq`, and
`sqlite3`. Keep the XDG config, data, and runtime directories private (mode
`0700`). Config, policy, token, certificate, and key files must be mode `0600`.
Do not paste enrollment tokens into argv, shell history, logs, or this document.

Choose explicit, machine-local paths before running any command:

```sh
export XDG_CONFIG_HOME=/absolute/path/to/xdg-config
export XDG_DATA_HOME=/absolute/path/to/xdg-data
export XDG_RUNTIME_DIR=/absolute/path/to/xdg-runtime
export GIFFORD_CONFIG="$XDG_CONFIG_HOME/gifford"
export GIFFORD_DATA="$XDG_DATA_HOME/gifford"
export GIFFORD_TRUST="$GIFFORD_DATA/trust"
umask 077
mkdir -p "$GIFFORD_CONFIG/policies" "$GIFFORD_DATA" "$GIFFORD_TRUST" "$XDG_RUNTIME_DIR"
chmod 0700 "$XDG_CONFIG_HOME" "$XDG_DATA_HOME" "$XDG_RUNTIME_DIR" \
  "$GIFFORD_CONFIG" "$GIFFORD_CONFIG/policies" "$GIFFORD_DATA" "$GIFFORD_TRUST"
task build
```

## Configure the nodes

On A, create `$GIFFORD_CONFIG/config.yaml` with mode `0600`:

```yaml
schema_version: 1
node_id: node-a
organization_id: org-a
federation:
  listen: ":7443"
  trust_bundle: /absolute/path/to/node-a/trust
  advertise: https://node-a.example.invalid:7443
```

On B, initially omit `organization_id` so that the local node has the explicit
unenrolled state:

```yaml
schema_version: 1
node_id: node-b
federation:
  listen: ":7443"
  trust_bundle: /absolute/path/to/node-b/trust
  advertise: https://node-b.example.invalid:7443
```

Create this policy on A as `$GIFFORD_CONFIG/policies/federation.yaml`:

```yaml
schema_version: 1
rules:
  - id: allow-node-b-codex
    effect: allow
    peer: node-b
    action: deliver_message
    agent: codex
```

Create the corresponding policy on B:

```yaml
schema_version: 1
rules:
  - id: allow-node-a-claude
    effect: allow
    peer: node-a
    action: deliver_message
    agent: claude
```

Start `./bin/gifford hub` in a dedicated terminal on each machine. A serves
federation immediately. B creates its persistent identities and local control
socket but does not serve federation until enrollment is complete.

## Pin and enroll

On each machine, derive the full lowercase SHA-256 fingerprint from its local
certificate. `openssl x509` reads only the certificate block from the combined
mode-`0600` identity file:

```sh
NODE_FINGERPRINT=$(openssl x509 -in "$GIFFORD_DATA/keys/node-tls.pem" -outform DER |
  openssl dgst -sha256 -r | awk '{print $1}')
test "${#NODE_FINGERPRINT}" -eq 64
```

Record A's value as `NODE_A_FINGERPRINT` and B's as `NODE_B_FINGERPRINT`, and
confirm each value out of band on its source machine.

On A, write the one-use token directly to a private file. The CLI emits JSON,
so extract only its `token` member:

```sh
TOKEN_FILE=$(mktemp)
chmod 0600 "$TOKEN_FILE"
./bin/gifford node enroll-token --org org-b --ttl 10m |
  jq -er '.token' >"$TOKEN_FILE"
```

Transfer that file once through an approved secret channel to B, preserving
mode `0600`. On B, join A without putting the token in argv:

```sh
./bin/gifford node join https://node-a.example.invalid:7443 \
  --fingerprint "$NODE_A_FINGERPRINT" --token-file "$TOKEN_FILE"
```

Delete the consumed token file on both sides. Add `organization_id: org-b` to
B's config, stop B's hub cleanly, and restart it. The database already pins the
same organization; the config makes the intended state explicit. Verify on
both machines:

```sh
./bin/gifford node peers
```

Each side must show the other node as `TRUSTED`, with the expected endpoint and
an abbreviated fingerprint. Confirm the full fingerprint separately from the
certificate command above.

## Register the two agents

Configure each harness to launch the absolute local command
`/absolute/path/to/bin/gifford mcp` as its Gifford MCP server. The MCP process
must be a descendant of the registered harness; identity never comes from an
MCP argument.

On A, launch Codex through the proxy:

```sh
./bin/gifford run --agent codex --harness codex -- codex
```

On B, launch Claude Code through the proxy:

```sh
./bin/gifford run --agent claude --harness claude-code -- claude
```

Leave both sessions connected. Directory synchronization runs at startup and
every 60 seconds; after registration, allow one interval for A to import
Claude's node-signed directory record. A key conflict or sync error is a stop
condition, not a reason to weaken trust.

## Positive message and ACK round trip

From the Codex session on A, call `send_message` with:

```json
{
  "to": "claude",
  "subject": "federation 7.5 verification",
  "body": "two-node delivery check",
  "priority": "NORMAL",
  "thread_id": "federation-7.5",
  "ttl": "10m",
  "client_msg_id": "federation-7.5-a-b-1"
}
```

Save the returned `message_id` as `MESSAGE_ID`. On B, call
`list_pending_messages`, then `read_message` with that ID and the returned
`delivery_generation`. Finally call:

```json
{
  "id": "<MESSAGE_ID>",
  "delivery_generation": 1,
  "status": "ACKNOWLEDGED"
}
```

On A, `get_delivery_status` must eventually report message state
`ACKNOWLEDGED` and recipient ACK status `ACKNOWLEDGED`. Before the ACK arrives,
the signed delivery receipt advances A through `STORED_REMOTE`; the ACK from B
is a separate signed federation flow that advances the origin to
`ACKNOWLEDGED`.

Run the verification script on each machine after the round trip. It opens
`gifford.db` read-only and emits `OK` or `FAIL` for every durable step; on the
origin it also runs the opt-in live boundary suite:

```sh
scripts/federation-two-node-check.sh \
  --role origin --data-dir "$GIFFORD_DATA" --message-id "$MESSAGE_ID" \
  --origin-node node-a --destination-node node-b \
  --sender-agent codex --recipient-agent claude \
  --endpoint https://node-b.example.invalid:7443 \
  --fingerprint "$NODE_B_FINGERPRINT"
```

```sh
scripts/federation-two-node-check.sh \
  --role destination --data-dir "$GIFFORD_DATA" --message-id "$MESSAGE_ID" \
  --origin-node node-a --destination-node node-b \
  --sender-agent codex --recipient-agent claude
```

Both commands must exit zero. Each metadata step emits `OK` or `FAIL`; the
origin check also runs the tagged Go suite and ends with `OK live-fedprobe`.

## Live boundary probe

Run the probe from A after the successful delivery exists. It reads the newest
local `STORED_REMOTE` request for B, uses A's enrolled TLS identity, and never
prints a request body, token, private key, or response body:

```sh
GIFFORD_FEDPROBE_ENDPOINT=https://node-b.example.invalid:7443 \
GIFFORD_FEDPROBE_FINGERPRINT="$NODE_B_FINGERPRINT" \
GIFFORD_FEDPROBE_DATA_DIR="$GIFFORD_DATA" \
go test -tags fedprobe ./test/fedprobe -v
```

The suite exposes seven subtests: `tls-1.3`, `tls-1.2-rejected`,
`no-client-certificate`, `unenrolled-client-certificate`, `forged-origin`,
`invalid-signature`, and `exact-replay`. Each appears separately as PASS or
FAIL in verbose Go test output, and any failure makes `go test` exit nonzero.
The exact replay must return the original byte-identical signed receipt twice;
the normal in-process test also proves that this causes no second delivery
effect. Set `GIFFORD_FEDPROBE_TIMEOUT` to a positive Go duration only when the
default ten-second request timeout is unsuitable.

## Remaining destructive and recovery exercises

Perform these only on disposable nodes and preserve the final metadata output,
not message bodies or tokens:

1. Stop B cleanly. Send several new A-to-B messages. A must retain them as
   queued/retryable work. Restart B; the outbox must drain in sequence without
   duplicate messages.
2. Replace B's allow rule temporarily with an explicit deny rule for peer
   `node-a`, action `deliver_message`, agent `claude`; restart B and send a new
   message. A must settle it once as `FAILED_PERMANENT` with a signed
   `CONTROL_DENIED` receipt and no retry. Restore the allow policy afterwards.
3. Exercise a lost response or lost ACK with a test-network fault. Restoring
   connectivity must safely redeliver the exact request and converge without a
   second effect.
4. Run key rotation/revocation checks before node revocation. Revoking a peer is
   terminal: run `./bin/gifford node revoke node-b` on A (or revoke `node-a` on
   B) only as the final exercise on a throwaway pair. A revoked peer must remain
   blocked and cannot be reactivated by a directory import.

## Negative-matrix evidence

| Required case | Live check | Automated evidence |
|---|---|---|
| TLS 1.3 accepted; TLS 1.2 rejected | `tls-1.3`, `tls-1.2-rejected` | `TestFederationTLSAuthorization` |
| No client certificate | `no-client-certificate` | `TestFederationTLSAuthorization` |
| Unenrolled, suspended, or revoked certificate | `unenrolled-client-certificate` covers unenrolled | `TestFederationTLSAuthorization`; `TestEnrollmentJoinsTwoNodesAndRevokesLive` |
| Forged `origin_node` | `forged-origin` | `TestFederatedDeliveryRejectsNonRootMetadataBeforeEffects` |
| Bad Ed25519 signature | `invalid-signature` | `TestFederatedDeliveryRejectsInvalidSenderAuthenticity` |
| Exact replay, same receipt, no second effect | `exact-replay` | `TestFederatedDeliveryStoresClaimsAndReplaysExactReceipt`; `TestFederationProbeExercisesLiveNegativeMatrix` |
| Key rotation and revocation | final throwaway-node exercise | `TestImportedRotationAndRevocationAdvanceOnly`; `TestPeerLifecyclePinsKeyAndNeverRevives` |
| Peer down, outbox accumulates and drains | recovery exercise 1 | `TestOutboundFederationBacklogDrainsInSequenceAfterMTLSPeerReturns` |
| Lost ACK and safe redelivery | recovery exercise 3 | `TestFederatedAckRetriesLostSignedReceipt`; `TestFederatedAckBacklogDrainsInOrderAfterOriginReturns` |
| Accumulated outbox and backpressure | recovery exercise 1 | `TestReplicatorSequencesPerPeerWhileOtherPeerProgresses`; `TestConcurrentReplicatorsDoNotDoubleSendLiveLease` |
| A allows, B denies; final, no retry | recovery exercise 2 | `TestOutboundFederationControlDeniedOverMTLSIsFinalAndOpaque` |
| Peer cannot reactivate a local agent | automated only | `TestImportDirectoryCannotReactivateAgentOwnedByLocalNode` |
| `CONTROL_DENIED` leaks no policy rule | recovery exercise 2 observes only final state | `TestOutboundFederationControlDeniedOverMTLSIsFinalAndOpaque` |

The automated focal suites are:

```sh
go test ./internal/federation ./internal/hub ./internal/registry
go test -race ./internal/federation ./internal/hub ./internal/registry
```
