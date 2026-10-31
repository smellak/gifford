# ml/serve — Muse Glimmer on Modal

The Glimmer endpoint that `internal/gateway` calls. It has two parts:

- **vLLM.** Its OpenAI-compatible server serves one set of Muse Glimmer base weights, with the Analyst LoRA as an adapter (`--enable-lora`) and prefix caching on (`--enable-prefix-caching`, so the fixed system message is cached). It is bound to localhost and never public.
- **A thin endpoint in front.** It speaks the gateway wire format (`wireRequest` and `wireResponse`) behind a Bearer token. The model call runs in a worker thread, so concurrent requests overlap. Any malformed model output gives a fixed `502 {"error":"model call failed"}`.
  - It picks the base model or the LoRA from `inference_profile` and `adapter_name`/`adapter_revision`.
  - It reads the label of each contract from the logprobs of the first token.
  - It never logs payloads, prompts, screens or answers.

Nothing here is imported by the Go binary. Weights, adapters, data and secrets never enter the repository.

## Layout

- `glimmer_serve/endpoint.py`: the wire format, Bearer check, model choice, logprob reading and evidence. It uses the standard library only.
- `glimmer_serve/asgi.py`: one `POST /infer` route, with no framework.
- `glimmer_serve/config.py`: reads the TOML configuration and builds the vLLM command line.
- `glimmer_serve/modal_app.py`: the Modal app. It needs Modal and a GPU, so tests never import it.
- `serve.example.toml`: the configuration, with placeholders.
- `tests/`: unit tests with a fake vLLM, using `task ml:serve:test`. They need no GPU, Modal or network.

## How the label is read

`prompt_version` must be the one of the request's contract; any other version gets a 400:

| Contract | `prompt_version` | Letters |
|---|---|---|
| `ANALYZE_AGENT_STATE` | `analyze_agent_state.v1` | A–G, the seven states |
| `TRIAGE_MESSAGE` | `triage_message.v1` | A–D: `NORMAL`, `CONFLICT_OR_URGENT`, `LOW_PRIORITY`, `REQUIRES_HUMAN` |
| `CHOOSE_ORCHESTRATION_ACTION` | `choose_orchestration_action.v1` | one per entry of the payload's `allowed_actions`, in order, A first, as the prompt letters them |

A CHOOSE request whose `allowed_actions` is not a list of 2 to 26 distinct names gets a 400. The chat messages are passed to vLLM as they are, with these settings:
- `max_tokens` 40
- `temperature` 0
- `logprobs` on, `top_logprobs` 20
- reasoning off, via `chat_template_kwargs`

The label is then read in four steps:
1. Take the first token that is not a special token.
2. Over that token's top logprobs, sum each of the contract's letters with its variants (`A`, ` A`, `▁A`, `ĠA`). Any other token counts for nothing, even a letter outside the contract's set.
3. Renormalise over the letters and map them to label names. The answer's `letter_mass` is the probability the letters held before that, at most 1.
4. Pick the label:
   - `ANALYZE_AGENT_STATE`: if the letters hold less than 0.5, the label is `UNKNOWN`. The probabilities stay as they were renormalised, or are `{UNKNOWN: 1}` if no letter appeared at all.
   - `TRIAGE_MESSAGE` and `CHOOSE_ORCHESTRATION_ACTION`: the label is the most probable letter's; nothing else is judged here. The gateway refuses a `letter_mass` below 0.5 and a tie at the top. If none of the letters appeared, the answer is the fixed 502.

Evidence is what follows the first `|`:
- `ANALYZE_AGENT_STATE` and `TRIAGE_MESSAGE`: split on `;`, at most 3 phrases of 1–120 characters each.
- `CHOOSE_ORCHESTRATION_ACTION`: one reason, neither split nor cut. The gateway checks that it is 1–80 characters, then drops it.

## Deployment runbook (CTO)

1. **Configuration.** Copy `serve.example.toml` outside the repository and fill in the placeholders:
   - the base repo and a pinned `revision` (a commit, never a branch);
   - `quantization`, if any;
   - the adapter's name, revision, module and path;
   - `gpu`;
   - a pinned `vllm_version`, then check the flags in `config.vllm_args` against that version.
2. **Secret.** Run `modal secret create <secret_name> GLIMMER_TOKEN=<token>`. Generate the token, for example with `openssl rand -hex 32`, and give it only to the gateway.
3. **Weights and adapter.** The base weights download into the `glimmer-weights` volume on first start. Upload the adapter with `modal volume put glimmer-adapters <local-dir> /analyst/<revision>`.
4. **Check the worker.** From `ml/serve/`, run `GLIMMER_SERVE_CONFIG=/path/to/serve.toml modal run -m glimmer_serve.modal_app::check_config`. It needs no GPU: the worker imports the app, reads the configuration from its image and returns the vLLM command line. The configuration text travels in the image environment (`GLIMMER_SERVE_CONFIG_TOML`), so the worker never needs the local file. It holds no secret; the token stays in the Modal secret.
5. **Deploy.** From `ml/serve/`, run `GLIMMER_SERVE_CONFIG=/path/to/serve.toml modal deploy -m glimmer_serve.modal_app`.
6. **Gateway.** Point the environment variables named in the gateway config at the deployed URL plus `/infer` and at the same token. Set `adapter_sha256` in the gateway profile to the adapter's digest.
7. **Smoke test.** Run one `ANALYZE_AGENT_STATE` call through the gateway and check that the call record says `ok`.
8. **GPU choice.** Measure latency on a small and a larger GPU against the gateway's 3 s deadline, then keep the cheaper GPU that holds the deadline at p95.

Rotate the token by updating the secret and the gateway environment together.
