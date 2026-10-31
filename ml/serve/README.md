# ml/serve — Muse Glimmer on Modal

The Glimmer endpoint that `internal/gateway` calls. It has two parts:

- **vLLM.** Its OpenAI-compatible server serves one set of Muse Glimmer base weights with prefix caching on (`--enable-prefix-caching`, so the fixed system message is cached). When the configuration has an `[adapter]` table, it also serves the Analyst LoRA as an adapter (`--enable-lora`); without one it serves the base model only. It is bound to localhost and never public.
- **A thin endpoint in front.** It speaks the gateway wire format (`wireRequest` and `wireResponse`) behind a Bearer token. The model call runs in a worker thread, so concurrent requests overlap. Any malformed model output gives a fixed `502 {"error":"model call failed"}`.
  - It picks the base model or the LoRA from `inference_profile` and `adapter_name`/`adapter_revision`. With no adapter served, every `analyst-lora` request gets `400 {"error":"adapter not served"}`, whatever names it carries, empty ones included.
  - It reads the label of each contract from the logprobs of the first token.
  - It never logs payloads, prompts, screens or answers.

Nothing here is imported by the Go binary. Weights, adapters, data and secrets never enter the repository.

## Layout

- `glimmer_serve/endpoint.py`: the wire format, Bearer check, model choice, logprob reading and evidence. It uses the standard library only.
- `glimmer_serve/asgi.py`: one `POST /infer` route, with no framework.
- `glimmer_serve/config.py`: reads the TOML configuration and derives everything the Modal app applies: the vLLM command line and environment, the volumes and the deployment limits.
- `glimmer_serve/modal_app.py`: the Modal app: `check_config` and `download_weights`, which run without a GPU, and `serve`. It needs Modal, so tests never import it.
- `serve.example.toml`: the configuration with an adapter, with placeholders.
- `serve.base-only.example.toml`: the configuration of a base-only deployment, with placeholders.
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

## Base model only

A deployment serves the base model only when its configuration has no `[adapter]` table. This is the first deployment, before any adapter is trained; `serve.base-only.example.toml` shows it.
- vLLM runs without `--enable-lora` and `--lora-modules`. The rest of its command line is the same.
- The `glimmer-adapters` volume is neither created nor mounted.
- `inference_profile: "base"` works as always; `analyst-lora` gets a 400.

An `[adapter]` table that is there needs all four keys: `name`, `revision`, `module` and `path`. An empty or partial table is refused with an error that names the missing key, never a value, rather than read as base only.

## Limits

The `serve` function's limits are fixed in `config.serve_options`, and no configuration raises them:
- at most one container (`max_containers=1`);
- none kept warm (`min_containers=0`): an idle container stops after `scaledown_window`, 300 s, so the deployment scales to zero;
- `startup_timeout` from `[serve] startup_timeout_s`, required, 60 to 1800 s. The same figure bounds the wait for vLLM to become ready, so the two never disagree.

These are the Modal client's parameter names from version **1.1.4**, which added `startup_timeout`; `max_containers`, `min_containers` and `scaledown_window` exist since 0.73.76. Deploy with a Modal client of 1.1.4 or later.

## Deployment runbook (CTO)

1. **Configuration.** Copy `serve.example.toml`, or `serve.base-only.example.toml` for the base model only, outside the repository and fill in the placeholders:
   - the base repo and a pinned `revision` (a commit, never a branch);
   - `quantization`, if any;
   - with an adapter, its name, revision, module and path;
   - `gpu`;
   - a pinned `vllm_version`, then check the flags in `config.vllm_args` against that version;
   - `startup_timeout_s`.
2. **Secrets.** Run `modal secret create <secret_name> GLIMMER_TOKEN=<token>`. Generate the token, for example with `openssl rand -hex 32`, and give it only to the gateway. If the base weights need a Hugging Face token, also run `modal secret create <hf_secret_name> HF_TOKEN=<token>` and set `hf_secret_name` in `[serve]`; without that key, the download runs with no token. No token ever goes in the configuration.
3. **Check the worker.** From `ml/serve/`, run `GLIMMER_SERVE_CONFIG=/path/to/serve.toml modal run -m glimmer_serve.modal_app::check_config`. It needs no GPU: the worker imports the app, reads the configuration from its image and returns what `serve` would run with: whether it serves the base model only, the limits, the volumes and the vLLM command line. The configuration text travels in the image environment (`GLIMMER_SERVE_CONFIG_TOML`), so the worker never needs the local file. It holds no secret; the tokens stay in the Modal secrets.
4. **Download the weights.** From `ml/serve/`, run `GLIMMER_SERVE_CONFIG=/path/to/serve.toml modal run -m glimmer_serve.modal_app::download_weights`.
   - It runs on Modal without a GPU, for up to an hour.
   - It downloads the exact base repo and revision into the Hugging Face cache on the `glimmer-weights` volume, the one vLLM reads, then commits the volume.
   - Nothing downloads on the deploying machine.

   Do this before the first deploy. `serve` runs vLLM offline (`HF_HUB_OFFLINE=1`), so if the weights are missing its start fails fast, instead of downloading them with the GPU on. With an adapter, also upload it: `modal volume put glimmer-adapters <local-dir> /analyst/<revision>`.
5. **Deploy.** From `ml/serve/`, run `GLIMMER_SERVE_CONFIG=/path/to/serve.toml modal deploy -m glimmer_serve.modal_app`.
6. **Gateway.** Point the environment variables named in the gateway config at the deployed URL plus `/infer` and at the same token. With an adapter, set `adapter_sha256` in the gateway profile to the adapter's digest; a base-only deployment serves only the `base` profile.
7. **Smoke test.** Run one `ANALYZE_AGENT_STATE` call through the gateway and check that the call record says `ok`.
8. **GPU choice.** Measure latency on a small and a larger GPU against the gateway's 3 s deadline, then keep the cheaper GPU that holds the deadline at p95.

Rotate the token by updating the secret and the gateway environment together.
