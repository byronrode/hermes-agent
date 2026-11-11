# Post-authorization messaging source and return

## Observing an existing native response owner

The selected source provider may call
`gateway.accepted_source.record_gateway_observation(source=source, observation=proof)`
from the admitted native tool execution. Its optional
`validate_gateway_observation(source, observation)` method must freshly verify
the same source's existing native response owner and return exactly `True`.
Absent, failed or mismatched proof leaves normal publication available.

Successful observation creates only an in-process event receipt. Progress,
queued edits, status, streamed commentary and the final secondary response are
muted while that exact source, profile, run generation and provider registration
remain valid. A distinct queued user message keeps its response. Revocation does
not revive when a provider is re-added. Model silence markers, ingress metadata
and copied receipts cannot select this policy. Native task ownership, authorization,
durable source records and the original owner's reply route remain unchanged.

## Current source custody

`gateway.accepted_source.current_gateway_source(session_id=...)` is a trusted
in-process consumer API. Public IDs, hook metadata and copied dictionaries are
not credentials. The initial supported transport is native Discord, default
profile, external human message only. It requires current gateway authorization,
the live receiving adapter's SDK connection/message identity, current session run
generation, native agent turn/session correlation and the actual agent worker
thread. Delegated execution, internal events, foreign homes/profiles, stale scopes,
and background copied contexts cannot resolve it. Scope exit revokes every copy.
The owning agent worker and its actually registered native tool workers may
exercise source intake. Arbitrary copied helper threads and interrupted or
abandoned workers may not. Both sequential and parallel tools use Hermes' native
worker registration; no thread whitelist or second executor is introduced.
Authorization remains independent of business permission.
`admit=False` permits the existing pre-LLM guidance hook to inspect current
native ownership without admitting a durable source. It still requires the real
agent object's turn, never a caller/hook-provided turn ID. The actual pre-LLM
hook runs before `_persist_turn_start`: inspection requires current revocable
runtime ownership but no persisted candidate. The existing bounded pre-LLM
worker receives inspection only, tied to its exact thread and actual parent
execution; callback return or timeout revokes it, even if the worker is late.
Arbitrary copied background contexts and admitting calls remain denied.
It creates no durable authority;
admitting tool use still requires the exact persisted candidate row. Provider replacement
revokes the current scope even if the old registration is later restored.

The gateway stamps `gateway_source_candidate` on its existing persisted native
user row. On first qualified tool use, SessionDB conditionally binds
`gateway_accepted_source` to the actual core turn. Replay cannot bind a different
turn. The live user metadata carries that same binding through turn-end flush.
There is no new source table or registry. Metadata contains IDs, profile/home
fingerprint and input hashes, not a second copy of channel history or prompts.

The immutable snapshot includes platform/profile/home scope, native message/user/
guild/channel/thread, receiving transport profile, session/key, native turn ID,
normalized event and native SDK content hashes, and provider/intent. `run_id` is the existing
consumer contract's stable source key, derived by Hermes from the native event;
it is **not** an API execution run or the native model turn ID. It remains stable
across session reset so replay cannot acquire another admission. `auth_scope` is a
source fingerprint, not a shared HTTP API key or a grant.

An existing `register_api_source_provider` may opt in with synchronous
`qualify_gateway_source(snapshot)` returning exactly `{'intent': ...}` or None,
and async `bind_gateway_receipt(source, receipt)`. Qualification is pure and
multiple matching providers deny authority. Provider registration/unload remains
profile-local. `read_result(source)` uses the existing bounded accepted-source
result shape and validator. It must revalidate its own business/canonical source
and native work on every read; it cannot choose the original recipient.

The existing supervised completion watcher calls `drain_gateway_sources` while
sessions are idle. It projects at most 100 accepted user rows ordered by the
existing custody metadata's `gateway_source_poll_sequence`. Each attempted row
is conditionally stamped with the next sequence before inspection, so revoked
or unknown sources cannot monopolize the page, even after restart or a drain
deadline partway through a page. This scheduling counter never changes source
authority, meaningful progress or delivery state. The watcher revalidates
current authorization and the exact delivery adapter before and after producer
read/send, commits the exact detached SessionDB result and uses the existing
`delivery_obligations` table for the native send. No new model turn, worker,
scheduler or daemon is created. The adapter receives the stored exact channel,
thread and original reply anchor; no caller destination is accepted.

Source obligations add immutable `source_binding` and `native_message_id` fields
to that existing outbox. They store a content hash instead of a second copy of
result text; the actual text stays in its existing SessionDB result row.
`pending → unknown` is conditional **before** awaiting
send. Only a successful native send with message ID, still-current adapter,
provider/profile/authorization and process owner may mark it delivered. Unknown
or lost ACK remains held and is excluded from generic blind redelivery. A pending
unattempted obligation may be claimed once after restart. A delivered obligation
is re-read and only its provider-binding callback retried, never sent again.
Minimal source-bound outbox receipts are retained for subsequent OS/canonical revalidation;
both age and capacity pruning exclude every source-bound obligation, including
pending, unknown and delivered-awaiting-provider-binding receipts. Ordinary
rows retain their separate 500-row capacity and normal age/recovery behavior.

`delivery_ledger.source_delivery_receipt(source, identity)` resolves the native
ACK from durable outbox custody. Consumers must also validate the exact OS source
and result rows, metadata and current canonical work; a callback Boolean is not
proof. Only after the provider callback succeeds is the source row marked
consumed. A callback may explicitly return `False` after binding a qualified
interim notice; its distinct immutable delivery identity is not resent, and the
source stays pending for the final result. Text ACK does not imply attachment
transfer or human acceptance.

Regression proof uses real Discord SDK Message/connection objects, gateway
allowlist authorization, native generated turn IDs and temporary SQLite owners.
Model output and network send are synthetic. It exercises unauthorized/internal/
delegated/foreign-profile/forged SDK/replay/copy-context rejection, A→B→A home and
adapter replacement, restart, lost central ACK, unknown send ACK and late owner
denial. Run with the documented PM dev/test environment plus `--extra discord`
and `HERMES_PYTHON=... scripts/run_tests.sh
tests/gateway/test_authorized_messaging_source.py`.

Focused opening-review repair (same PR, no additional review): source retention
regression inserts 525 ordinary obligations while protecting old pending,
unknown and delivered source receipts. Queue regression persists 101 revoked
unknown sources before a deliverable source, reopens SQLite between drains and
verifies a single later ACK without touching held source authority or send state.
The connected Mara test invokes the actual pre-LLM collector before the actual
turn-start persistence helper and verifies guidance before durable admission.
All SDK/model/network fixtures are synthetic; these are source-owner tests,
not live Discord acceptance.

Focused and broader verification: 216 tests across 18 files pass through the
PM source runner, including hook failure/event/stream regressions.

## Settled API source validation after cache retirement

A parent read-only/live retry reported HTTP 503 with diagnostic
`producer_validation / ValueError / source_owner_unavailable` after a remote
producer read. The original stored result and central receipt were preserved.
The native HTTP/SQLite regression reproduces the mechanism: after an actual
original owner process exits, the replacement serves its durable completed run;
while the synthetic producer awaits, `_sweep_orphaned_runs_once` expires terminal
status/transport, and `_release_run_owner_if_forgotten` drops the cache owner.
Previously, post-await source validation required that transient stamp and
rejected the otherwise qualified reservation. Both first delivery and repeat of
an already persisted result failed. This proves a matching mechanism locally;
installation and an exact original live retry remain parent-owned acceptance.

Post-await validation now reuses the existing authenticated durable-run lifecycle
to hydrate normally retired cache state, then applies all original source,
profile, fingerprint, provider, snapshot/epoch, terminal-state and old-owner
liveness checks. An explicit foreign cache owner is denied and never overwritten.
The original durable source/status/PID/start/fingerprint and producer snapshot
are not rewritten. Recovery does not launch another worker. The same validation
covers snapshot-recovery and pre-commit await boundaries; IDs and copied producer
metadata do not grant authority.

Regression uses actual API-generated runs, real owner-process exit and temporary
SQLite custody, native sweeper calls during the producer read, and HTTP result
commit/retry to the exact original row. Synthetic provider/model transport is
explicit; no private history or production fixture is read or changed. Existing
live-owner/failed/unqualified/foreign-auth denials remain exercised, plus provider
revocation and explicit foreign ownership during the awaited producer read.

Verification: 111 tests across seven owning API/source/run/SSE/room-grant/
multiplex files pass in 20.4 seconds (`/tmp/source-retirement-broad.log`);
the red reproduction is `/tmp/source-retirement-red.log`.
Jev compact advisory receipt is retained in
[settled-source-retirement-jev.json](settled-source-retirement-jev.json).
Invocation: `hermes -p default jev evaluate --file /tmp/settled-retirement-outcome.json`;
input SHA-256 `54efbdae2138fc81d6413757a374cd6e93dd9e21821742be1db388ccbe8afb1e`.
No score establishes execution authority or live delivery acceptance.
