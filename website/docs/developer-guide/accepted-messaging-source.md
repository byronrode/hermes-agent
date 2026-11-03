# Post-authorization messaging source and return

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
agent object's turn, never a caller/hook-provided turn ID. Provider replacement
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
sessions are idle. It projects accepted user-row metadata only, revalidates
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
normal final-response outbox retention/recovery stays unchanged.

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
