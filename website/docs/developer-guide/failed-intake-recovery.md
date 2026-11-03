# Qualified failed-intake recovery

The existing gateway source drain can retry a provider's opted-in failed intake
without running another model turn or receiving another user message. The provider
declares `gateway_intake_tool`, `validate_gateway_intake_recovery()` and
`retry_gateway_intake()`. The actual consumer is Mara's source provider.

Only the gateway owner reconstructs authority: exactly one persisted Discord user
row with its native candidate and accepted source, one matching stored tool call
(direct or the native single-call `tool_call` envelope), and its bound tool result.
The result must explicitly report `canonical_capture` failure, false execution
and delivery verification, and no returned item receipt. No caller payload or
source metadata is accepted as authority. Multiple/ambiguous/successful tool
results and unauthenticated source failures are unavailable.

Current source authorization, profile/home, transport owner, provider and source
policy are rechecked. A native cross-process conversation lease excludes live
turns/recoveries. The source user row's existing display metadata holds the exact
raw-call/result intent hash, attempt, execution UUID, PID/start incarnation and
compact receipt. Source/run/session/turn/message and original tool rows are never
rewritten. CAS checks source custody, previous recovery state and both original
tool-row hashes. A live recorded recovery PID denies takeover even after lease
expiry. A dead owner is recoverable using the same immutable evidence; providers
must use native conditional/idempotent capture/admission to recover lost ACKs.

Recovery is limited to three attempts and a120-second validation budget (native
provider operations also retain their own deadlines). Completed receipts are
revalidated and reused without dispatch. No lease/receipt/custody reset operation
is exposed. An unavailable owner/policy or exhausted attempt stays unavailable.
The normal `current_gateway_source()` remains revoked after the original turn.
Only the internal recovery callback thread can inspect
`current_intake_recovery()`; copied contexts in another thread cannot use it.

The existing API-server operator transport also exposes:

`POST /v1/gateway-sources/{original_message_id}/retry-intake` with JSON `{}`.

It requires the existing API operator authentication (room grants denied).
This credential selects an already qualified durable failure; it cannot create
a channel gesture or supply another source, payload, turn or recipient. The
connected gateway runner must be active. Success returns source IDs, the intent
hash, recovery execution UUID and per-item capture/admission receipt; it is not
delivery proof. Conflict returns a bounded sanitized reason. Disconnecting the
HTTP caller does not abandon a worker: existing API worker lifetime accounting
keeps its SessionDB alive and the durable PID claim prevents duplicate work.

Install through the normal native updater and managed gateway restart. With no
provider callback, old providers are unchanged. Install the consuming provider
only when its current company/source policy is configured. The existing periodic
drain may immediately recover stored qualified failures after restart, so operator
activation must anticipate the original authorized work and its existing return
route. No new daemon, store, credential or grant is required.

Rollback through the supported updater to the previous source/provider disables
retry. Preserve the native source, tool rows, recovery receipt and any existing
canonical work; never clear them or reexecute a completed worker. Existing result
delivery continues through SessionDB, the delivery ledger, adapter ACK and provider
central binding, with unknown ACKs held by their original owner.

Tests use real SDK/HTTP/SQLite and a real process lifetime, with explicitly
synthetic provider/model/channel transports. They do not establish live delivery.
