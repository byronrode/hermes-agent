# Accepted API source metadata

The existing `pre_llm_call` hook receives optional `accepted_api_run` metadata
for a qualified authenticated `POST /v1/runs` execution. Its fields are
`run_id`, `session_id`, resolved `profile`, opaque `auth_scope`,
`message_sha256` and original ISO8601 `accepted_at`. It contains no bearer key,
idempotency key, raw input or destination. Other execution paths omit the field.

The API run owner installs a context-local resolver around its synchronous
conversation execution. `gateway.platforms.api_server_accepted_source.current_accepted_run()`
re-reads the owner's existing durable idempotency record with the exact captured
scope/key/fingerprint and checks run/session/owner identity. Context is reset
even when execution fails, and the resolver is revoked for retained context
copies when its owning scope exits. Delegated execution is rejected using the
existing delegated-child context predicate; hook projection additionally binds
the actual executing agent session. A copied child context cannot borrow the
parent's accepted source, even after the parent exits.
No new store, endpoint, tool, environment variable,
adapter or background process is introduced. Model turns, replayed HTTP
responses, caller metadata and memory author labels do not create admissions.

This allows a concrete trusted source consumer (Mara's company role-status
attestation bridge) to bind a real accepted API input before its existing
query hook runs. In-process consumers must resolve the current owner record;
copied shell JSON is only metadata. Company/role permission and protected
attester transport must remain separate. Never put an attester credential in
the model's tools, process environment or context. This patch installs no
consumer, credentials or grants and sends no attestation.

Qualification requires an authenticated API-key listener and a durable
idempotency reservation for string input. Unauthenticated listeners, room
grants, non-idempotent requests, rich input and execution forwarded to a live
Bot Chat owner remain unqualified. They must not borrow another run's metadata.

Run `scripts/run_tests.sh tests/gateway/test_api_server_accepted_source.py`
and the existing API run tests. The tests use disposable registry files and
actual authenticated in-process HTTP routes with a synthetic model; they do
not launch a provider or send real messages.
