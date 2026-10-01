# External execution supervision

`hermes_cli.kanban_supervision.handoff_to_supervision` transfers an owning
routing run to an external supervisor. The calling plugin verifies external
execution and supplies its exact reference. The kernel atomically ends the
routing run as released, retains its process provenance, and records a running
supervision run without a worker PID or expiring claim. The card stays running
and no completion or blocked notification is generated.

A worker supplies its exact run ID. Recovery instead supplies the inspected
body digest and can restore only an idle transient block. Changed bodies,
claims, other block types and unmet parents are refused. Existing run history
and original completion gates remain intact. Repeated identical handoffs do
not create another run. `run_handed_to_supervision` allows the owning worker's
stop guard to recognize this durable kernel receipt without trusting a
plugin tool name or success-looking text.
Dispatcher orphan and stale-heartbeat sweeps recognize the active supervision
run in the ledger and leave it running without spawning another local worker.

The API contains no provider routing or product-specific policy. External
supervision verification belongs in the consuming plugin. Callers continue to
use ordinary completion and authority gates for the actual outcome.
