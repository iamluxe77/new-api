# Native durable task retry policy

The request-policy settings API and form configure retry timing for newly
accepted `durable-submit@1` tasks. Update these options through the existing
atomic request-policy write; an invalid value rejects the whole update. The
host stores the validated policy with the task intake and dispatch in the same
transaction. Already accepted tasks keep their original policy. `RetryTimes`
controls ordinary request retries and does not set the native task budget.

| Option | Default | Allowed value | Effect |
| --- | ---: | ---: | --- |
| `DurableTaskAttemptBudget` | 2 | 1-8 | Total supplier attempts, including the first |
| `DurableTaskSuccessorDelaySeconds` | 1 | 1-60 | Delay before a verified safe successor attempt |
| `DurableTaskReconcileDelaySeconds` | 15 | 1-3600 | Base delay for receipt reconciliation and temporary worker failures |
| `DurableTaskReplayInitialDelaySeconds` | 60 | 30-3600 | First same-key replay delay |
| `DurableTaskReplayMaxDelaySeconds` | 3840 | Initial delay-86400 | Cap for same-key exponential backoff |
| `DurableTaskReplayJitterSeconds` | 10 | 0-60 | Additional same-key replay delay; zero disables it |
| `DurableTaskUncertainRetryEnabled` | false | Boolean | Explicitly permit at most one replacement after an unknown outcome |
| `DurableTaskUncertainRetryDelaySeconds` | 60 | 60-3600 | Minimum delay before that replacement can be considered |

Same-key replay doubles its delay up to the configured cap. A supplier
`Retry-After` can extend reconciliation delay up to one hour; the configured
base delay cannot shorten it. The existing 29-day replay window remains fixed.
The delay is an earliest eligible time, not an exact wall-clock schedule; the
native worker scans due dispatches on the system task runner's 15-second
schedule while idle.
Older dispatch rows without a policy version use the historical timing
defaults, independently of current operator settings.

Timing and budget changes never authorize a new supplier key by themselves.
After an uncertain send, reconcile the original receipt first. A successor
requires an authoritative `not_accepted` receipt, final zero supplier charge,
explicit safe-resubmission evidence, and a qualified temporary cause. A
receipt already observed durably fences off automatic same-key replay.
Changing this policy does not change the frozen account, route, wire, or client
idempotency key. An unknown outcome remains unknown until authoritative
evidence resolves it.

The receipt ID identifies the original submission. A final customer refund
has a separate receipt ID; refreshing an unknown receipt never substitutes the
refund ID for the submission ID. Once an attempt is accepted with a bound task
identity, subsequent read-only reconciliation follows that exact task and its
final billing. A later by-key nonacceptance response cannot erase acceptance.

The optional uncertain-replacement policy is a separate, default-off operator
risk decision frozen only for newly accepted tasks. It permits one new supplier
key within the existing attempt budget after the minimum delay, a fresh bound
unknown receipt, a direct original-task lookup returning not found, and a
confirmed final zero customer charge. These facts do not establish that the
original supplier generation did not start or that supplier cost is zero. A
late duplicate generation and extra supplier expense remain possible; the
operator bears that cost while the customer is charged at most once. The
original attempt remains unknown and is reconciled read-only, including after
the customer task becomes terminal. A new key is never allocated again for
that task. Before the replacement POST the worker rechecks the original.

Outside this opt-in policy, a final authoritative `not_accepted` receipt with
zero supplier charge and no remaining attempt budget completes through the
native failure outcome and refund path. It does not leave a client reservation
open indefinitely. An unresolved unknown outcome remains in reconciliation.

When either attempt returns a confirmed result, the host fixes one winning
attempt before requesting customer artifact archival. Only the winner can use
the shared public task artifact path. The native pending outcome and financial
settlement follow successful archival of that winner; later results are
retained as private operator evidence and never change the delivered result or
customer charge.
