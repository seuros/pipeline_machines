# pipeline-machines

> **AMS Welsh**, NCC-4212, *Welsh*-class stage carrier.

Engine-room flight manual: account for bounded cargo stages and unresolved flights.

Allocation-free `no_std` coordinator for fixed lanes and shared bounded job slots.
Copyable payloads should be command IDs or small descriptors; adapters own actual
resources and execute work. WRR selects ready lanes, FIFO within each lane.
External eligibility masks can incorporate arbitration or deadline policy.

Flow credits bound **all unreaped jobs**, including running and terminal jobs.
Fanout admission is atomic across distinct lanes. Fan-in reports readiness over
unreaped handles, not an output aggregation or distributed transaction.

Queued cancellation is immediate. Running cancellation becomes `Cancelling` and
keeps its capacity until the executor confirms cancellation or reports completion.
This never assumes an unresolved external effect has disappeared. Reap only
terminal jobs to return credit; generation checks reject stale handles.
Handle namespaces are scoped to one stage lifetime.

Draining refuses submissions while allowing dispatch and completion. No async
runtime, multi-stage graph orchestration, automatic propagation, result payloads,
durability, or Ruby binding is implemented.

```rust
use core::num::NonZeroU32;
use pipeline_machines::{Stage, State};
# fn example() -> Option<()> {
let mut stage = Stage::<u64, 1, 4>::new([NonZeroU32::MIN])?;
let id = stage.submit(0, 42)?;
assert_eq!(stage.dispatch(&[true])?.id, id);
assert!(stage.complete(id));
assert_eq!(stage.reap(id), Some((42, State::Complete)));
# Some(())
# }
# assert_eq!(example(), Some(()));
```
