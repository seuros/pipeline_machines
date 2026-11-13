# pipeline-machines

> **AMS Welsh**, NCC-4212, *Welsh*-class stage carrier.

Engine-room flight manual: account for bounded cargo stages and unresolved flights.

Allocation-free `no_std` coordinator for fixed lanes and shared bounded job slots.
Copyable payloads should be command IDs or small descriptors; adapters own actual
resources and execute work. WRR selects ready lanes, FIFO within each lane.
External eligibility masks can incorporate arbitration or deadline policy.

Flow credits bound **all unreaped jobs**, including running and terminal jobs.
Fanout admission is atomic across distinct lanes. Fan-in reports readiness over
unreaped jobs, not an output aggregation or distributed transaction.

Queued cancellation is immediate. Running cancellation becomes `Cancelling` and
keeps its capacity until the executor confirms cancellation or reports completion.
This never assumes an unresolved external effect has disappeared. Reap only
terminal jobs to return credit; a reaped job's id is stale forever.

Draining refuses submissions while allowing dispatch and completion. No async
runtime, multi-stage graph orchestration, automatic propagation, result payloads
or durability is implemented.

```rust
use core::num::NonZeroU32;
use pipeline_machines::{
    Cancellation, Completion, JobId, Join, Reaped, Refusal, Stage, Terminal,
};

let weights = [NonZeroU32::MIN; 2];
let mut stage = Stage::<u64, 2, 4>::with_scope(weights, 7).expect("two lanes, four slots");

// One descriptor, both lanes, all or none.
let mut ids = [JobId::default(); 2];
assert_eq!(stage.fan_out(&[0, 1], 42, &mut ids), Ok(2));
assert_eq!(stage.fan_out(&[0, 0], 42, &mut ids), Err(Refusal::RepeatedLane));
assert_eq!(ids[0].scope(), 7);

// Lane 1 is held back this round.
let work = stage.dispatch(&[true, false]).expect("lane 0 has work");
assert_eq!((work.id, work.payload, work.lane), (ids[0], 42, 0));
assert_eq!(stage.dispatch(&[true, false]), None);

assert_eq!(stage.complete(work.id), Ok(Completion::Completed));
assert_eq!(stage.complete(work.id), Ok(Completion::AlreadyComplete));
assert_eq!(stage.cancel(work.id), Err(Refusal::Concluded));
assert_eq!(stage.cancel(ids[1]), Ok(Cancellation::Cancelled));
assert_eq!(stage.join(&ids), Ok(Join::Cancelled));

let reaped = Reaped { payload: 42, terminal: Terminal::Complete };
assert_eq!(stage.reap(work.id), Ok(reaped));
assert_eq!(stage.reap(work.id), Err(Refusal::Stale));
assert_eq!(stage.join(&ids), Err(Refusal::Stale));
```

## Contract

- **Capacities.** `Stage<T, N, C>`: `N` lanes share `C` slots; `T: Copy` is
  the descriptor. `N` must be positive and `C` in `1..=u32::MAX`, or the
  constructors answer `None`. A slot is held from admission until `reap`.
  `capacity()`, `lanes()`, `len()` and `is_empty()` read the bounds and the
  jobs held; `state(id)` reads one job.
- **Refusals.** `Refusal` is the crate's one refusal type and a refused call
  changes nothing: not a job, not a slot credit, not an issued id, not the
  scheduler, not the caller's buffer.

  | Call | Honoured | Refused |
  | --- | --- | --- |
  | `submit` | `JobId` | `Draining`, `UnknownLane`, `Full`, `Exhausted` |
  | `fan_out` | ids written | `Draining`, `UnknownLane`, `RepeatedLane`, `Undersized`, `Full`, `Exhausted` |
  | `dispatch` | `Some(Dispatch)`, `None` when nothing eligible is queued | never refuses |
  | `cancel` | `Cancelled`, `Cancelling`, `AlreadyCancelling`, `AlreadyCancelled` | `Stale`, `Concluded` |
  | `complete` | `Completed`, `AlreadyComplete` | `Stale`, `NotRunning`, `Concluded` |
  | `confirm_cancel` | `Confirmed`, `AlreadyCancelled` | `Stale`, `NotCancelling`, `Concluded` |
  | `reap` | `Reaped { payload, terminal }` | `Stale`, `NotTerminal` |
  | `join` | `Ready`, `Pending`, `Cancelled` | `Stale` |
  | `begin_drain` | `Begun`, `AlreadyDraining` | never refuses |

  `submit` and `fan_out` check in the order listed.
- **Repeats.** While a job is held its state proves what already happened, so
  a repeated `cancel`, `complete` or `confirm_cancel` is honoured with an
  `Already…` answer and changes nothing; `confirm_cancel` also honours a job
  cancelled while queued, which is cancelled all the same. The opposite
  outcome is not a repeat: completing a cancelled job or cancelling a complete
  one is `Concluded`. A repeated `reap` is **not** honoured: the slot is free
  and the stage keeps no history, so the id is `Stale` like any other that
  names no held job.
- **Fan-out is all lanes or none.** Every reason to refuse any lane is
  checked before the first job is admitted, and what can still refuse runs on
  copies of the credit window and the issuer, so there is no partial fan-out:
  a refusal leaves the stage and `out` untouched and consumes no id.
- **Output buffer.** `fan_out(lanes, payload, out: &mut [JobId])` writes the
  first `lanes.len()` entries of `out` in lane order, leaves the rest
  untouched and returns the count; a shorter buffer is `Undersized`. Build it
  as `[JobId::default(); K]`. The default is a placeholder, not a sentinel:
  raw zero is never issued and is `Stale` everywhere. Read only the entries
  reported.
- **Dispatch is a drain.** `None` is the honoured answer "no eligible lane
  has a queued job". It changes no job, but the scheduler records what the
  round observed: the lane being visited forfeits its unused turns. A second
  `None` in a row changes nothing. A lane also gives up the rest of its burst
  when its last queued job is dispatched or cancelled. Only a burst already
  begun can be given up: cancelling the only queued job of a lane the
  scheduler has not yet served this visit costs that lane neither its place
  nor its next burst.
- **Join is a query that can refuse.** `Join` holds only readiness; an id
  that names no held job, wherever it sits in the slice, is `Refusal::Stale`.
- **Terminal states.** `reap` answers `Terminal::{Complete, Cancelled}`, the
  only two states a reaped job can be in; `State` is the full lifecycle read
  by `state`.
- **Ids.** `JobId` comes from one `periscope_machines::Issuer` and is never
  reissued, so a reaped job's id is `Stale` forever and a slot reused by a
  later job cannot be resolved by an old id. After
  `periscope_machines::SEQUENCE_MAX` (2^48 - 1) admissions `submit` and
  `fan_out` answer `Exhausted`. `raw` / `from_raw` carry ids across FFI or
  queues. Ids order by admission, which is the FIFO order within a lane.
- **Scopes.** Ids are only meaningful to the stage that issued them.
  `Stage::with_scope(weights, scope)` puts a caller-chosen `u16` in the top 16
  bits of every id; `new` is scope zero. `Stage::scope` and `JobId::scope`
  read it. An id of another scope, or one the stage never issued, is `Stale`
  (`None` from `state`) before any slot is read. Give each stage its own
  scope: the core reads no clock and no random source, so it cannot pick one,
  and two stages left in the same scope issue the same ids.
- **Restarts.** A fresh stage counts from one again, so an id that outlived a
  restart could name a new job. Save `Stage::issued` and rebuild with
  `Stage::resumed(weights, scope, issued)`, or give each incarnation a new
  scope. `resumed` takes the scope explicitly and restores only the id
  sequence: nothing is held, every earlier id is `Stale`, and resubmitting is
  the caller's job. It is `None` when `issued` is beyond `SEQUENCE_MAX`.
- **Siblings.** Slot credits are a `flow_machines::CreditWindow`, lane turns
  a `priority_machines::WeightedRoundRobin`. Their refusals are mapped onto
  `Refusal`, every variant listed, never asserted: `Insufficient` is `Full`,
  `ExceedsInFlight` is `Stale`, `UnknownLane` is `UnknownLane`. The stage
  checks its own slots and lanes first, so none of them is reachable through
  this API, and the stage never panics.
- **Time.** There is none: the stage reads no clock and takes no instant.

The Ruby gem mirrors `Stage`, `Dispatch` and `Reaped` under the same names;
job ids are Integers, unit variants are symbols, and `fan_out` returns the
ids it would have written to `out`.
