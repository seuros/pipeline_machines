#![allow(clippy::expect_used)]
mod lifecycle;
mod model;
mod scope;

use super::*;
use matryona_machines::testing::{self, refuses};

const ONE: NonZeroU32 = NonZeroU32::MIN;

fn stage<const N: usize, const C: usize>() -> Stage<u64, N, C> {
    Stage::new([ONE; N]).expect("stage")
}

/// A stage of two lanes, the first weighted `first`, the second one.
fn weighted<const C: usize>(first: u32) -> Stage<u64, 2, C> {
    Stage::new([NonZeroU32::new(first).expect("weight"), ONE]).expect("stage")
}

/// A call that must answer `expected` and leave the stage as it was.
fn unchanged<const N: usize, const C: usize, R: core::fmt::Debug + PartialEq>(
    stage: &mut Stage<u64, N, C>,
    expected: &Result<R, Refusal>,
    call: impl FnOnce(&mut Stage<u64, N, C>) -> Result<R, Refusal>,
) {
    assert_eq!(testing::unchanged(stage, call), *expected);
}

/// A call that must be honoured as the repeat `answer`, changing nothing.
fn repeated<const N: usize, const C: usize, R: core::fmt::Debug + PartialEq>(
    stage: &mut Stage<u64, N, C>,
    answer: R,
    call: impl FnOnce(&mut Stage<u64, N, C>) -> Result<R, Refusal>,
) {
    unchanged(stage, &Ok(answer), call);
}

fn dispatched<const N: usize, const C: usize>(stage: &mut Stage<u64, N, C>) -> Dispatch<u64> {
    stage.dispatch(&[true; N]).expect("work")
}

const fn reaped(payload: u64, terminal: Terminal) -> Reaped<u64> {
    Reaped::new(payload, terminal)
}

#[test]
fn capacity_includes_running_and_unreaped_terminal_jobs() {
    let mut stage = stage::<1, 1>();
    assert_eq!((stage.capacity(), stage.lanes(), stage.len()), (1, 1, 0));
    assert!(stage.is_empty());
    let first = stage.submit(0, 7).expect("job");
    assert_eq!(stage.state(first), Some(State::Queued));
    assert_eq!(stage.dispatch(&[false]), None);
    assert_eq!(dispatched(&mut stage).id(), first);
    refuses(&mut stage, Refusal::Full, |s| s.submit(0, 8));
    assert_eq!(stage.complete(first), Ok(Completion::Completed));
    refuses(&mut stage, Refusal::Full, |s| s.submit(0, 8));
    assert_eq!(stage.len(), 1);
    assert_eq!(stage.reap(first), Ok(reaped(7, Terminal::Complete)));
    assert!(stage.is_empty());
    let next = stage.submit(0, 8).expect("replacement");
    assert_ne!(first, next);
    // The slot is held again, under another id; a reaped id is stale for
    // every call, the repeated reap included: the stage keeps no history.
    refuses(&mut stage, Refusal::Stale, |s| s.reap(first));
    refuses(&mut stage, Refusal::Stale, |s| s.complete(first));
    refuses(&mut stage, Refusal::Stale, |s| s.cancel(first));
    refuses(&mut stage, Refusal::Stale, |s| s.confirm_cancel(first));
    assert_eq!(stage.state(first), None);
}

#[test]
fn submit_refusals_in_order() {
    assert!(Stage::<u8, 0, 1>::new([]).is_none());
    assert!(Stage::<u8, 1, 0>::new([ONE]).is_none());
    assert!(Stage::<u8, 0, 1>::with_scope([], 1).is_none());
    assert!(Stage::<u8, 1, 0>::resumed([ONE], 1, 0).is_none());
    let mut stage = stage::<2, 1>();
    refuses(&mut stage, Refusal::UnknownLane, |s| s.submit(2, 1));
    stage.submit(1, 1).expect("job");
    refuses(&mut stage, Refusal::Full, |s| s.submit(0, 1));
    refuses(&mut stage, Refusal::UnknownLane, |s| s.submit(2, 1));
    assert_eq!(stage.begin_drain(), DrainBegin::Begun);
    refuses(&mut stage, Refusal::Draining, |s| s.submit(2, 1));
    refuses(&mut stage, Refusal::Draining, |s| s.submit(0, 1));
}

#[test]
fn fan_out_is_all_lanes_or_none() {
    const HOLE: JobId = JobId::from_raw(u64::MAX);
    let mut stage = stage::<3, 3>();
    let mut out = [HOLE; 4];
    refuses(&mut stage, Refusal::RepeatedLane, |s| {
        s.fan_out(&[0, 0], 7, &mut out)
    });
    refuses(&mut stage, Refusal::UnknownLane, |s| {
        s.fan_out(&[0, 3], 7, &mut out)
    });
    // The first offending lane decides.
    refuses(&mut stage, Refusal::UnknownLane, |s| {
        s.fan_out(&[3, 0, 0], 7, &mut out)
    });
    refuses(&mut stage, Refusal::RepeatedLane, |s| {
        s.fan_out(&[0, 0, 3], 7, &mut out)
    });
    refuses(&mut stage, Refusal::Undersized, |s| {
        s.fan_out(&[0, 1], 7, &mut out[..1])
    });
    assert_eq!(stage.fan_out(&[], 7, &mut out), Ok(0));
    assert_eq!(out, [HOLE; 4]);

    let held = stage.submit(2, 1).expect("job");
    // Three lanes, two free slots: no lane is admitted.
    refuses(&mut stage, Refusal::Full, |s| {
        s.fan_out(&[0, 1, 2], 7, &mut out)
    });
    assert_eq!(out, [HOLE; 4]);

    assert_eq!(stage.fan_out(&[1, 0], 7, &mut out), Ok(2));
    assert_eq!(out[2..], [HOLE; 2], "entries past the count were written");
    let ids = [out[0], out[1]];
    assert_eq!(ids.map(JobId::raw), [2, 3]);
    assert_eq!(stage.issued(), 3);
    assert_eq!(stage.join(&ids), Ok(Join::Pending));
    // Lane order is the order asked for, dispatch order is the scheduler's.
    assert_eq!(dispatched(&mut stage).id(), ids[1]);
    assert_eq!(dispatched(&mut stage).id(), ids[0]);
    assert_eq!(dispatched(&mut stage).id(), held);

    assert_eq!(stage.begin_drain(), DrainBegin::Begun);
    refuses(&mut stage, Refusal::Draining, |s| {
        s.fan_out(&[], 7, &mut out)
    });
    refuses(&mut stage, Refusal::Draining, |s| {
        s.fan_out(&[9, 9], 7, &mut [])
    });
}

#[test]
fn join_waits_for_all_and_refuses_what_it_does_not_hold() {
    let mut stage = stage::<2, 3>();
    let mut ids = [JobId::default(); 2];
    assert_eq!(stage.fan_out(&[0, 1], 7, &mut ids), Ok(2));
    assert_eq!(stage.join(&[]), Ok(Join::Ready));
    for expected in ids {
        assert_eq!(stage.join(&ids), Ok(Join::Pending));
        let dispatch = dispatched(&mut stage);
        assert_eq!(dispatch.id(), expected);
        assert_eq!(stage.complete(dispatch.id()), Ok(Completion::Completed));
    }
    assert_eq!(stage.join(&ids), Ok(Join::Ready));
    let extra = stage.submit(0, 9).expect("job");
    assert_eq!(stage.join(&[ids[0], extra]), Ok(Join::Pending));
    assert_eq!(stage.cancel(extra), Ok(Cancellation::Cancelled));
    // Cancelled outranks pending and ready, wherever it sits.
    assert_eq!(stage.join(&[extra, ids[0]]), Ok(Join::Cancelled));
    assert_eq!(stage.join(&[ids[0], extra]), Ok(Join::Cancelled));
    assert!(stage.reap(ids[0]).is_ok());
    refuses(&mut stage, Refusal::Stale, |s| s.join(&ids));
    refuses(&mut stage, Refusal::Stale, |s| s.join(&[extra, ids[0]]));
    refuses(&mut stage, Refusal::Stale, |s| s.join(&[JobId::default()]));
}

#[test]
fn dispatch_honours_eligibility_weights_and_fifo() {
    let mut stage = weighted::<6>(2);
    let a: [JobId; 3] = core::array::from_fn(|i| stage.submit(0, i as u64).expect("job"));
    let b: [JobId; 2] = core::array::from_fn(|i| stage.submit(1, i as u64).expect("job"));
    // Lane 0 is held back: only lane 1 runs, oldest first.
    assert_eq!(
        stage.dispatch(&[false, true]).map(|d| (d.id(), d.lane())),
        Some((b[0], 1))
    );
    assert_eq!(stage.dispatch(&[false, false]), None);
    let order: [JobId; 4] = core::array::from_fn(|_| dispatched(&mut stage).id());
    assert_eq!(order, [a[0], a[1], b[1], a[2]]);
    assert_eq!(stage.dispatch(&[true; 2]), None);
    // A second empty round changes nothing.
    assert_eq!(
        testing::unchanged(&mut stage, |s| s.dispatch(&[true; 2])),
        None
    );
}

/// Values are valid or do not exist: a hand-built `Dispatch` is held to
/// what a stage could answer, and `Reaped::new` equals what `reap` answers.
#[test]
fn values_validate_like_the_stage_answers() {
    let mut stage = stage::<2, 2>();
    let id = stage.submit(1, 7).expect("submitted");
    let work = dispatched(&mut stage);
    assert_eq!(Dispatch::new::<2>(id, 7, 1), Some(work));
    assert_eq!((work.id(), work.payload(), work.lane()), (id, 7, 1));
    assert_eq!(Dispatch::new::<2>(id, 7, 2), None);
    assert_eq!(Dispatch::new::<2>(JobId::default(), 7, 0), None);
    let other_scope_zero = JobId::from_raw(periscope_machines::scoped(9, 0));
    assert!(!other_scope_zero.is_issuable());
    assert_eq!(Dispatch::new::<2>(other_scope_zero, 7, 0), None);
    assert_eq!(stage.complete(id), Ok(Completion::Completed));
    let reaped = stage.reap(id).expect("reaped");
    assert_eq!(reaped, Reaped::new(7, Terminal::Complete));
    assert_eq!(
        (reaped.payload(), reaped.terminal()),
        (7, Terminal::Complete)
    );
}

#[test]
fn refusals_display_and_are_errors() {
    use std::string::ToString;
    let all = [
        Refusal::Draining,
        Refusal::UnknownLane,
        Refusal::RepeatedLane,
        Refusal::Undersized,
        Refusal::Full,
        Refusal::Exhausted,
        Refusal::Stale,
        Refusal::NotRunning,
        Refusal::NotCancelling,
        Refusal::NotTerminal,
        Refusal::Concluded,
    ];
    for refusal in all {
        let error: &dyn core::error::Error = &refusal;
        assert_ne!(error.to_string(), "");
    }
}
