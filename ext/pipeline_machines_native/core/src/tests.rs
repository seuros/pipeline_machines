#![allow(clippy::expect_used)]
mod lifecycle;
mod model;
mod scope;

use super::*;

const ONE: NonZeroU32 = NonZeroU32::MIN;

fn stage<const N: usize, const C: usize>() -> Stage<u64, N, C> {
    Stage::new([ONE; N]).expect("stage")
}

/// A call that must refuse with `refusal` and leave the stage as it was.
fn refused<const N: usize, const C: usize, R: core::fmt::Debug + PartialEq>(
    stage: &mut Stage<u64, N, C>,
    refusal: Refusal,
    call: impl FnOnce(&mut Stage<u64, N, C>) -> Result<R, Refusal>,
) {
    let before = stage.clone();
    assert_eq!(call(stage), Err(refusal));
    assert_eq!(*stage, before, "a refusal changed the stage");
}

/// A call that must be honoured as `answer` and leave the stage as it was.
fn repeated<const N: usize, const C: usize, R: core::fmt::Debug + PartialEq>(
    stage: &mut Stage<u64, N, C>,
    answer: R,
    call: impl FnOnce(&mut Stage<u64, N, C>) -> Result<R, Refusal>,
) {
    let before = stage.clone();
    assert_eq!(call(stage), Ok(answer));
    assert_eq!(*stage, before, "an honoured repeat changed the stage");
}

fn dispatched<const N: usize, const C: usize>(stage: &mut Stage<u64, N, C>) -> Dispatch<u64> {
    stage.dispatch(&[true; N]).expect("work")
}

const fn reaped(payload: u64, terminal: Terminal) -> Reaped<u64> {
    Reaped { payload, terminal }
}

#[test]
fn capacity_includes_running_and_unreaped_terminal_jobs() {
    let mut stage = stage::<1, 1>();
    assert_eq!((stage.capacity(), stage.lanes(), stage.len()), (1, 1, 0));
    assert!(stage.is_empty());
    let first = stage.submit(0, 7).expect("job");
    assert_eq!(stage.state(first), Some(State::Queued));
    assert_eq!(stage.dispatch(&[false]), None);
    assert_eq!(dispatched(&mut stage).id, first);
    refused(&mut stage, Refusal::Full, |s| s.submit(0, 8));
    assert_eq!(stage.complete(first), Ok(Completion::Completed));
    refused(&mut stage, Refusal::Full, |s| s.submit(0, 8));
    assert_eq!(stage.len(), 1);
    assert_eq!(stage.reap(first), Ok(reaped(7, Terminal::Complete)));
    assert!(stage.is_empty());
    let next = stage.submit(0, 8).expect("replacement");
    assert_ne!(first, next);
    // The slot is held again, under another id.
    refused(&mut stage, Refusal::Stale, |s| s.complete(first));
    assert_eq!(stage.state(first), None);
}

#[test]
fn submit_refusals_in_order() {
    assert!(Stage::<u8, 0, 1>::new([]).is_none());
    assert!(Stage::<u8, 1, 0>::new([ONE]).is_none());
    assert!(Stage::<u8, 0, 1>::with_scope([], 1).is_none());
    assert!(Stage::<u8, 1, 0>::resumed([ONE], 1, 0).is_none());
    let mut stage = stage::<2, 1>();
    refused(&mut stage, Refusal::UnknownLane, |s| s.submit(2, 1));
    stage.submit(1, 1).expect("job");
    refused(&mut stage, Refusal::Full, |s| s.submit(0, 1));
    refused(&mut stage, Refusal::UnknownLane, |s| s.submit(2, 1));
    assert_eq!(stage.begin_drain(), DrainBegin::Begun);
    refused(&mut stage, Refusal::Draining, |s| s.submit(2, 1));
    refused(&mut stage, Refusal::Draining, |s| s.submit(0, 1));
}

#[test]
fn fan_out_is_all_lanes_or_none() {
    const HOLE: JobId = JobId::from_raw(u64::MAX);
    let mut stage = stage::<3, 3>();
    let mut out = [HOLE; 4];
    refused(&mut stage, Refusal::RepeatedLane, |s| {
        s.fan_out(&[0, 0], 7, &mut out)
    });
    refused(&mut stage, Refusal::UnknownLane, |s| {
        s.fan_out(&[0, 3], 7, &mut out)
    });
    // The first offending lane decides.
    refused(&mut stage, Refusal::UnknownLane, |s| {
        s.fan_out(&[3, 0, 0], 7, &mut out)
    });
    refused(&mut stage, Refusal::RepeatedLane, |s| {
        s.fan_out(&[0, 0, 3], 7, &mut out)
    });
    refused(&mut stage, Refusal::Undersized, |s| {
        s.fan_out(&[0, 1], 7, &mut out[..1])
    });
    assert_eq!(stage.fan_out(&[], 7, &mut out), Ok(0));
    assert_eq!(out, [HOLE; 4]);

    let held = stage.submit(2, 1).expect("job");
    // Three lanes, two free slots: no lane is admitted.
    refused(&mut stage, Refusal::Full, |s| {
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
    assert_eq!(dispatched(&mut stage).id, ids[1]);
    assert_eq!(dispatched(&mut stage).id, ids[0]);
    assert_eq!(dispatched(&mut stage).id, held);

    assert_eq!(stage.begin_drain(), DrainBegin::Begun);
    refused(&mut stage, Refusal::Draining, |s| {
        s.fan_out(&[], 7, &mut out)
    });
    refused(&mut stage, Refusal::Draining, |s| {
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
        assert_eq!(dispatch.id, expected);
        assert_eq!(stage.complete(dispatch.id), Ok(Completion::Completed));
    }
    assert_eq!(stage.join(&ids), Ok(Join::Ready));
    let extra = stage.submit(0, 9).expect("job");
    assert_eq!(stage.join(&[ids[0], extra]), Ok(Join::Pending));
    assert_eq!(stage.cancel(extra), Ok(Cancellation::Cancelled));
    // Cancelled outranks pending and ready, wherever it sits.
    assert_eq!(stage.join(&[extra, ids[0]]), Ok(Join::Cancelled));
    assert_eq!(stage.join(&[ids[0], extra]), Ok(Join::Cancelled));
    assert!(stage.reap(ids[0]).is_ok());
    let before = stage.clone();
    assert_eq!(stage.join(&ids), Err(Refusal::Stale));
    assert_eq!(stage.join(&[extra, ids[0]]), Err(Refusal::Stale));
    assert_eq!(stage.join(&[JobId::default()]), Err(Refusal::Stale));
    assert_eq!(stage, before);
}

#[test]
fn dispatch_honours_eligibility_weights_and_fifo() {
    let weights = [NonZeroU32::new(2).expect("weight"), ONE];
    let mut stage = Stage::<u64, 2, 6>::new(weights).expect("stage");
    let a: [JobId; 3] = core::array::from_fn(|i| stage.submit(0, i as u64).expect("job"));
    let b: [JobId; 2] = core::array::from_fn(|i| stage.submit(1, i as u64).expect("job"));
    // Lane 0 is held back: only lane 1 runs, oldest first.
    assert_eq!(
        stage.dispatch(&[false, true]).map(|d| (d.id, d.lane)),
        Some((b[0], 1))
    );
    assert_eq!(stage.dispatch(&[false, false]), None);
    let order: [JobId; 4] = core::array::from_fn(|_| dispatched(&mut stage).id);
    assert_eq!(order, [a[0], a[1], b[1], a[2]]);
    assert_eq!(stage.dispatch(&[true; 2]), None);
    // A second empty round changes nothing.
    let before = stage.clone();
    assert_eq!(stage.dispatch(&[true; 2]), None);
    assert_eq!(stage, before);
}

#[test]
fn sibling_refusals_map_onto_the_stage_refusal() {
    use flow_machines::Refusal as Window;
    assert_eq!(credit(Window::Insufficient { available: 0 }), Refusal::Full);
    assert_eq!(
        credit(Window::ExceedsInFlight { in_flight: 0 }),
        Refusal::Stale
    );
    assert_eq!(
        lane(priority_machines::Refusal::UnknownLane),
        Refusal::UnknownLane
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
