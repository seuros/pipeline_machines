//! Every transition of a held job: honoured, honoured as a repeat, or refused.

use super::*;

#[test]
fn a_queued_job() {
    let mut stage = stage::<1, 2>();
    let id = stage.submit(0, 1).expect("job");
    refused(&mut stage, Refusal::NotRunning, |s| s.complete(id));
    refused(&mut stage, Refusal::NotCancelling, |s| s.confirm_cancel(id));
    refused(&mut stage, Refusal::NotTerminal, |s| s.reap(id));
    assert_eq!(stage.cancel(id), Ok(Cancellation::Cancelled));
    assert_eq!(stage.state(id), Some(State::Cancelled));
    assert_eq!(
        stage.dispatch(&[true]),
        None,
        "a cancelled job was dispatched"
    );
}

#[test]
fn a_running_job() {
    let mut stage = stage::<1, 2>();
    let id = stage.submit(0, 1).expect("job");
    assert_eq!(dispatched(&mut stage).id, id);
    assert_eq!(stage.state(id), Some(State::Running));
    refused(&mut stage, Refusal::NotCancelling, |s| s.confirm_cancel(id));
    refused(&mut stage, Refusal::NotTerminal, |s| s.reap(id));
    assert_eq!(stage.complete(id), Ok(Completion::Completed));
}

#[test]
fn a_cancelling_job_keeps_its_slot_until_the_executor_answers() {
    let mut stage = stage::<1, 2>();
    let running = stage.submit(0, 1).expect("job");
    let queued = stage.submit(0, 2).expect("job");
    assert_eq!(dispatched(&mut stage).id, running);
    assert_eq!(stage.cancel(running), Ok(Cancellation::Cancelling));
    repeated(&mut stage, Cancellation::AlreadyCancelling, |s| {
        s.cancel(running)
    });
    refused(&mut stage, Refusal::NotTerminal, |s| s.reap(running));
    assert_eq!(stage.cancel(queued), Ok(Cancellation::Cancelled));
    assert_eq!(stage.join(&[running]), Ok(Join::Pending));
    assert_eq!(stage.join(&[queued]), Ok(Join::Cancelled));

    assert_eq!(stage.begin_drain(), DrainBegin::Begun);
    let before = stage.clone();
    assert_eq!(stage.begin_drain(), DrainBegin::AlreadyDraining);
    assert_eq!(stage, before);
    assert!(stage.is_draining() && !stage.is_drained());
    refused(&mut stage, Refusal::Draining, |s| s.submit(0, 3));

    assert_eq!(stage.confirm_cancel(running), Ok(Confirmation::Confirmed));
    assert_eq!(stage.reap(running), Ok(reaped(1, Terminal::Cancelled)));
    assert_eq!(stage.reap(queued), Ok(reaped(2, Terminal::Cancelled)));
    assert!(stage.is_drained());
}

#[test]
fn a_cancelled_job_honours_repeats_and_cannot_complete() {
    let mut stage = stage::<1, 2>();
    let confirmed = stage.submit(0, 1).expect("job");
    let never_ran = stage.submit(0, 2).expect("job");
    assert_eq!(dispatched(&mut stage).id, confirmed);
    assert_eq!(stage.cancel(confirmed), Ok(Cancellation::Cancelling));
    assert_eq!(stage.confirm_cancel(confirmed), Ok(Confirmation::Confirmed));
    assert_eq!(stage.cancel(never_ran), Ok(Cancellation::Cancelled));
    for id in [confirmed, never_ran] {
        repeated(&mut stage, Cancellation::AlreadyCancelled, |s| s.cancel(id));
        repeated(&mut stage, Confirmation::AlreadyCancelled, |s| {
            s.confirm_cancel(id)
        });
        refused(&mut stage, Refusal::Concluded, |s| s.complete(id));
    }
}

#[test]
fn a_complete_job_honours_the_repeat_and_cannot_be_cancelled() {
    let mut stage = stage::<1, 1>();
    let id = stage.submit(0, 1).expect("job");
    assert_eq!(dispatched(&mut stage).id, id);
    // Completion wins a race with a cancellation request.
    assert_eq!(stage.cancel(id), Ok(Cancellation::Cancelling));
    assert_eq!(stage.complete(id), Ok(Completion::Completed));
    repeated(&mut stage, Completion::AlreadyComplete, |s| s.complete(id));
    refused(&mut stage, Refusal::Concluded, |s| s.cancel(id));
    refused(&mut stage, Refusal::Concluded, |s| s.confirm_cancel(id));
    assert_eq!(stage.reap(id), Ok(reaped(1, Terminal::Complete)));
}

#[test]
fn a_reaped_job_is_stale_for_every_call_repeats_included() {
    let mut stage = stage::<1, 1>();
    let id = stage.submit(0, 1).expect("job");
    assert_eq!(dispatched(&mut stage).id, id);
    assert_eq!(stage.complete(id), Ok(Completion::Completed));
    assert_eq!(stage.reap(id), Ok(reaped(1, Terminal::Complete)));
    refused(&mut stage, Refusal::Stale, |s| s.reap(id));
    refused(&mut stage, Refusal::Stale, |s| s.complete(id));
    refused(&mut stage, Refusal::Stale, |s| s.cancel(id));
    refused(&mut stage, Refusal::Stale, |s| s.confirm_cancel(id));
    assert_eq!(stage.state(id), None);
}

#[test]
fn cancelling_the_last_queued_job_gives_up_the_lanes_burst() {
    let weights = [NonZeroU32::new(3).expect("weight"), ONE];
    let mut stage = Stage::<u64, 2, 6>::new(weights).expect("stage");
    let first = stage.submit(0, 0).expect("job");
    let second = stage.submit(0, 1).expect("job");
    let other = stage.submit(1, 2).expect("job");
    assert_eq!(dispatched(&mut stage).id, first);
    // Lane 0 has two turns left; its last queued job is withdrawn.
    assert_eq!(stage.cancel(second), Ok(Cancellation::Cancelled));
    let late = stage.submit(0, 3).expect("job");
    // The burst was given up: lane 1 goes first, lane 0 starts a new one.
    assert_eq!(dispatched(&mut stage).id, other);
    assert_eq!(dispatched(&mut stage).id, late);
}

fn weighted() -> Stage<u64, 2, 6> {
    Stage::new([NonZeroU32::new(2).expect("weight"), ONE]).expect("stage")
}

#[test]
fn cancelling_a_lanes_only_queued_job_does_not_cost_the_lane_its_position() {
    let mut stage = weighted();
    // The cursor sits on lane 0, which has not been served this visit.
    let only = stage.submit(0, 1).expect("job");
    assert_eq!(stage.cancel(only), Ok(Cancellation::Cancelled));
    for (lane, payload) in [(0, 2), (0, 3), (1, 4)] {
        stage.submit(lane, payload).expect("job");
    }
    let lanes: [_; 3] = core::array::from_fn(|_| dispatched(&mut stage).lane);
    assert_eq!(lanes, [0, 0, 1], "lane 0 lost its place or its burst");
}
