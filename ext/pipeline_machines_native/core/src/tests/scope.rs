//! Scopes, the id sequence, and restarts.

use periscope_machines::{SEQUENCE_MAX, scoped};

use super::*;

fn scoped_stage<const C: usize>(scope: u16) -> Stage<u64, 2, C> {
    Stage::with_scope([ONE; 2], scope).expect("stage")
}

#[test]
fn ids_carry_the_scope_and_a_sequence_that_never_repeats() {
    let mut stage = scoped_stage::<1>(7);
    assert_eq!((stage.scope(), stage.issued()), (7, 0));
    let first = stage.submit(0, 1).expect("job");
    assert_eq!((first.scope(), first.raw()), (7, scoped(7, 1)));
    assert_eq!(JobId::from_raw(first.raw()), first);
    assert_eq!(stage.cancel(first), Ok(Cancellation::Cancelled));
    assert!(stage.reap(first).is_ok());
    let second = stage.submit(0, 1).expect("job");
    assert_eq!(second.raw(), scoped(7, 2));
    assert_eq!(stage.issued(), 2);
    assert_eq!(super::stage::<1, 1>().scope(), 0);
    assert_eq!(JobId::default().raw(), 0);
}

#[test]
fn a_refused_call_consumes_no_id() {
    let mut stage = scoped_stage::<1>(1);
    refused(&mut stage, Refusal::UnknownLane, |s| s.submit(2, 1));
    stage.submit(0, 1).expect("job");
    refused(&mut stage, Refusal::Full, |s| s.submit(0, 1));
    refused(&mut stage, Refusal::Full, |s| {
        s.fan_out(&[0, 1], 1, &mut [JobId::default(); 2])
    });
    assert_eq!(stage.issued(), 1);
}

#[test]
fn an_id_of_another_scope_or_never_issued_is_stale() {
    let mut a = scoped_stage::<2>(1);
    let mut b = scoped_stage::<2>(2);
    let from_a = a.submit(0, 1).expect("job");
    let from_b = b.submit(0, 1).expect("job");
    assert_eq!(from_a.raw() & 0xFFFF, from_b.raw() & 0xFFFF);
    let ahead = JobId::from_raw(scoped(2, 2));
    let zero = JobId::from_raw(scoped(2, 0));
    for foreign in [from_a, ahead, zero, JobId::default()] {
        refused(&mut b, Refusal::Stale, |s| s.cancel(foreign));
        refused(&mut b, Refusal::Stale, |s| s.complete(foreign));
        refused(&mut b, Refusal::Stale, |s| s.confirm_cancel(foreign));
        refused(&mut b, Refusal::Stale, |s| s.reap(foreign));
        assert_eq!(b.join(&[from_b, foreign]), Err(Refusal::Stale));
        assert_eq!(b.state(foreign), None);
    }
    assert_eq!(b.state(from_b), Some(State::Queued));
    assert_eq!(a.state(from_a), Some(State::Queued));
}

#[test]
fn two_stages_in_one_scope_alias_which_is_why_scopes_exist() {
    let mut a = stage::<1, 1>();
    let mut b = stage::<1, 1>();
    let from_a = a.submit(0, 1).expect("job");
    assert_eq!(b.submit(0, 2), Ok(from_a));
}

#[test]
fn a_resumed_stage_continues_the_sequence_and_refuses_earlier_ids() {
    let mut stage = scoped_stage::<2>(3);
    let before = stage.submit(0, 1).expect("job");
    let saved = stage.issued();

    let mut resumed = Stage::<u64, 2, 2>::resumed([ONE; 2], 3, saved).expect("stage");
    assert_eq!(
        (resumed.scope(), resumed.issued(), resumed.len()),
        (3, 1, 0)
    );
    refused(&mut resumed, Refusal::Stale, |s| s.cancel(before));
    let after = resumed.submit(0, 1).expect("job");
    assert_eq!(after.raw(), scoped(3, 2));
    refused(&mut resumed, Refusal::Stale, |s| s.cancel(before));

    assert!(Stage::<u64, 2, 2>::resumed([ONE; 2], 3, SEQUENCE_MAX).is_some());
    assert!(Stage::<u64, 2, 2>::resumed([ONE; 2], 3, SEQUENCE_MAX + 1).is_none());
}

#[test]
fn a_fan_out_needs_an_id_for_every_lane_or_takes_none() {
    let mut stage = Stage::<u64, 2, 4>::resumed([ONE; 2], 4, SEQUENCE_MAX - 1).expect("stage");
    let mut out = [JobId::default(); 2];
    // One id left, two lanes: nothing is admitted and the id is not spent.
    refused(&mut stage, Refusal::Exhausted, |s| {
        s.fan_out(&[0, 1], 1, &mut out)
    });
    assert_eq!(out, [JobId::default(); 2]);
    assert_eq!(stage.fan_out(&[1], 1, &mut out), Ok(1));
    assert_eq!(out[0].raw(), scoped(4, SEQUENCE_MAX));
    refused(&mut stage, Refusal::Exhausted, |s| s.submit(0, 1));
    refused(&mut stage, Refusal::Exhausted, |s| {
        s.fan_out(&[0], 1, &mut out)
    });
    // A spent scope still resolves what it holds.
    assert_eq!(stage.cancel(out[0]), Ok(Cancellation::Cancelled));
    assert!(stage.reap(out[0]).is_ok());
}
