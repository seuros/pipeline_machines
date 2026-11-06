#![allow(clippy::expect_used)]
use super::*;
#[test]
fn capacity_includes_running_and_unreaped_terminal_jobs() {
    let mut stage = Stage::<u64, 1, 1>::new([NonZeroU32::MIN]).expect("stage");
    let first = stage.submit(0, 7).expect("job");
    assert_eq!(stage.dispatch(&[false]), None);
    assert_eq!(stage.dispatch(&[true]).expect("dispatch").id, first);
    assert_eq!(stage.submit(0, 8), None);
    assert!(!stage.confirm_cancel(first));
    assert!(stage.complete(first));
    assert_eq!(stage.submit(0, 8), None);
    assert_eq!(stage.reap(first), Some((7, State::Complete)));
    let next = stage.submit(0, 8).expect("replacement");
    assert_ne!(first, next);
    assert!(!stage.complete(first));
}
#[test]
fn fanout_is_atomic_and_join_waits_for_all_handles() {
    let mut stage = Stage::<u64, 2, 2>::new([NonZeroU32::MIN; 2]).expect("stage");
    assert_eq!(stage.fan_out(&[0, 0], 7), None);
    assert_eq!(stage.fan_out(&[0, 2], 7), None);
    let ids = stage
        .fan_out(&[0, 1], 7)
        .expect("fanout")
        .map(|id| id.expect("id"));
    assert_eq!(stage.join(&ids), Join::Pending);
    for expected in ids {
        let dispatch = stage.dispatch(&[true; 2]).expect("work");
        assert_eq!(dispatch.id, expected);
        assert!(stage.complete(dispatch.id));
    }
    assert_eq!(stage.join(&ids), Join::Ready);
    assert!(stage.reap(ids[0]).is_some());
    assert_eq!(stage.join(&ids), Join::Unknown);
}
#[test]
fn running_cancellation_is_not_silently_freed_and_draining_waits_for_reap() {
    let mut stage = Stage::<u64, 1, 2>::new([NonZeroU32::MIN]).expect("stage");
    let running = stage.submit(0, 1).expect("job");
    let queued = stage.submit(0, 2).expect("job");
    assert!(stage.dispatch(&[true]).is_some());
    assert!(stage.cancel(running));
    assert_eq!(stage.reap(running), None);
    assert!(stage.cancel(queued));
    assert_eq!(stage.join(&[running]), Join::Pending);
    assert_eq!(stage.join(&[queued]), Join::Cancelled);
    stage.begin_drain();
    assert_eq!(stage.submit(0, 3), None);
    assert!(!stage.is_drained());
    assert!(stage.confirm_cancel(running));
    assert_eq!(stage.reap(running), Some((1, State::Cancelled)));
    assert_eq!(stage.reap(queued), Some((2, State::Cancelled)));
    assert!(stage.is_drained());
}
#[test]
fn completion_can_win_a_cancellation_race_and_zero_bounds_reject() {
    assert!(Stage::<u8, 0, 1>::new([]).is_none());
    assert!(Stage::<u8, 1, 0>::new([NonZeroU32::MIN]).is_none());
    let mut stage = Stage::<u8, 1, 1>::new([NonZeroU32::MIN]).expect("stage");
    let id = stage.submit(0, 1).expect("job");
    assert!(stage.dispatch(&[true]).is_some());
    assert!(stage.cancel(id));
    assert!(stage.complete(id));
    assert!(!stage.confirm_cancel(id));
    assert_eq!(stage.reap(id), Some((1, State::Complete)));
}

#[test]
fn mixed_lifecycle_operations_preserve_capacity_and_handle_invariants() {
    let mut stage = Stage::<u64, 2, 4>::new([NonZeroU32::MIN; 2]).expect("stage");
    for step in 0..2_000 {
        match step % 7 {
            0 | 1 => {
                let submitted = stage.submit(step % 2, step as u64);
                assert!(submitted.is_some() || stage.credits.available() == 0);
            }
            2 => {
                let dispatched = stage.dispatch(&[true; 2]);
                assert!(dispatched.is_none_or(|work| work.lane < 2));
            }
            operation => {
                let candidate = stage.jobs.iter().enumerate().find_map(|(slot, job)| {
                    job.map(|job| JobId {
                        slot,
                        generation: job.generation,
                    })
                });
                if let Some(id) = candidate {
                    let accepted = match operation {
                        3 => stage.cancel(id),
                        4 => stage.complete(id),
                        5 => stage.confirm_cancel(id),
                        _ => stage.reap(id).is_some(),
                    };
                    // A rejected transition must still leave a well-formed bounded stage.
                    assert!(accepted || stage.join(&[id]) != Join::Unknown);
                }
            }
        }
        assert_eq!(
            stage.jobs.iter().flatten().count(),
            stage.credits.in_flight() as usize
        );
        assert_eq!(stage.credits.available() + stage.credits.in_flight(), 4);
    }
}
