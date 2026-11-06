#![no_std]
#![doc = include_str!("../README.md")]
use core::num::NonZeroU32;
use flow_machines::CreditWindow;
use priority_machines::WeightedRoundRobin;

/// Stage-local generation-checked job identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobId {
    slot: usize,
    generation: u64,
}
/// Job lifecycle; cancellation is not inferred from a timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum State {
    /// Awaiting dispatch.
    Queued,
    /// Executor owns an unresolved action.
    Running,
    /// Executor was asked to cancel, but has not confirmed.
    Cancelling,
    /// Executor reported completion.
    Complete,
    /// Queued work was cancelled, or the executor confirmed cancellation.
    Cancelled,
}
/// Fan-in readiness for handles not yet reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Join {
    /// All jobs completed.
    Ready,
    /// Some jobs remain unresolved.
    Pending,
    /// At least one job was cancelled.
    Cancelled,
    /// A handle is stale/unknown/already reaped.
    Unknown,
}
/// Descriptor returned for external execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dispatch<T> {
    /// Handle to resolve after execution.
    pub id: JobId,
    /// Copyable command descriptor.
    pub payload: T,
    /// Originating lane.
    pub lane: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Job<T> {
    generation: u64,
    lane: usize,
    payload: T,
    state: State,
}
/// Bounded stage with caller-driven execution.
#[derive(Debug)]
pub struct Stage<T, const N: usize, const C: usize> {
    jobs: [Option<Job<T>>; C],
    sequence: u64,
    credits: CreditWindow,
    scheduler: WeightedRoundRobin<N>,
    draining: bool,
}
impl<T: Copy, const N: usize, const C: usize> Stage<T, N, C> {
    /// Construct positive lane/capacity bounds; capacity must fit in `u32`.
    #[must_use]
    pub fn new(weights: [NonZeroU32; N]) -> Option<Self> {
        if N == 0 {
            return None;
        }
        let Ok(capacity) = u32::try_from(C) else {
            return None;
        };
        Some(Self {
            jobs: [None; C],
            sequence: 0,
            credits: CreditWindow::new(NonZeroU32::new(capacity)?),
            scheduler: WeightedRoundRobin::new(weights),
            draining: false,
        })
    }
    /// Submit one descriptor, failing without changing state if admission is impossible.
    pub fn submit(&mut self, lane: usize, payload: T) -> Option<JobId> {
        if self.draining || lane >= N {
            return None;
        }
        let slot = self.jobs.iter().position(Option::is_none)?;
        let generation = self.sequence.checked_add(1)?;
        if !self.credits.try_acquire(NonZeroU32::MIN) {
            return None;
        }
        self.sequence = generation;
        self.jobs[slot] = Some(Job {
            generation,
            lane,
            payload,
            state: State::Queued,
        });
        Some(JobId { slot, generation })
    }
    /// Atomically submit the same descriptor to distinct lanes.
    ///
    /// # Panics
    /// Panics only if internal prevalidated admission invariants are inconsistent.
    pub fn fan_out(&mut self, lanes: &[usize], payload: T) -> Option<[Option<JobId>; C]> {
        if self.draining {
            return None;
        }
        let Ok(count) = u32::try_from(lanes.len()) else {
            return None;
        };
        if count > self.credits.available() || self.sequence.checked_add(u64::from(count)).is_none()
        {
            return None;
        }
        for (index, lane) in lanes.iter().enumerate() {
            if *lane >= N || lanes[..index].contains(lane) {
                return None;
            }
        }
        let mut ids = [None; C];
        for (slot, &lane) in lanes.iter().enumerate() {
            ids[slot] = self.submit(lane, payload);
            assert!(ids[slot].is_some());
        }
        Some(ids)
    }
    /// Dispatch FIFO within a WRR-selected eligible lane.
    ///
    /// # Panics
    /// Panics only if internal ready-lane invariants are inconsistent.
    pub fn dispatch(&mut self, eligible: &[bool; N]) -> Option<Dispatch<T>> {
        let ready = core::array::from_fn(|lane| eligible[lane] && self.has_queued(lane));
        let lane = self.scheduler.select(&ready)?;
        let slot = self
            .jobs
            .iter()
            .enumerate()
            .filter_map(|(slot, job)| {
                job.filter(|job| job.lane == lane && job.state == State::Queued)
                    .map(|job| (slot, job.generation))
            })
            .min_by_key(|&(_, generation)| generation)
            .map(|(slot, _)| slot)?;
        let job = self.jobs[slot].as_mut()?;
        job.state = State::Running;
        let dispatch = Dispatch {
            id: JobId {
                slot,
                generation: job.generation,
            },
            payload: job.payload,
            lane,
        };
        self.reset_drained_lane(lane);
        Some(dispatch)
    }
    /// Request cancellation; running jobs retain their credit and unresolved state.
    #[must_use]
    pub fn cancel(&mut self, id: JobId) -> bool {
        let Some(job) = self.job_mut(id) else {
            return false;
        };
        job.state = match job.state {
            State::Queued => State::Cancelled,
            State::Running | State::Cancelling => State::Cancelling,
            _ => return false,
        };
        let lane = job.lane;
        self.reset_drained_lane(lane);
        true
    }
    /// Record successful execution, including a completion racing cancellation.
    #[must_use]
    pub fn complete(&mut self, id: JobId) -> bool {
        let Some(job) = self.job_mut(id) else {
            return false;
        };
        if job.state != State::Running && job.state != State::Cancelling {
            return false;
        }
        job.state = State::Complete;
        true
    }
    /// Confirm that a requested cancellation actually stopped the action.
    #[must_use]
    pub fn confirm_cancel(&mut self, id: JobId) -> bool {
        let Some(job) = self.job_mut(id) else {
            return false;
        };
        if job.state != State::Cancelling {
            return false;
        }
        job.state = State::Cancelled;
        true
    }
    /// Remove a terminal descriptor and return its bounded slot credit.
    ///
    /// # Panics
    /// Panics only if internal credit accounting is inconsistent.
    pub fn reap(&mut self, id: JobId) -> Option<(T, State)> {
        let job = *self.job_mut(id)?;
        if job.state != State::Complete && job.state != State::Cancelled {
            return None;
        }
        self.jobs[id.slot] = None;
        assert!(self.credits.release(NonZeroU32::MIN));
        Some((job.payload, job.state))
    }
    /// Query readiness before reaping any constituent handle.
    #[must_use]
    pub fn join(&self, ids: &[JobId]) -> Join {
        let mut result = Join::Ready;
        for id in ids {
            let Some(Some(job)) = self.jobs.get(id.slot) else {
                return Join::Unknown;
            };
            if job.generation != id.generation {
                return Join::Unknown;
            }
            if job.state == State::Cancelled {
                result = Join::Cancelled;
            } else if job.state != State::Complete && result != Join::Cancelled {
                result = Join::Pending;
            }
        }
        result
    }
    /// Refuse future submissions; existing queued/running work may finish.
    pub const fn begin_drain(&mut self) {
        self.draining = true;
    }
    /// Draining is complete only after all descriptors are terminal and reaped.
    #[must_use]
    pub const fn is_drained(&self) -> bool {
        self.draining && self.credits.in_flight() == 0
    }
    fn job_mut(&mut self, id: JobId) -> Option<&mut Job<T>> {
        self.jobs
            .get_mut(id.slot)?
            .as_mut()
            .filter(|job| job.generation == id.generation)
    }
    /// Whether `lane` still holds a job waiting for dispatch.
    fn has_queued(&self, lane: usize) -> bool {
        self.jobs
            .iter()
            .flatten()
            .any(|job| job.lane == lane && job.state == State::Queued)
    }
    fn reset_drained_lane(&mut self, lane: usize) {
        if !self.has_queued(lane) {
            assert!(self.scheduler.deactivate(lane));
        }
    }
}
#[cfg(test)]
mod tests;
