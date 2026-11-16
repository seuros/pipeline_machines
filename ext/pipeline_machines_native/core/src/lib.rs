#![no_std]
#![doc = include_str!("../README.md")]
#![cfg_attr(not(test), deny(clippy::indexing_slicing))]

#[cfg(test)]
extern crate std;

mod refusal;
mod types;

use core::num::NonZeroU32;

use periscope_machines::{Issuer, scoped};
use priority_machines::{Deactivation, Lane, Ready, WeightedRoundRobin};

pub use refusal::Refusal;
pub use types::{
    Cancellation, Completion, Confirmation, Dispatch, DrainBegin, JobId, Join, Reaped, State,
    Terminal,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Job<T, const N: usize> {
    /// Scoped id from the stage's issuer; also the admission order.
    key: u64,
    lane: Lane<N>,
    payload: T,
    state: State,
}

/// Bounded stage with caller-driven execution.
///
/// `N` lanes share `C` slots. A slot is held from admission until the job is
/// reaped, so running, cancelling and unreaped terminal jobs all count
/// against capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stage<T, const N: usize, const C: usize> {
    jobs: [Option<Job<T, N>>; C],
    issuer: Issuer,
    scheduler: WeightedRoundRobin<N>,
    draining: bool,
}

impl<T: Copy, const N: usize, const C: usize> Stage<T, N, C> {
    /// An empty stage in scope zero, with no id issued.
    ///
    /// `None` unless `N` is positive and `C` is in `1..=u32::MAX`.
    #[must_use]
    pub fn new(weights: [NonZeroU32; N]) -> Option<Self> {
        Self::with_scope(weights, 0)
    }

    /// An empty stage whose job ids carry `scope`.
    ///
    /// Ids are only meaningful to the stage that issued them. Two stages in
    /// the same scope issue the same ids, so give every stage whose ids could
    /// meet its own scope. `None` unless `N` is positive and `C` is in
    /// `1..=u32::MAX`.
    #[must_use]
    pub fn with_scope(weights: [NonZeroU32; N], scope: u16) -> Option<Self> {
        Self::build(weights, Issuer::new(scope))
    }

    /// An empty stage that continues `scope` after a restart: `issued` is what
    /// [`Stage::issued`] answered when the caller saved it.
    ///
    /// Only the id sequence is restored. Nothing is held: every id issued
    /// before the restart is [`Refusal::Stale`], and submitting again what
    /// was unresolved is the caller's job. The next id carries sequence
    /// `issued + 1`, so none is reissued as long as the saved count is not
    /// lower than what was really issued: save it before handing an id out.
    ///
    /// `None` for the bounds [`Stage::with_scope`] refuses, and when `issued`
    /// is beyond `periscope_machines::SEQUENCE_MAX`, which no stage reaches.
    #[must_use]
    pub fn resumed(weights: [NonZeroU32; N], scope: u16, issued: u64) -> Option<Self> {
        Self::build(weights, Issuer::resumed(scope, issued)?)
    }

    fn build(weights: [NonZeroU32; N], issuer: Issuer) -> Option<Self> {
        if N == 0 {
            return None;
        }
        if C == 0 || u32::try_from(C).is_err() {
            return None;
        }
        Some(Self {
            jobs: [None; C],
            issuer,
            scheduler: WeightedRoundRobin::new(weights),
            draining: false,
        })
    }

    /// The scope carried by every id this stage issues.
    #[must_use]
    pub const fn scope(&self) -> u16 {
        self.issuer.scope()
    }

    /// Ids issued in this scope so far, live or not: the count to save for
    /// [`Stage::resumed`].
    #[must_use]
    pub const fn issued(&self) -> u64 {
        self.issuer.issued()
    }

    /// Compiled slot count `C`.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        C
    }

    /// Compiled lane count `N`.
    #[must_use]
    pub const fn lanes(&self) -> usize {
        N
    }

    /// Jobs held: admitted and not yet reaped, terminal ones included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.jobs.iter().flatten().count()
    }

    /// `true` when no job is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.jobs.iter().all(Option::is_none)
    }

    /// Where a held job is in its lifecycle; `None` for an id that names no
    /// job here, one this stage never issued included.
    #[must_use]
    pub fn state(&self, id: JobId) -> Option<State> {
        self.job(id).ok().map(|job| job.state)
    }

    /// Submit one descriptor to `lane`; it is queued behind the lane's
    /// earlier jobs.
    ///
    /// # Errors
    ///
    /// Checked in this order: [`Refusal::Draining`], [`Refusal::UnknownLane`],
    /// [`Refusal::Full`] when all `C` slots are held, and
    /// [`Refusal::Exhausted`] once all `periscope_machines::SEQUENCE_MAX`
    /// (2^48 - 1) ids of the stage's scope have been issued. A refused
    /// submission consumes no id.
    pub fn submit(&mut self, lane: usize, payload: T) -> Result<JobId, Refusal> {
        // A fan-out to one lane checks the same reasons in the same order;
        // its buffer of one is never undersized.
        let mut out = [JobId::default()];
        self.fan_out(&[lane], payload, &mut out)?;
        let [id] = out;
        Ok(id)
    }

    /// Submit the same descriptor to each of `lanes`, all of them or none.
    ///
    /// Atomic: every reason to refuse any lane is checked before the first
    /// job is admitted, so a refusal leaves the stage and `out` untouched and
    /// there is no partial fan-out. When honoured, the ids are written to the
    /// first `lanes.len()` entries of `out` in lane order, the rest of `out`
    /// is left untouched, and that count is returned. Build the buffer as
    /// `[JobId::default(); K]`. An empty `lanes` is honoured and admits nothing.
    ///
    /// # Errors
    ///
    /// Checked in this order: [`Refusal::Draining`]; [`Refusal::UnknownLane`]
    /// or [`Refusal::RepeatedLane`] for the first lane that is out of range
    /// or was already named; [`Refusal::Undersized`] when `out` is shorter
    /// than `lanes`; [`Refusal::Full`] when fewer than `lanes.len()` slots
    /// are free; [`Refusal::Exhausted`] when the scope has fewer ids left.
    pub fn fan_out(
        &mut self,
        lanes: &[usize],
        payload: T,
        out: &mut [JobId],
    ) -> Result<usize, Refusal> {
        if self.draining {
            return Err(Refusal::Draining);
        }
        for (index, lane) in lanes.iter().enumerate() {
            lane_of::<N>(*lane)?;
            if lanes.iter().take(index).any(|earlier| earlier == lane) {
                return Err(Refusal::RepeatedLane);
            }
        }
        if out.len() < lanes.len() {
            return Err(Refusal::Undersized);
        }
        // A slot is held exactly while it holds a job, so the vacant slots
        // are the free capacity.
        if lanes.len() > self.jobs.iter().filter(|job| job.is_none()).count() {
            return Err(Refusal::Full);
        }
        // Everything that can still refuse runs on a copy; the stage is
        // written only once nothing can.
        let mut issuer = self.issuer;
        for _ in lanes {
            match issuer.issue() {
                Ok(_) => {}
                Err(periscope_machines::Refusal::Exhausted) => return Err(Refusal::Exhausted),
            }
        }
        // An issuer hands out consecutive sequence numbers of its scope. The
        // copy just issued past `base` for every lane, so `base + 1..` stays
        // within the sequence and never overflows.
        let (scope, base) = (self.issuer.scope(), self.issuer.issued());
        // The jobs are admitted into a copy: the lanes are typed again on the
        // way in, and that typing carries its refusal rather than asserting
        // the check above.
        let mut jobs = self.jobs;
        let vacant = jobs.iter_mut().filter(|job| job.is_none());
        for ((job, &lane), sequence) in vacant.zip(lanes).zip(base + 1..) {
            *job = Some(Job::queued(
                scoped(scope, sequence),
                lane_of(lane)?,
                payload,
            ));
        }
        for (place, sequence) in out.iter_mut().zip(base + 1..).take(lanes.len()) {
            *place = JobId(scoped(scope, sequence));
        }
        self.jobs = jobs;
        self.issuer = issuer;
        Ok(lanes.len())
    }

    /// Dispatch the oldest queued job of a weighted-round-robin-selected
    /// eligible lane.
    ///
    /// A drain: `None` is the honoured answer "no eligible lane has a queued
    /// job", returned exactly then. It still records what the round observed
    /// in the scheduler: the lane being visited forfeits its unused turns, and
    /// a second `None` in a row changes nothing. No job changes.
    ///
    /// The scheduler is handed each eligible lane's head and answers the one
    /// it selected, so there is nothing to look up; a head that is the last
    /// of its lane gives up the rest of the lane's burst inside the same
    /// selection.
    pub fn dispatch(&mut self, eligible: &[bool; N]) -> Option<Dispatch<T>> {
        let heads: [Option<Head<T, N>>; N] = core::array::from_fn(|lane| {
            // `lane` runs over `0..N`, the length of `eligible`; a miss
            // would read as ineligible.
            let job = self
                .head(lane)
                .filter(|_| eligible.get(lane).is_some_and(|on| *on))?;
            Some(Head {
                job,
                last: !self.has_queued(lane, Some(job.key)),
            })
        });
        let (lane, Head { job, .. }) = self.scheduler.select(&heads)?;
        self.set_state(job.key, State::Running);
        Some(Dispatch::handed_out(
            JobId(job.key),
            job.payload,
            lane.get(),
        ))
    }

    /// Request cancellation. A queued job is cancelled outright; a running
    /// job keeps its slot and stays unresolved until the executor answers
    /// with [`Stage::confirm_cancel`] or [`Stage::complete`].
    ///
    /// Repeats are honoured and change nothing: the job is still held, so its
    /// state proves the request was already made
    /// ([`Cancellation::AlreadyCancelling`]) or already took effect
    /// ([`Cancellation::AlreadyCancelled`]).
    ///
    /// # Errors
    ///
    /// [`Refusal::Stale`] for an id that names no held job, and
    /// [`Refusal::Concluded`] for a job that already completed.
    pub fn cancel(&mut self, id: JobId) -> Result<Cancellation, Refusal> {
        let job = self.job(id)?;
        match job.state {
            State::Queued => {
                // Its lane empties: it gives up the rest of a burst it is in
                // the middle of. A lane not yet served this visit answers
                // `Idle` and keeps its place.
                if !self.has_queued(job.lane.get(), Some(job.key)) {
                    match self.scheduler.deactivate(job.lane) {
                        Deactivation::Deactivated | Deactivation::Idle => {}
                    }
                }
                self.set_state(job.key, State::Cancelled);
                Ok(Cancellation::Cancelled)
            }
            State::Running => {
                self.set_state(job.key, State::Cancelling);
                Ok(Cancellation::Cancelling)
            }
            State::Cancelling => Ok(Cancellation::AlreadyCancelling),
            State::Cancelled => Ok(Cancellation::AlreadyCancelled),
            State::Complete => Err(Refusal::Concluded),
        }
    }

    /// Record successful execution of a dispatched job, including a
    /// completion that raced a cancellation request.
    ///
    /// Repeating it on a job that is still held answers
    /// [`Completion::AlreadyComplete`] and changes nothing.
    ///
    /// # Errors
    ///
    /// [`Refusal::Stale`] for an id that names no held job (a reaped one
    /// included: the stage keeps nothing about a job once its slot is freed),
    /// [`Refusal::NotRunning`] for a job still queued, and
    /// [`Refusal::Concluded`] for a cancelled job.
    pub fn complete(&mut self, id: JobId) -> Result<Completion, Refusal> {
        let job = self.job(id)?;
        match job.state {
            State::Running | State::Cancelling => {
                self.set_state(job.key, State::Complete);
                Ok(Completion::Completed)
            }
            State::Complete => Ok(Completion::AlreadyComplete),
            State::Queued => Err(Refusal::NotRunning),
            State::Cancelled => Err(Refusal::Concluded),
        }
    }

    /// Confirm that a requested cancellation actually stopped the action.
    ///
    /// On a job that is already cancelled, because it was confirmed before or
    /// was cancelled while queued, it answers
    /// [`Confirmation::AlreadyCancelled`] and changes nothing.
    ///
    /// # Errors
    ///
    /// [`Refusal::Stale`] for an id that names no held job,
    /// [`Refusal::NotCancelling`] for a queued or running job nobody asked to
    /// cancel, and [`Refusal::Concluded`] for a job that completed.
    pub fn confirm_cancel(&mut self, id: JobId) -> Result<Confirmation, Refusal> {
        let job = self.job(id)?;
        match job.state {
            State::Cancelling => {
                self.set_state(job.key, State::Cancelled);
                Ok(Confirmation::Confirmed)
            }
            State::Cancelled => Ok(Confirmation::AlreadyCancelled),
            State::Queued | State::Running => Err(Refusal::NotCancelling),
            State::Complete => Err(Refusal::Concluded),
        }
    }

    /// Remove a terminal job, returning its descriptor and freeing its slot.
    ///
    /// # Errors
    ///
    /// [`Refusal::NotTerminal`] for a job that is queued, running or
    /// cancelling, and [`Refusal::Stale`] for an id that names no held job.
    /// A repeated `reap` is never honoured: the slot is free and the stage
    /// keeps no history, so the repeat is [`Refusal::Stale`].
    pub fn reap(&mut self, id: JobId) -> Result<Reaped<T>, Refusal> {
        let job = self.job(id)?;
        let terminal = match job.state {
            State::Complete => Terminal::Complete,
            State::Cancelled => Terminal::Cancelled,
            State::Queued | State::Running | State::Cancelling => {
                return Err(Refusal::NotTerminal);
            }
        };
        for held in &mut self.jobs {
            if held.is_some_and(|held| held.key == job.key) {
                *held = None;
            }
        }
        Ok(Reaped::new(job.payload, terminal))
    }

    /// Fan-in readiness of `ids`, to be asked before reaping any of them.
    /// A cancelled job outranks an unresolved one. No ids at all are ready.
    ///
    /// # Errors
    ///
    /// [`Refusal::Stale`] when any id names no held job, wherever it sits in
    /// `ids`: readiness is only answered for jobs the stage still holds.
    pub fn join(&self, ids: &[JobId]) -> Result<Join, Refusal> {
        let mut join = Join::Ready;
        for id in ids {
            join = match self.job(*id)?.state {
                State::Cancelled => Join::Cancelled,
                State::Complete => join,
                State::Queued | State::Running | State::Cancelling => match join {
                    Join::Ready | Join::Pending => Join::Pending,
                    Join::Cancelled => Join::Cancelled,
                },
            };
        }
        Ok(join)
    }

    /// Refuse future submissions; queued and running work may finish.
    ///
    /// Never refuses. Repeating it answers [`DrainBegin::AlreadyDraining`].
    pub const fn begin_drain(&mut self) -> DrainBegin {
        if self.draining {
            return DrainBegin::AlreadyDraining;
        }
        self.draining = true;
        DrainBegin::Begun
    }

    /// Whether [`Stage::begin_drain`] was called.
    #[must_use]
    pub const fn is_draining(&self) -> bool {
        self.draining
    }

    /// Draining is complete only after every job is terminal and reaped.
    #[must_use]
    pub fn is_drained(&self) -> bool {
        self.draining && self.is_empty()
    }

    /// A copy of the job `id` names. An id this stage never issued is
    /// refused before any slot is read.
    fn job(&self, id: JobId) -> Result<Job<T, N>, Refusal> {
        if !self.issuer.owns(id.0) {
            return Err(Refusal::Stale);
        }
        self.jobs
            .iter()
            .flatten()
            .find(|job| job.key == id.0)
            .copied()
            .ok_or(Refusal::Stale)
    }

    /// Move the held job keyed `key` to `state`. Keys are unique, so this
    /// writes at most one slot.
    fn set_state(&mut self, key: u64, state: State) {
        for job in self.jobs.iter_mut().flatten() {
            if job.key == key {
                job.state = state;
            }
        }
    }

    /// A copy of the oldest job of `lane` waiting for dispatch.
    fn head(&self, lane: usize) -> Option<Job<T, N>> {
        self.jobs
            .iter()
            .flatten()
            .filter(|job| job.lane.get() == lane && job.state == State::Queued)
            .min_by_key(|job| job.key)
            .copied()
    }

    /// Whether `lane` holds a job waiting for dispatch, other than `except`.
    fn has_queued(&self, lane: usize, except: Option<u64>) -> bool {
        self.jobs.iter().flatten().any(|job| {
            job.lane.get() == lane && job.state == State::Queued && Some(job.key) != except
        })
    }
}

impl<T, const N: usize> Job<T, N> {
    const fn queued(key: u64, lane: Lane<N>, payload: T) -> Self {
        Self {
            key,
            lane,
            payload,
            state: State::Queued,
        }
    }
}

/// A lane's oldest queued job, as the scheduler reads it.
#[derive(Clone, Copy)]
struct Head<T, const N: usize> {
    job: Job<T, N>,
    /// No other job of the lane is queued: dispatching this one empties it.
    last: bool,
}

impl<T: Copy, const N: usize> Ready for Head<T, N> {
    fn last(self) -> bool {
        self.last
    }
}

/// `lane` typed as one of the stage's `N` lanes. The scheduler's
/// `UnknownLane` is the stage's own.
const fn lane_of<const N: usize>(lane: usize) -> Result<Lane<N>, Refusal> {
    match Lane::new(lane) {
        Ok(lane) => Ok(lane),
        Err(priority_machines::Refusal::UnknownLane) => Err(Refusal::UnknownLane),
    }
}

#[cfg(test)]
mod tests;
