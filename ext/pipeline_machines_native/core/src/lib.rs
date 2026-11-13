#![no_std]
#![doc = include_str!("../README.md")]

#[cfg(test)]
extern crate std;

mod refusal;
mod types;

use core::num::NonZeroU32;

use flow_machines::CreditWindow;
use periscope_machines::{Issuer, scoped};
use priority_machines::{Deactivation, WeightedRoundRobin};

pub use refusal::Refusal;
pub use types::{
    Cancellation, Completion, Confirmation, Dispatch, DrainBegin, JobId, Join, Reaped, State,
    Terminal,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Job<T> {
    /// Scoped id from the stage's issuer; also the admission order.
    key: u64,
    lane: usize,
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
    jobs: [Option<Job<T>>; C],
    issuer: Issuer,
    credits: CreditWindow,
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
        let capacity = u32::try_from(C).ok()?;
        Some(Self {
            jobs: [None; C],
            issuer,
            credits: CreditWindow::new(NonZeroU32::new(capacity)?),
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
    pub const fn len(&self) -> usize {
        self.credits.in_flight() as usize
    }

    /// `true` when no job is held.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.credits.in_flight() == 0
    }

    /// Where a held job is in its lifecycle; `None` for an id that names no
    /// job here, one this stage never issued included.
    #[must_use]
    pub fn state(&self, id: JobId) -> Option<State> {
        self.job(id).ok().map(|(_, job)| job.state)
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
        if self.draining {
            return Err(Refusal::Draining);
        }
        if lane >= N {
            return Err(Refusal::UnknownLane);
        }
        let Some(slot) = self.jobs.iter().position(Option::is_none) else {
            return Err(Refusal::Full);
        };
        let mut credits = self.credits.clone();
        credits.try_acquire(NonZeroU32::MIN).map_err(credit)?;
        // Last check: an id is only spent on a submission that is honoured.
        let key = match self.issuer.issue() {
            Ok(key) => key,
            Err(periscope_machines::Refusal::Exhausted) => return Err(Refusal::Exhausted),
        };
        self.credits = credits;
        self.jobs[slot] = Some(Job::queued(key, lane, payload));
        Ok(JobId(key))
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
            if *lane >= N {
                return Err(Refusal::UnknownLane);
            }
            if lanes[..index].contains(lane) {
                return Err(Refusal::RepeatedLane);
            }
        }
        if out.len() < lanes.len() {
            return Err(Refusal::Undersized);
        }
        let free = self.jobs.iter().filter(|job| job.is_none()).count();
        let Some(count) = u32::try_from(lanes.len())
            .ok()
            .filter(|_| lanes.len() <= free)
        else {
            return Err(Refusal::Full);
        };
        let Some(units) = NonZeroU32::new(count) else {
            return Ok(0);
        };
        // Everything that can still refuse runs on copies; the stage is
        // written only once nothing can.
        let mut credits = self.credits.clone();
        credits.try_acquire(units).map_err(credit)?;
        let mut issuer = self.issuer;
        for _ in lanes {
            match issuer.issue() {
                Ok(_) => {}
                Err(periscope_machines::Refusal::Exhausted) => return Err(Refusal::Exhausted),
            }
        }
        // An issuer hands out consecutive sequence numbers of its scope.
        let (scope, base) = (self.issuer.scope(), self.issuer.issued());
        self.credits = credits;
        self.issuer = issuer;
        let vacant = self.jobs.iter_mut().filter(|job| job.is_none());
        for (((job, &lane), place), sequence) in vacant.zip(lanes).zip(out).zip(base + 1..) {
            let key = scoped(scope, sequence);
            *job = Some(Job::queued(key, lane, payload));
            *place = JobId(key);
        }
        Ok(lanes.len())
    }

    /// Dispatch the oldest queued job of a weighted-round-robin-selected
    /// eligible lane.
    ///
    /// A drain: `None` is the honoured answer "no eligible lane has a queued
    /// job", returned exactly then. It still records what the round observed
    /// in the scheduler: the lane being visited forfeits its unused turns, and
    /// a second `None` in a row changes nothing. No job changes.
    pub fn dispatch(&mut self, eligible: &[bool; N]) -> Option<Dispatch<T>> {
        let ready = core::array::from_fn(|lane| eligible[lane] && self.has_queued(lane, None));
        let lane = self.scheduler.select(&ready)?;
        let job = self
            .jobs
            .iter_mut()
            .flatten()
            .filter(|job| job.lane == lane && job.state == State::Queued)
            .min_by_key(|job| job.key)?;
        job.state = State::Running;
        let dispatch = Dispatch {
            id: JobId(job.key),
            payload: job.payload,
            lane,
        };
        if !self.has_queued(lane, None) {
            // The lane emptied: it gives up the rest of its burst. `select`
            // only names one of the `N` lanes, so the scheduler has nothing
            // to refuse here; the pattern lists every answer so that a new
            // one is a compile error.
            let (Ok(Deactivation::Deactivated | Deactivation::Idle)
            | Err(priority_machines::Refusal::UnknownLane)) = self.scheduler.deactivate(lane);
        }
        Some(dispatch)
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
    /// [`Refusal::UnknownLane`] is the scheduler's refusal passed on; a held
    /// job always sits in one of the `N` lanes, so it is not reachable.
    pub fn cancel(&mut self, id: JobId) -> Result<Cancellation, Refusal> {
        let (slot, job) = self.job(id)?;
        match job.state {
            State::Queued => {
                if !self.has_queued(job.lane, Some(job.key)) {
                    // Its lane empties: it gives up the rest of a burst it
                    // is in the middle of. A lane not yet served this visit
                    // answers `Idle` and keeps its place.
                    // Asked first, so a refusal leaves the job queued.
                    let (Deactivation::Deactivated | Deactivation::Idle) =
                        self.scheduler.deactivate(job.lane).map_err(lane)?;
                }
                self.set(slot, State::Cancelled);
                Ok(Cancellation::Cancelled)
            }
            State::Running => {
                self.set(slot, State::Cancelling);
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
        let (slot, job) = self.job(id)?;
        match job.state {
            State::Running | State::Cancelling => {
                self.set(slot, State::Complete);
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
        let (slot, job) = self.job(id)?;
        match job.state {
            State::Cancelling => {
                self.set(slot, State::Cancelled);
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
        let (slot, job) = self.job(id)?;
        let terminal = match job.state {
            State::Complete => Terminal::Complete,
            State::Cancelled => Terminal::Cancelled,
            State::Queued | State::Running | State::Cancelling => {
                return Err(Refusal::NotTerminal);
            }
        };
        let mut credits = self.credits.clone();
        credits.release(NonZeroU32::MIN).map_err(credit)?;
        self.credits = credits;
        self.jobs[slot] = None;
        Ok(Reaped {
            payload: job.payload,
            terminal,
        })
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
            join = match self.job(*id)?.1.state {
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
    pub const fn is_drained(&self) -> bool {
        self.draining && self.credits.in_flight() == 0
    }

    /// The slot and a copy of the job `id` names. An id this stage never
    /// issued is refused before any slot is read.
    fn job(&self, id: JobId) -> Result<(usize, Job<T>), Refusal> {
        if !self.issuer.owns(id.0) {
            return Err(Refusal::Stale);
        }
        self.jobs
            .iter()
            .enumerate()
            .find_map(|(slot, job)| job.filter(|job| job.key == id.0).map(|job| (slot, job)))
            .ok_or(Refusal::Stale)
    }

    const fn set(&mut self, slot: usize, state: State) {
        if let Some(job) = self.jobs[slot].as_mut() {
            job.state = state;
        }
    }

    /// Whether `lane` holds a job waiting for dispatch, other than `except`.
    fn has_queued(&self, lane: usize, except: Option<u64>) -> bool {
        self.jobs
            .iter()
            .flatten()
            .any(|job| job.lane == lane && job.state == State::Queued && Some(job.key) != except)
    }
}

impl<T> Job<T> {
    const fn queued(key: u64, lane: usize, payload: T) -> Self {
        Self {
            key,
            lane,
            payload,
            state: State::Queued,
        }
    }
}

/// What a refusal of the slot-credit window means to the stage.
///
/// The stage keeps exactly one credit per held job, so the window agrees
/// with the slots and both calls are checked against the slots first. The
/// answers are mapped rather than asserted: a stage that ever disagreed with
/// its window refuses and stays as it was.
const fn credit(refusal: flow_machines::Refusal) -> Refusal {
    match refusal {
        // Fewer credits than asked for: not enough free slots.
        flow_machines::Refusal::Insufficient { .. } => Refusal::Full,
        // No credit out: there is no held job to release.
        flow_machines::Refusal::ExceedsInFlight { .. } => Refusal::Stale,
    }
}

/// The scheduler's refusal, which is the stage's own under the same name.
const fn lane(refusal: priority_machines::Refusal) -> Refusal {
    match refusal {
        priority_machines::Refusal::UnknownLane => Refusal::UnknownLane,
    }
}

#[cfg(test)]
mod tests;
