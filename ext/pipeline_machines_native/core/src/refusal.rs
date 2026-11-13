use core::fmt;

/// Why a call was not honoured. A refused call changes nothing: not a job,
/// not a slot credit, not an issued id, not the scheduler, not the caller's
/// output buffer.
///
/// This is the crate's one refusal type; every method lists the variants it
/// answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The stage is draining and admits nothing.
    Draining,
    /// The lane index is not one of the stage's `N` lanes.
    UnknownLane,
    /// A fan-out names the same lane twice.
    RepeatedLane,
    /// The output buffer is shorter than the fan-out it must hold.
    Undersized,
    /// Not enough free slots: every one the call needs is held by a job that
    /// has not been reaped, terminal ones included.
    Full,
    /// The stage's scope has too few job ids left for the call; none is reissued.
    Exhausted,
    /// The id names no job held by this stage: already reaped, issued by a
    /// stage of another scope, or never issued. The last two are answered
    /// before any slot is looked at.
    Stale,
    /// The job is still queued: it was never dispatched, so it cannot complete.
    NotRunning,
    /// No cancellation of this job is awaiting confirmation: it is queued or
    /// running and `cancel` was not called.
    NotCancelling,
    /// The job is not terminal, so it cannot be reaped.
    NotTerminal,
    /// The job already ended the other way: a complete job cannot be
    /// cancelled, a cancelled job cannot complete.
    Concluded,
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Draining => "the stage is draining",
            Self::UnknownLane => "lane index is not one of the stage's lanes",
            Self::RepeatedLane => "the fan-out names a lane twice",
            Self::Undersized => "the output buffer is shorter than the fan-out",
            Self::Full => "not enough free slots",
            Self::Exhausted => "not enough job ids left in this scope",
            Self::Stale => "the id names no job held by this stage",
            Self::NotRunning => "the job was never dispatched",
            Self::NotCancelling => "no cancellation of the job awaits confirmation",
            Self::NotTerminal => "the job is not complete or cancelled",
            Self::Concluded => "the job already ended the other way",
        })
    }
}

impl core::error::Error for Refusal {}
