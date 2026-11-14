use periscope_machines::{scope_of, sequence_of};

/// Identity of one job held by a stage, and the token that resolves it.
///
/// Issued by [`Stage::submit`](crate::Stage::submit) and
/// [`Stage::fan_out`](crate::Stage::fan_out). Unique for the lifetime of the
/// stage that issued it: an id is never reissued, so a reaped job's id is
/// refused as [`Refusal::Stale`](crate::Refusal::Stale) forever after. It
/// carries its issuer's scope, so stages built with different scopes never
/// share an id.
///
/// `JobId::default()` is raw zero, which is never issued: a placeholder for
/// filling an output buffer, stale everywhere.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(pub(crate) u64);

impl JobId {
    /// Rebuild an id from [`Self::raw`], e.g. after it crossed an FFI or queue boundary.
    ///
    /// An id the stage never issued, or whose job was reaped, is refused as stale.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The id as a plain integer. Never zero for an issued id.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// The scope of the stage that issued this id, see
    /// [`Stage::with_scope`](crate::Stage::with_scope).
    #[must_use]
    pub const fn scope(self) -> u16 {
        scope_of(self.0)
    }

    /// Whether a stage could have issued this id: its sequence is not zero.
    /// Sequence zero is the placeholder of every scope and is never issued.
    #[must_use]
    pub const fn is_issuable(self) -> bool {
        sequence_of(self.0) != 0
    }
}

/// Job lifecycle; cancellation is not inferred from a timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// How a reaped job ended: the two terminal states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    /// The executor reported completion.
    Complete,
    /// Queued work was cancelled, or the executor confirmed cancellation.
    Cancelled,
}

/// Honoured outcome of [`Stage::reap`](crate::Stage::reap): the descriptor the
/// slot held and how its job ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reaped<T> {
    payload: T,
    terminal: Terminal,
}

impl<T: Copy> Reaped<T> {
    /// The value [`Stage::reap`](crate::Stage::reap) answers. Every payload
    /// with either terminal state is one a stage can answer, so this never
    /// refuses.
    #[must_use]
    pub const fn new(payload: T, terminal: Terminal) -> Self {
        Self { payload, terminal }
    }

    /// The descriptor that was submitted.
    #[must_use]
    pub const fn payload(&self) -> T {
        self.payload
    }

    /// How the job ended.
    #[must_use]
    pub const fn terminal(&self) -> Terminal {
        self.terminal
    }
}

/// Fan-in readiness of a set of live jobs, from [`Stage::join`](crate::Stage::join).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Join {
    /// All jobs completed.
    Ready,
    /// Some jobs remain unresolved and none was cancelled.
    Pending,
    /// At least one job was cancelled.
    Cancelled,
}

/// Descriptor returned for external execution.
///
/// Built only by [`Stage::dispatch`](crate::Stage::dispatch) or the validating
/// [`Dispatch::new`], so it never holds an id no stage issues or a lane past
/// the stage's last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dispatch<T> {
    id: JobId,
    payload: T,
    lane: usize,
}

impl<T: Copy> Dispatch<T> {
    /// A dispatch as a stage of `N` lanes hands one out.
    ///
    /// `None` for what no such stage answers: an id that is not
    /// [issuable](JobId::is_issuable), or a `lane` that is not below `N`.
    /// It does not prove a stage issued `id`; only the stage can.
    #[must_use]
    pub const fn new<const N: usize>(id: JobId, payload: T, lane: usize) -> Option<Self> {
        if !id.is_issuable() || lane >= N {
            return None;
        }
        Some(Self { id, payload, lane })
    }

    /// What [`Stage::dispatch`](crate::Stage::dispatch) hands out: an id the
    /// stage issued and one of its lanes, so there is nothing to check.
    pub(crate) const fn handed_out(id: JobId, payload: T, lane: usize) -> Self {
        Self { id, payload, lane }
    }

    /// Id to resolve after execution.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Copyable command descriptor.
    #[must_use]
    pub const fn payload(&self) -> T {
        self.payload
    }

    /// Originating lane.
    #[must_use]
    pub const fn lane(&self) -> usize {
        self.lane
    }
}

/// Honoured outcome of [`Stage::cancel`](crate::Stage::cancel).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancellation {
    /// The job was queued and is now cancelled; it will not be dispatched.
    Cancelled,
    /// The job is running: the executor must now stop it and
    /// [`confirm_cancel`](crate::Stage::confirm_cancel), or
    /// [`complete`](crate::Stage::complete) it if it finished first.
    Cancelling,
    /// An idempotent repeat: cancellation was already requested and is still
    /// unconfirmed. Nothing changed.
    AlreadyCancelling,
    /// An idempotent repeat: the job is already cancelled. Nothing changed.
    AlreadyCancelled,
}

/// Honoured outcome of [`Stage::complete`](crate::Stage::complete).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    /// The dispatched job is now complete, also when it raced a cancellation.
    Completed,
    /// An idempotent repeat: the job is already complete. Nothing changed.
    AlreadyComplete,
}

/// Honoured outcome of [`Stage::confirm_cancel`](crate::Stage::confirm_cancel).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmation {
    /// The requested cancellation stopped the action; the job is cancelled.
    Confirmed,
    /// An idempotent repeat: the job is already cancelled, because it was
    /// confirmed before or was cancelled while queued. Nothing changed.
    AlreadyCancelled,
}

/// Honoured outcome of [`Stage::begin_drain`](crate::Stage::begin_drain).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainBegin {
    /// The stage is now draining.
    Begun,
    /// An idempotent repeat: the stage was already draining. Nothing changed.
    AlreadyDraining,
}
