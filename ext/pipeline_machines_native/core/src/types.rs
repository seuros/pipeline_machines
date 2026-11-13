use periscope_machines::scope_of;

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
    /// The descriptor that was submitted.
    pub payload: T,
    /// How the job ended.
    pub terminal: Terminal,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dispatch<T> {
    /// Id to resolve after execution.
    pub id: JobId,
    /// Copyable command descriptor.
    pub payload: T,
    /// Originating lane.
    pub lane: usize,
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
