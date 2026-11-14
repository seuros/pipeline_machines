//! Ruby symbol names: the snake case of each core variant, every one listed.
use pipeline_machines::{
    Cancellation, Completion, Confirmation, DrainBegin, Join, Refusal, State, Terminal,
};

matryona_machines::symbols! {
    /// The stage's refusals.
    pub Refusals for Refusal as "a refusal" {
        Draining => "draining",
        UnknownLane => "unknown_lane",
        RepeatedLane => "repeated_lane",
        Undersized => "undersized",
        Full => "full",
        Exhausted => "exhausted",
        Stale => "stale",
        NotRunning => "not_running",
        NotCancelling => "not_cancelling",
        NotTerminal => "not_terminal",
        Concluded => "concluded",
    }
}

matryona_machines::symbols! {
    /// Where a job is in its lifecycle.
    pub States for State as "a state" {
        Queued => "queued",
        Running => "running",
        Cancelling => "cancelling",
        Complete => "complete",
        Cancelled => "cancelled",
    }
}

matryona_machines::symbols! {
    /// How a reaped job ended, read back from `Reaped.new`.
    pub Terminals for Terminal as "terminal" {
        Complete => "complete",
        Cancelled => "cancelled",
    }
}

matryona_machines::symbols! {
    /// Where a set of jobs stands as a whole.
    pub Joins for Join as "a join" {
        Ready => "ready",
        Pending => "pending",
        Cancelled => "cancelled",
    }
}

matryona_machines::symbols! {
    /// An honoured cancel.
    pub Cancellations for Cancellation as "a cancellation" {
        Cancelled => "cancelled",
        Cancelling => "cancelling",
        AlreadyCancelling => "already_cancelling",
        AlreadyCancelled => "already_cancelled",
    }
}

matryona_machines::symbols! {
    /// An honoured complete.
    pub Completions for Completion as "a completion" {
        Completed => "completed",
        AlreadyComplete => "already_complete",
    }
}

matryona_machines::symbols! {
    /// An honoured cancel confirmation.
    pub Confirmations for Confirmation as "a confirmation" {
        Confirmed => "confirmed",
        AlreadyCancelled => "already_cancelled",
    }
}

matryona_machines::symbols! {
    /// An honoured begin_drain.
    pub DrainBegins for DrainBegin as "a drain begin" {
        Begun => "begun",
        AlreadyDraining => "already_draining",
    }
}
