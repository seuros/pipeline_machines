//! Ruby symbol names: the snake case of each core variant, every one listed.
use pipeline_machines::{
    Cancellation, Completion, Confirmation, DrainBegin, Join, Refusal, State, Terminal,
};

pub const fn refusal(refusal: Refusal) -> &'static str {
    match refusal {
        Refusal::Draining => "draining",
        Refusal::UnknownLane => "unknown_lane",
        Refusal::RepeatedLane => "repeated_lane",
        Refusal::Undersized => "undersized",
        Refusal::Full => "full",
        Refusal::Exhausted => "exhausted",
        Refusal::Stale => "stale",
        Refusal::NotRunning => "not_running",
        Refusal::NotCancelling => "not_cancelling",
        Refusal::NotTerminal => "not_terminal",
        Refusal::Concluded => "concluded",
    }
}

pub const fn state(state: State) -> &'static str {
    match state {
        State::Queued => "queued",
        State::Running => "running",
        State::Cancelling => "cancelling",
        State::Complete => "complete",
        State::Cancelled => "cancelled",
    }
}

matryona_machines::symbols! {
    /// How a reaped job ended, read back from `Reaped.new`.
    pub Terminals for Terminal as "terminal" {
        Complete => "complete",
        Cancelled => "cancelled",
    }
}

pub const fn join(join: Join) -> &'static str {
    match join {
        Join::Ready => "ready",
        Join::Pending => "pending",
        Join::Cancelled => "cancelled",
    }
}

pub const fn cancellation(outcome: Cancellation) -> &'static str {
    match outcome {
        Cancellation::Cancelled => "cancelled",
        Cancellation::Cancelling => "cancelling",
        Cancellation::AlreadyCancelling => "already_cancelling",
        Cancellation::AlreadyCancelled => "already_cancelled",
    }
}

pub const fn completion(outcome: Completion) -> &'static str {
    match outcome {
        Completion::Completed => "completed",
        Completion::AlreadyComplete => "already_complete",
    }
}

pub const fn confirmation(outcome: Confirmation) -> &'static str {
    match outcome {
        Confirmation::Confirmed => "confirmed",
        Confirmation::AlreadyCancelled => "already_cancelled",
    }
}

pub const fn drain_begin(outcome: DrainBegin) -> &'static str {
    match outcome {
        DrainBegin::Begun => "begun",
        DrainBegin::AlreadyDraining => "already_draining",
    }
}
