//! Randomized runs against an independent naive model.

use std::vec::Vec;

use matryona_machines::testing::SplitMix64;

use super::*;

/// A draw in `0..len`.
fn pick(rng: &mut SplitMix64, len: usize) -> usize {
    usize::try_from(rng.below(len as u64)).unwrap_or(0)
}

const LANES: usize = 3;
const SLOTS: usize = 5;
const HOLE: JobId = JobId::from_raw(u64::MAX);

#[derive(Clone, Copy)]
struct Row {
    id: u64,
    lane: usize,
    payload: u64,
    state: State,
}

/// Independent reference: one growable list in admission order, no slots, no
/// credit window, no issuer. Lane choice is the sibling scheduler's, which is
/// not what is under test here.
struct Model {
    rows: Vec<Row>,
    scope: u16,
    issued: u64,
    last: u64,
    draining: bool,
    turns: WeightedRoundRobin<LANES>,
}

impl Model {
    /// The id of a sequence number, spelled out here rather than borrowed
    /// from the crate under test: scope in the top 16 bits, sequence below.
    fn id(&self, sequence: u64) -> u64 {
        (u64::from(self.scope) << 48) | sequence
    }

    fn queued(&self, lane: usize) -> bool {
        self.rows
            .iter()
            .any(|r| r.lane == lane && r.state == State::Queued)
    }

    fn settle(&mut self, lane: usize) {
        if !self.queued(lane) {
            assert!(self.turns.deactivate(lane).is_ok());
        }
    }

    fn fan_out(&mut self, lanes: &[usize], payload: u64, room: usize) -> Result<Vec<u64>, Refusal> {
        if self.draining {
            return Err(Refusal::Draining);
        }
        for (index, lane) in lanes.iter().enumerate() {
            if *lane >= LANES {
                return Err(Refusal::UnknownLane);
            }
            if lanes[..index].contains(lane) {
                return Err(Refusal::RepeatedLane);
            }
        }
        if room < lanes.len() {
            return Err(Refusal::Undersized);
        }
        if self.rows.len() + lanes.len() > SLOTS {
            return Err(Refusal::Full);
        }
        if self.last - self.issued < lanes.len() as u64 {
            return Err(Refusal::Exhausted);
        }
        let mut ids = Vec::new();
        for &lane in lanes {
            self.issued += 1;
            let id = self.id(self.issued);
            self.rows.push(Row {
                id,
                lane,
                payload,
                state: State::Queued,
            });
            ids.push(id);
        }
        Ok(ids)
    }

    fn submit(&mut self, lane: usize, payload: u64) -> Result<u64, Refusal> {
        if self.draining {
            Err(Refusal::Draining)
        } else if lane >= LANES {
            Err(Refusal::UnknownLane)
        } else {
            self.fan_out(&[lane], payload, 1).map(|ids| ids[0])
        }
    }

    fn dispatch(&mut self, eligible: [bool; LANES]) -> Option<(u64, u64, usize)> {
        let ready = core::array::from_fn(|lane| eligible[lane] && self.queued(lane));
        let lane = self.turns.select(&ready)?;
        let row = self
            .rows
            .iter_mut()
            .find(|r| r.lane == lane && r.state == State::Queued)?;
        row.state = State::Running;
        let answer = (row.id, row.payload, lane);
        self.settle(lane);
        Some(answer)
    }

    fn at(&self, id: u64) -> Result<usize, Refusal> {
        self.rows
            .iter()
            .position(|r| r.id == id)
            .ok_or(Refusal::Stale)
    }

    fn cancel(&mut self, id: u64) -> Result<Cancellation, Refusal> {
        let at = self.at(id)?;
        match self.rows[at].state {
            State::Queued => {
                self.rows[at].state = State::Cancelled;
                self.settle(self.rows[at].lane);
                Ok(Cancellation::Cancelled)
            }
            State::Running => {
                self.rows[at].state = State::Cancelling;
                Ok(Cancellation::Cancelling)
            }
            State::Cancelling => Ok(Cancellation::AlreadyCancelling),
            State::Cancelled => Ok(Cancellation::AlreadyCancelled),
            State::Complete => Err(Refusal::Concluded),
        }
    }

    fn complete(&mut self, id: u64) -> Result<Completion, Refusal> {
        let at = self.at(id)?;
        match self.rows[at].state {
            State::Running | State::Cancelling => {
                self.rows[at].state = State::Complete;
                Ok(Completion::Completed)
            }
            State::Complete => Ok(Completion::AlreadyComplete),
            State::Queued => Err(Refusal::NotRunning),
            State::Cancelled => Err(Refusal::Concluded),
        }
    }

    fn confirm_cancel(&mut self, id: u64) -> Result<Confirmation, Refusal> {
        let at = self.at(id)?;
        match self.rows[at].state {
            State::Cancelling => {
                self.rows[at].state = State::Cancelled;
                Ok(Confirmation::Confirmed)
            }
            State::Cancelled => Ok(Confirmation::AlreadyCancelled),
            State::Queued | State::Running => Err(Refusal::NotCancelling),
            State::Complete => Err(Refusal::Concluded),
        }
    }

    fn reap(&mut self, id: u64) -> Result<Reaped<u64>, Refusal> {
        let at = self.at(id)?;
        let terminal = match self.rows[at].state {
            State::Complete => Terminal::Complete,
            State::Cancelled => Terminal::Cancelled,
            State::Queued | State::Running | State::Cancelling => {
                return Err(Refusal::NotTerminal);
            }
        };
        let row = self.rows.remove(at);
        Ok(Reaped::new(row.payload, terminal))
    }

    fn join(&self, ids: &[u64]) -> Result<Join, Refusal> {
        let mut states = Vec::new();
        for id in ids {
            states.push(self.rows[self.at(*id)?].state);
        }
        Ok(if states.contains(&State::Cancelled) {
            Join::Cancelled
        } else if states.iter().all(|s| *s == State::Complete) {
            Join::Ready
        } else {
            Join::Pending
        })
    }
}

/// Whether an honoured answer is a repeat that must have changed nothing.
trait Repeat {
    fn is_repeat(&self) -> bool;
}

impl Repeat for Cancellation {
    fn is_repeat(&self) -> bool {
        matches!(self, Self::AlreadyCancelling | Self::AlreadyCancelled)
    }
}

impl Repeat for Completion {
    fn is_repeat(&self) -> bool {
        *self == Self::AlreadyComplete
    }
}

impl Repeat for Confirmation {
    fn is_repeat(&self) -> bool {
        *self == Self::AlreadyCancelled
    }
}

fn unchanged<R: Repeat>(answer: &Result<R, Refusal>) -> bool {
    answer.as_ref().map_or(true, Repeat::is_repeat)
}

struct Run {
    rng: SplitMix64,
    stage: Stage<u64, LANES, SLOTS>,
    model: Model,
    /// Every id ever issued, plus a few no stage issued.
    ids: Vec<u64>,
}

impl Run {
    fn new(seed: u64, scope: u16, start: u64) -> Self {
        let weights = [3, 1, 2].map(|w| NonZeroU32::new(w).expect("weight"));
        let model = Model {
            rows: Vec::new(),
            scope,
            issued: start,
            last: periscope_machines::SEQUENCE_MAX,
            draining: false,
            turns: WeightedRoundRobin::new(weights),
        };
        let ids = Vec::from([0, model.id(start + 1), (u64::from(scope ^ 1) << 48) | 1]);
        Self {
            rng: SplitMix64::new(seed),
            stage: Stage::resumed(weights, scope, start).expect("stage"),
            model,
            ids,
        }
    }

    fn id(&mut self) -> u64 {
        self.ids[pick(&mut self.rng, self.ids.len())]
    }

    fn admit(&mut self) -> bool {
        let payload = self.rng.next_u64();
        if self.rng.below(2) == 0 {
            let lane = pick(&mut self.rng, LANES + 1);
            let expected = self.model.submit(lane, payload);
            assert_eq!(self.stage.submit(lane, payload).map(JobId::raw), expected);
            self.ids.extend(expected.iter());
            return expected.is_err();
        }
        let lanes: Vec<usize> = (0..pick(&mut self.rng, LANES + 1))
            .map(|_| pick(&mut self.rng, LANES + 1))
            .collect();
        let room = lanes.len() + 2 - pick(&mut self.rng, 4).min(lanes.len() + 2);
        let mut out = [HOLE; LANES + 3];
        let expected = self.model.fan_out(&lanes, payload, room);
        let actual = self.stage.fan_out(&lanes, payload, &mut out[..room]);
        assert_eq!(actual, expected.as_ref().map(Vec::len).map_err(|r| *r));
        let written = expected.as_ref().map_or(0, Vec::len);
        let raw = out.map(JobId::raw);
        assert_eq!(raw[..written], *expected.as_deref().unwrap_or(&[]));
        assert!(
            raw[written..].iter().all(|id| *id == HOLE.raw()),
            "out was written past the count"
        );
        self.ids.extend(expected.iter().flatten());
        expected.is_err()
    }

    /// One call on both; `true` when it must have left the stage as it was.
    fn step(&mut self) -> bool {
        match self.rng.below(16) {
            0..=3 => self.admit(),
            4..=6 => {
                let eligible = core::array::from_fn(|_| self.rng.below(4) != 0);
                let expected = self.model.dispatch(eligible);
                let actual = self.stage.dispatch(&eligible);
                assert_eq!(
                    actual.map(|d| (d.id().raw(), d.payload(), d.lane())),
                    expected
                );
                false
            }
            7 | 8 => {
                let id = self.id();
                let expected = self.model.cancel(id);
                assert_eq!(self.stage.cancel(JobId::from_raw(id)), expected);
                unchanged(&expected)
            }
            9 | 10 => {
                let id = self.id();
                let expected = self.model.complete(id);
                assert_eq!(self.stage.complete(JobId::from_raw(id)), expected);
                unchanged(&expected)
            }
            11 | 12 => {
                let id = self.id();
                let expected = self.model.confirm_cancel(id);
                assert_eq!(self.stage.confirm_cancel(JobId::from_raw(id)), expected);
                unchanged(&expected)
            }
            13 | 14 => {
                let id = self.id();
                let expected = self.model.reap(id);
                assert_eq!(self.stage.reap(JobId::from_raw(id)), expected);
                expected.is_err()
            }
            _ => {
                if self.rng.below(10) != 0 {
                    return true;
                }
                let expected = if self.model.draining {
                    DrainBegin::AlreadyDraining
                } else {
                    DrainBegin::Begun
                };
                self.model.draining = true;
                assert_eq!(self.stage.begin_drain(), expected);
                expected == DrainBegin::AlreadyDraining
            }
        }
    }

    fn check(&mut self) {
        let (stage, model) = (&self.stage, &self.model);
        assert_eq!(stage.len(), model.rows.len());
        assert_eq!(stage.issued(), model.issued);
        assert_eq!(stage.scheduler, model.turns);
        assert_eq!(stage.is_drained(), model.draining && model.rows.is_empty());
        for id in &self.ids {
            let held = model.rows.iter().find(|r| r.id == *id).map(|r| r.state);
            assert_eq!(stage.state(JobId::from_raw(*id)), held);
        }
        let some: Vec<u64> = (0..pick(&mut self.rng, 4)).map(|_| self.id()).collect();
        let asked: Vec<JobId> = some.iter().copied().map(JobId::from_raw).collect();
        assert_eq!(self.stage.join(&asked), self.model.join(&some));
    }
}

fn run(seed: u64, scope: u16, start: u64, steps: usize) {
    let mut run = Run::new(seed, scope, start);
    for _ in 0..steps {
        let before = run.stage.clone();
        if run.step() {
            assert_eq!(
                run.stage, before,
                "a refusal or an honoured repeat changed the stage"
            );
        }
        let before = run.stage.clone();
        run.check();
        assert_eq!(run.stage, before, "a query changed the stage");
    }
}

#[test]
fn random_runs_agree_with_the_model() {
    for seed in 0..300 {
        run(seed, (seed % 5) as u16, 0, 400);
    }
}

#[test]
fn random_runs_agree_with_the_model_at_the_end_of_the_sequence() {
    for seed in 0..100 {
        let left = seed % 9;
        run(seed, 3, periscope_machines::SEQUENCE_MAX - left, 300);
    }
}
