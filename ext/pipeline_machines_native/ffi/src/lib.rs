//! Native boundary: all lifecycle decisions are delegated to the stage core.
mod classes;
mod init;
mod names;

use std::{cell::RefCell, num::NonZeroU32};

use classes::data;
use magnus::{
    Error, Integer, RArray, Ruby, Symbol, TryConvert, Value,
    prelude::*,
    typed_data::Obj,
    value::{Qfalse, Qtrue},
};
use pipeline_machines::{Dispatch, JobId, Reaped, Refusal, Terminal};

const LANES: usize = 4;
const CAPACITY: usize = 64;

type Core = pipeline_machines::Stage<u64, LANES, CAPACITY>;

#[derive(Clone, Copy)]
struct Number(u64);

impl Number {
    /// A lane index. One beyond the address space names no lane, like any
    /// other index past the stage's last: the core refuses it.
    fn lane(self) -> usize {
        usize::try_from(self.0).unwrap_or(usize::MAX)
    }

    const fn id(self) -> JobId {
        JobId::from_raw(self.0)
    }
}

/// `TypeError` unless `value` is an Integer; the range is checked by the
/// conversion that follows (`RangeError`).
fn integer(value: Value, expected: &'static str) -> Result<Value, Error> {
    if Integer::from_value(value).is_none() {
        return Err(Error::new(
            Ruby::get_with(value).exception_type_error(),
            expected,
        ));
    }
    Ok(value)
}

impl TryConvert for Number {
    fn try_convert(value: Value) -> Result<Self, Error> {
        u64::try_convert(integer(value, "expected an Integer (unsigned 64-bit)")?).map(Self)
    }
}

/// An issuer scope: an Integer in `0..=65535`.
#[derive(Clone, Copy)]
struct Scope(u16);

impl TryConvert for Scope {
    fn try_convert(value: Value) -> Result<Self, Error> {
        u16::try_convert(integer(value, "expected an Integer scope (0..=65535)")?).map(Self)
    }
}

/// Lane weights: an Array of exactly `LANES` Integers in `1..=u32::MAX`.
fn weights(ruby: &Ruby, weights: RArray) -> Result<[NonZeroU32; LANES], Error> {
    let values = weights.to_value_array::<LANES>()?;
    let mut converted = [NonZeroU32::MIN; LANES];
    for (slot, value) in converted.iter_mut().zip(values) {
        let number = Number::try_convert(value)?.0;
        *slot = u32::try_from(number)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| {
                Error::new(
                    ruby.exception_range_error(),
                    "weights must be in 1..2**32-1",
                )
            })?;
    }
    Ok(converted)
}

/// Lane eligibility: an Array of exactly `LANES` values, each `true` or `false`.
fn eligibility(ruby: &Ruby, eligible: RArray) -> Result<[bool; LANES], Error> {
    let values = eligible.to_value_array::<LANES>()?;
    let mut converted = [false; LANES];
    for (slot, value) in converted.iter_mut().zip(values) {
        *slot = if Qtrue::from_value(value).is_some() {
            true
        } else if Qfalse::from_value(value).is_some() {
            false
        } else {
            return Err(Error::new(
                ruby.exception_type_error(),
                "eligible must hold true or false for every lane",
            ));
        };
    }
    Ok(converted)
}

/// An Array of Integers, converted before the core is touched.
fn numbers(array: RArray) -> Result<Vec<Number>, Error> {
    (0..array.len())
        .map(|index| Number::try_convert(array.entry(index.cast_signed())?))
        .collect()
}

fn refused(ruby: &Ruby, refusal: Refusal) -> Value {
    ruby.to_symbol(names::refusal(refusal)).as_value()
}

/// An honoured unit outcome or a refusal, as the symbol it maps to.
fn answer<T>(ruby: &Ruby, outcome: Result<T, Refusal>, name: fn(T) -> &'static str) -> Value {
    ruby.to_symbol(outcome.map_or_else(names::refusal, name))
        .as_value()
}

/// What `Dispatch.new` stores, as `Dispatch::new` for a stage of `LANES`
/// lanes accepts it: `TypeError` for a non-Integer, `RangeError` for an id no
/// stage issues or a lane past the last.
fn checked_dispatch(
    ruby: &Ruby,
    id: Number,
    payload: Number,
    lane: Number,
) -> Result<(u64, u64, usize), Error> {
    Dispatch::new::<LANES>(id.id(), payload.0, lane.lane())
        .map(|work| (work.id().raw(), work.payload(), work.lane()))
        .ok_or_else(|| {
            Error::new(
                ruby.exception_range_error(),
                "a stage dispatches only an issued id (sequence above zero) on lanes 0...4",
            )
        })
}

/// What `Reaped.new` stores: `TypeError` for a non-Integer payload or a
/// non-Symbol terminal, `RangeError` for a symbol that names no `Terminal`.
fn checked_reaped(ruby: &Ruby, payload: Number, terminal: Value) -> Result<(u64, Symbol), Error> {
    let symbol = Symbol::from_value(terminal)
        .ok_or_else(|| Error::new(ruby.exception_type_error(), "terminal must be a Symbol"))?;
    let terminal = match symbol.name()?.as_ref() {
        "complete" => Terminal::Complete,
        "cancelled" => Terminal::Cancelled,
        _ => {
            return Err(Error::new(
                ruby.exception_range_error(),
                "terminal must be :complete or :cancelled",
            ));
        }
    };
    let reaped = Reaped::new(payload.0, terminal);
    Ok((
        reaped.payload(),
        ruby.to_symbol(names::terminal(reaped.terminal())),
    ))
}

/// `PipelineMachines.scope_of(id)`: the scope an id was issued under.
const fn scope_of_id(id: Number) -> u16 {
    id.id().scope()
}

#[magnus::wrap(class = "PipelineMachines::Stage", free_immediately, size)]
struct Stage {
    core: RefCell<Core>,
}

impl Stage {
    fn build(ruby: &Ruby, core: Option<Core>, why: &'static str) -> Result<Self, Error> {
        core.map(|core| Self {
            core: RefCell::new(core),
        })
        .ok_or_else(|| Error::new(ruby.exception_range_error(), why))
    }

    fn new(ruby: &Ruby, lanes: RArray) -> Result<Self, Error> {
        let core = Core::new(weights(ruby, lanes)?);
        Self::build(ruby, core, "invalid stage bounds")
    }

    fn with_scope(ruby: &Ruby, lanes: RArray, scope: Scope) -> Result<Self, Error> {
        let core = Core::with_scope(weights(ruby, lanes)?, scope.0);
        Self::build(ruby, core, "invalid stage bounds")
    }

    fn resumed(ruby: &Ruby, lanes: RArray, scope: Scope, issued: Number) -> Result<Self, Error> {
        let core = Core::resumed(weights(ruby, lanes)?, scope.0, issued.0);
        Self::build(
            ruby,
            core,
            "issued is past the last sequence of a scope (2**48 - 1)",
        )
    }

    /// Run `act` on a copy of the stage and build its answer with `build`;
    /// the copy replaces the stage only once the answer exists. A `build`
    /// that raises loses nothing; one that froze the stage meanwhile raises
    /// `FrozenError`, one that changed it (a redefined constructor calling
    /// back in) `RuntimeError`, and neither applies the copy.
    fn commit<A>(
        ruby: &Ruby,
        this: Obj<Self>,
        act: impl FnOnce(&mut Core) -> A,
        build: impl FnOnce(A) -> Result<Value, Error>,
    ) -> Result<Value, Error> {
        let before = this.core.borrow().clone();
        let mut after = before.clone();
        let value = build(act(&mut after))?;
        this.check_frozen()?;
        let mut core = this.core.borrow_mut();
        if *core != before {
            return Err(Error::new(
                ruby.exception_runtime_error(),
                "the stage changed while its answer was built; the answer was not applied",
            ));
        }
        *core = after;
        Ok(value)
    }

    fn scope(&self) -> u16 {
        self.core.borrow().scope()
    }

    fn issued(&self) -> u64 {
        self.core.borrow().issued()
    }

    fn capacity(&self) -> usize {
        self.core.borrow().capacity()
    }

    fn lanes(&self) -> usize {
        self.core.borrow().lanes()
    }

    fn size(&self) -> usize {
        self.core.borrow().len()
    }

    fn is_empty(&self) -> bool {
        self.core.borrow().is_empty()
    }

    fn is_draining(&self) -> bool {
        self.core.borrow().is_draining()
    }

    fn is_drained(&self) -> bool {
        self.core.borrow().is_drained()
    }

    fn state(ruby: &Ruby, this: Obj<Self>, id: Number) -> Option<Value> {
        let state = this.core.borrow().state(id.id());
        state.map(|state| ruby.to_symbol(names::state(state)).as_value())
    }

    fn submit(ruby: &Ruby, this: Obj<Self>, lane: Number, payload: Number) -> Result<Value, Error> {
        this.check_frozen()?;
        let outcome = this.core.borrow_mut().submit(lane.lane(), payload.0);
        Ok(match outcome {
            Ok(id) => ruby.integer_from_u64(id.raw()).as_value(),
            Err(refusal) => refused(ruby, refusal),
        })
    }

    /// The core writes ids to a caller's buffer and reports how many; here
    /// the buffer is sized to the lanes asked for and returned as an Array.
    fn fan_out(
        ruby: &Ruby,
        this: Obj<Self>,
        lanes: RArray,
        payload: Number,
    ) -> Result<Value, Error> {
        this.check_frozen()?;
        let lanes: Vec<usize> = numbers(lanes)?.into_iter().map(Number::lane).collect();
        let mut out = vec![JobId::default(); lanes.len()];
        let outcome = this.core.borrow_mut().fan_out(&lanes, payload.0, &mut out);
        Ok(match outcome {
            Ok(written) => {
                let ids = ruby.ary_new_capa(written);
                for id in out.iter().take(written) {
                    ids.push(id.raw())?;
                }
                ids.as_value()
            }
            Err(refusal) => refused(ruby, refusal),
        })
    }

    /// The answer is a `Dispatch` built by Ruby code, so the job is taken
    /// on a copy and the stage moves only once that value exists.
    fn dispatch(ruby: &Ruby, this: Obj<Self>, eligible: RArray) -> Result<Value, Error> {
        this.check_frozen()?;
        let eligible = eligibility(ruby, eligible)?;
        let class = data(ruby, |classes| classes.dispatch)?;
        Self::commit(
            ruby,
            this,
            |core| core.dispatch(&eligible),
            |work| {
                work.map_or_else(
                    || Ok(ruby.qnil().as_value()),
                    |work| class.funcall("new", (work.id().raw(), work.payload(), work.lane())),
                )
            },
        )
    }

    fn cancel(ruby: &Ruby, this: Obj<Self>, id: Number) -> Result<Value, Error> {
        this.check_frozen()?;
        let outcome = this.core.borrow_mut().cancel(id.id());
        Ok(answer(ruby, outcome, names::cancellation))
    }

    fn complete(ruby: &Ruby, this: Obj<Self>, id: Number) -> Result<Value, Error> {
        this.check_frozen()?;
        let outcome = this.core.borrow_mut().complete(id.id());
        Ok(answer(ruby, outcome, names::completion))
    }

    fn confirm_cancel(ruby: &Ruby, this: Obj<Self>, id: Number) -> Result<Value, Error> {
        this.check_frozen()?;
        let outcome = this.core.borrow_mut().confirm_cancel(id.id());
        Ok(answer(ruby, outcome, names::confirmation))
    }

    /// The answer is a `Reaped` built by Ruby code, so the job is reaped on
    /// a copy and the stage moves only once that value exists.
    fn reap(ruby: &Ruby, this: Obj<Self>, id: Number) -> Result<Value, Error> {
        this.check_frozen()?;
        let class = data(ruby, |classes| classes.reaped)?;
        Self::commit(
            ruby,
            this,
            |core| core.reap(id.id()),
            |outcome| match outcome {
                Ok(reaped) => {
                    let terminal = ruby.to_symbol(names::terminal(reaped.terminal()));
                    class.funcall("new", (reaped.payload(), terminal))
                }
                Err(refusal) => Ok(refused(ruby, refusal)),
            },
        )
    }

    fn join(ruby: &Ruby, this: Obj<Self>, ids: RArray) -> Result<Value, Error> {
        let ids: Vec<JobId> = numbers(ids)?.into_iter().map(Number::id).collect();
        let outcome = this.core.borrow().join(&ids);
        Ok(answer(ruby, outcome, names::join))
    }

    fn begin_drain(ruby: &Ruby, this: Obj<Self>) -> Result<Value, Error> {
        this.check_frozen()?;
        let outcome = this.core.borrow_mut().begin_drain();
        Ok(ruby.to_symbol(names::drain_begin(outcome)).as_value())
    }
}
