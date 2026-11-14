//! Native boundary: all lifecycle decisions are delegated to the stage core.
mod classes;
mod init;
mod names;

use std::{cell::RefCell, num::NonZeroU32};

use classes::classes;
use magnus::{Error, Ruby, Symbol, Value, prelude::*, typed_data::Obj};
use matryona_machines::ruby::{
    Symbols, boolean, exact_array, exact_array_n, integer, keep, positive, symbol,
};
use names::{
    Cancellations, Completions, Confirmations, DrainBegins, Joins, Refusals, States, Terminals,
};
use pipeline_machines::{Dispatch, JobId, Reaped, Refusal};

const LANES: usize = 4;
const CAPACITY: usize = 64;

type Core = pipeline_machines::Stage<u64, LANES, CAPACITY>;

/// A lane index. One beyond the address space names no lane, like any other
/// index past the stage's last: the core refuses it.
fn lane(value: Value, name: &str) -> Result<usize, Error> {
    integer::<u64>(value, name).map(|lane| usize::try_from(lane).unwrap_or(usize::MAX))
}

/// A job id: an Integer in `0..=u64::MAX`, whatever it encodes.
fn id(value: Value, name: &str) -> Result<JobId, Error> {
    integer(value, name).map(JobId::from_raw)
}

/// Lane weights: exactly an Array (no subclass) of exactly `LANES` Integers
/// in `1..=u32::MAX`.
fn weights(ruby: &Ruby, weights: Value) -> Result<[NonZeroU32; LANES], Error> {
    let values = exact_array_n::<LANES>(ruby, weights, "weights must be an Array of 4 Integers")?;
    let mut converted = [NonZeroU32::MIN; LANES];
    for (slot, value) in converted.iter_mut().zip(values) {
        *slot = positive(value, "weights")?;
    }
    Ok(converted)
}

/// Lane eligibility: exactly an Array (no subclass) of exactly `LANES`
/// values, each `true` or `false`.
fn eligibility(ruby: &Ruby, eligible: Value) -> Result<[bool; LANES], Error> {
    let values = exact_array_n::<LANES>(
        ruby,
        eligible,
        "eligible must be an Array of 4 true or false",
    )?;
    let mut converted = [false; LANES];
    for (slot, value) in converted.iter_mut().zip(values) {
        *slot = boolean(value, "eligible")?;
    }
    Ok(converted)
}

/// Exactly an Array (no subclass) of Integers, each read by `read`, before
/// the core is touched.
fn numbers<T>(
    ruby: &Ruby,
    array: Value,
    name: &str,
    read: fn(Value, &str) -> Result<T, Error>,
) -> Result<Vec<T>, Error> {
    let array = exact_array(ruby, array, format!("{name} must be an Array"))?;
    (0..array.len())
        .map(|index| read(array.entry(index.cast_signed())?, name))
        .collect()
}

fn refused(ruby: &Ruby, refusal: Refusal) -> Value {
    Refusals::symbol(ruby, refusal).as_value()
}

/// An honoured unit outcome or a refusal, as the symbol it maps to.
fn answer<T>(ruby: &Ruby, outcome: Result<T, Refusal>, name: fn(T) -> &'static str) -> Value {
    symbol(ruby, outcome.map_or_else(Refusals::name, name)).as_value()
}

/// What `Dispatch.new` stores, as `Dispatch::new` for a stage of `LANES`
/// lanes accepts it: `TypeError` for a non-Integer, `RangeError` for an id no
/// stage issues or a lane past the last.
fn checked_dispatch(
    ruby: &Ruby,
    id: Value,
    payload: Value,
    lane: Value,
) -> Result<(u64, u64, usize), Error> {
    let (id, payload) = (self::id(id, "id")?, integer(payload, "payload")?);
    Dispatch::new::<LANES>(id, payload, self::lane(lane, "lane")?)
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
fn checked_reaped(ruby: &Ruby, payload: Value, terminal: Value) -> Result<(u64, Symbol), Error> {
    let payload = integer(payload, "payload")?;
    let terminal = Terminals::parse(ruby, terminal)?;
    let reaped = Reaped::new(payload, terminal);
    Ok((reaped.payload(), Terminals::symbol(ruby, reaped.terminal())))
}

/// `PipelineMachines.scope_of(id)`: the scope an id was issued under.
fn scope_of_id(id: Value) -> Result<u16, Error> {
    self::id(id, "id").map(JobId::scope)
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

    fn new(ruby: &Ruby, lanes: Value) -> Result<Self, Error> {
        let core = Core::new(weights(ruby, lanes)?);
        Self::build(ruby, core, "invalid stage bounds")
    }

    fn with_scope(ruby: &Ruby, lanes: Value, scope: Value) -> Result<Self, Error> {
        let scope = integer(scope, "scope")?;
        let core = Core::with_scope(weights(ruby, lanes)?, scope);
        Self::build(ruby, core, "invalid stage bounds")
    }

    fn resumed(ruby: &Ruby, lanes: Value, scope: Value, issued: Value) -> Result<Self, Error> {
        let (scope, issued) = (integer(scope, "scope")?, integer(issued, "issued")?);
        let core = Core::resumed(weights(ruby, lanes)?, scope, issued);
        Self::build(
            ruby,
            core,
            "issued is past the last sequence of a scope (2**48 - 1)",
        )
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

    fn state(ruby: &Ruby, this: Obj<Self>, id: Value) -> Result<Option<Value>, Error> {
        let id = self::id(id, "id")?;
        let state = this.core.borrow().state(id);
        Ok(state.map(|state| States::symbol(ruby, state).as_value()))
    }

    fn submit(ruby: &Ruby, this: Obj<Self>, lane: Value, payload: Value) -> Result<Value, Error> {
        this.check_frozen()?;
        let (lane, payload) = (self::lane(lane, "lane")?, integer(payload, "payload")?);
        let outcome = this.core.borrow_mut().submit(lane, payload);
        Ok(match outcome {
            Ok(id) => ruby.integer_from_u64(id.raw()).as_value(),
            Err(refusal) => refused(ruby, refusal),
        })
    }

    /// The core writes ids to a caller's buffer and reports how many; here
    /// the buffer is sized to the lanes asked for and returned as an Array.
    fn fan_out(ruby: &Ruby, this: Obj<Self>, lanes: Value, payload: Value) -> Result<Value, Error> {
        this.check_frozen()?;
        let payload = integer(payload, "payload")?;
        let lanes = numbers(ruby, lanes, "lanes", lane)?;
        let mut out = vec![JobId::default(); lanes.len()];
        let outcome = this.core.borrow_mut().fan_out(&lanes, payload, &mut out);
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
    fn dispatch(ruby: &Ruby, this: Obj<Self>, eligible: Value) -> Result<Value, Error> {
        this.check_frozen()?;
        let eligible = eligibility(ruby, eligible)?;
        let class = classes(ruby)?.dispatch.get(ruby);
        keep(
            ruby,
            this,
            &this.core,
            |core| core.dispatch(&eligible),
            |work| {
                work.map_or_else(
                    || Ok(ruby.qnil().as_value()),
                    |work| class.funcall("new", (work.id().raw(), work.payload(), work.lane())),
                )
            },
        )
    }

    fn cancel(ruby: &Ruby, this: Obj<Self>, id: Value) -> Result<Value, Error> {
        this.check_frozen()?;
        let id = self::id(id, "id")?;
        let outcome = this.core.borrow_mut().cancel(id);
        Ok(answer(ruby, outcome, Cancellations::name))
    }

    fn complete(ruby: &Ruby, this: Obj<Self>, id: Value) -> Result<Value, Error> {
        this.check_frozen()?;
        let id = self::id(id, "id")?;
        let outcome = this.core.borrow_mut().complete(id);
        Ok(answer(ruby, outcome, Completions::name))
    }

    fn confirm_cancel(ruby: &Ruby, this: Obj<Self>, id: Value) -> Result<Value, Error> {
        this.check_frozen()?;
        let id = self::id(id, "id")?;
        let outcome = this.core.borrow_mut().confirm_cancel(id);
        Ok(answer(ruby, outcome, Confirmations::name))
    }

    /// The answer is a `Reaped` built by Ruby code, so the job is reaped on
    /// a copy and the stage moves only once that value exists.
    fn reap(ruby: &Ruby, this: Obj<Self>, id: Value) -> Result<Value, Error> {
        this.check_frozen()?;
        let id = self::id(id, "id")?;
        let class = classes(ruby)?.reaped.get(ruby);
        keep(
            ruby,
            this,
            &this.core,
            |core| core.reap(id),
            |outcome| match outcome {
                Ok(reaped) => {
                    let terminal = Terminals::symbol(ruby, reaped.terminal());
                    class.funcall("new", (reaped.payload(), terminal))
                }
                Err(refusal) => Ok(refused(ruby, refusal)),
            },
        )
    }

    fn join(ruby: &Ruby, this: Obj<Self>, ids: Value) -> Result<Value, Error> {
        let ids = numbers(ruby, ids, "ids", id)?;
        let outcome = this.core.borrow().join(&ids);
        Ok(answer(ruby, outcome, Joins::name))
    }

    fn begin_drain(ruby: &Ruby, this: Obj<Self>) -> Result<Value, Error> {
        this.check_frozen()?;
        let outcome = this.core.borrow_mut().begin_drain();
        Ok(DrainBegins::symbol(ruby, outcome).as_value())
    }
}
