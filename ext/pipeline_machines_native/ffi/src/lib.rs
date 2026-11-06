//! Native boundary: all lifecycle decisions are delegated to the stage core.
mod init;

use magnus::{
    Error, Integer, RArray, Ruby, Symbol, TryConvert, Value, prelude::*, typed_data::Obj,
};
use pipeline_machines::{JobId, Join, Stage, State};
use std::{cell::RefCell, num::NonZeroU32, sync::Arc};

const LANES: usize = 4;
const CAPACITY: usize = 64;

#[derive(Clone, Copy)]
struct Number(u64);

impl TryConvert for Number {
    fn try_convert(value: Value) -> Result<Self, Error> {
        if Integer::from_value(value).is_none() {
            return Err(Error::new(
                Ruby::get_with(value).exception_type_error(),
                "expected an unsigned Integer",
            ));
        }
        u64::try_convert(value).map(Self)
    }
}

#[magnus::wrap(class = "PipelineMachines::Job", free_immediately, size)]
struct Handle {
    owner: Arc<()>,
    id: JobId,
}

#[magnus::wrap(class = "PipelineMachines::Stage", free_immediately, size)]
struct Wrapper {
    owner: Arc<()>,
    core: RefCell<Stage<u64, LANES, CAPACITY>>,
}

impl Wrapper {
    fn new(ruby: &Ruby, weights: RArray) -> Result<Self, Error> {
        let values = weights.to_value_array::<LANES>()?;
        let mut converted = [NonZeroU32::MIN; LANES];
        for (slot, value) in converted.iter_mut().zip(values) {
            let number = Number::try_convert(value)?.0;
            *slot = u32::try_from(number)
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or_else(|| {
                    Error::new(ruby.exception_arg_error(), "weights must be in 1..2**32-1")
                })?;
        }
        let core = Stage::new(converted)
            .ok_or_else(|| Error::new(ruby.exception_arg_error(), "invalid stage bounds"))?;
        Ok(Self {
            owner: Arc::new(()),
            core: RefCell::new(core),
        })
    }

    fn handle(&self, id: JobId) -> Handle {
        Handle {
            owner: Arc::clone(&self.owner),
            id,
        }
    }

    fn submit(this: Obj<Self>, lane: Number, payload: Number) -> Result<Option<Handle>, Error> {
        this.check_frozen()?;
        let lane = usize::try_from(lane.0).map_err(|_| {
            Error::new(
                Ruby::get_with(this).exception_range_error(),
                "lane out of range",
            )
        })?;
        let id = this.core.borrow_mut().submit(lane, payload.0);
        Ok(id.map(|id| this.handle(id)))
    }

    fn fan_out(
        ruby: &Ruby,
        this: Obj<Self>,
        lanes: RArray,
        payload: Number,
    ) -> Result<Option<RArray>, Error> {
        this.check_frozen()?;
        if lanes.len() > LANES {
            return Err(Error::new(
                ruby.exception_arg_error(),
                "at most four distinct lanes",
            ));
        }
        let mut converted = Vec::with_capacity(lanes.len());
        for index in 0..lanes.len() {
            let number = Number::try_convert(lanes.entry(index.cast_signed())?)?.0;
            converted.push(
                usize::try_from(number)
                    .map_err(|_| Error::new(ruby.exception_range_error(), "lane out of range"))?,
            );
        }
        let ids = this.core.borrow_mut().fan_out(&converted, payload.0);
        let Some(ids) = ids else {
            return Ok(None);
        };
        let result = ruby.ary_new();
        for id in ids.into_iter().flatten() {
            result.push(this.handle(id))?;
        }
        Ok(Some(result))
    }

    fn dispatch(ruby: &Ruby, this: Obj<Self>) -> Result<Option<RArray>, Error> {
        this.check_frozen()?;
        let work = this.core.borrow_mut().dispatch(&[true; LANES]);
        let Some(work) = work else {
            return Ok(None);
        };
        let result = ruby.ary_new();
        result.push(this.handle(work.id))?;
        result.push(work.payload)?;
        result.push(work.lane)?;
        Ok(Some(result))
    }

    fn cancel(this: Obj<Self>, handle: Obj<Handle>) -> Result<bool, Error> {
        this.check_frozen()?;
        Ok(Arc::ptr_eq(&this.owner, &handle.owner) && this.core.borrow_mut().cancel(handle.id))
    }

    fn complete(this: Obj<Self>, handle: Obj<Handle>) -> Result<bool, Error> {
        this.check_frozen()?;
        Ok(Arc::ptr_eq(&this.owner, &handle.owner) && this.core.borrow_mut().complete(handle.id))
    }

    fn confirm_cancel(this: Obj<Self>, handle: Obj<Handle>) -> Result<bool, Error> {
        this.check_frozen()?;
        Ok(Arc::ptr_eq(&this.owner, &handle.owner)
            && this.core.borrow_mut().confirm_cancel(handle.id))
    }

    fn reap(
        ruby: &Ruby,
        this: Obj<Self>,
        handle: Obj<Handle>,
    ) -> Result<Option<(u64, Symbol)>, Error> {
        this.check_frozen()?;
        if !Arc::ptr_eq(&this.owner, &handle.owner) {
            return Ok(None);
        }
        let result = this.core.borrow_mut().reap(handle.id);
        Ok(result.map(|(payload, state)| (payload, ruby.to_symbol(state_name(state)))))
    }

    fn join(ruby: &Ruby, this: Obj<Self>, handles: RArray) -> Result<Symbol, Error> {
        if handles.len() > CAPACITY {
            return Err(Error::new(
                ruby.exception_arg_error(),
                "at most 64 join handles",
            ));
        }
        let mut ids = Vec::with_capacity(handles.len());
        for index in 0..handles.len() {
            let handle: Obj<Handle> = handles.entry(index.cast_signed())?;
            if !Arc::ptr_eq(&this.owner, &handle.owner) {
                return Ok(ruby.to_symbol("unknown"));
            }
            ids.push(handle.id);
        }
        let result = this.core.borrow().join(&ids);
        Ok(ruby.to_symbol(match result {
            Join::Ready => "ready",
            Join::Pending => "pending",
            Join::Cancelled => "cancelled",
            _ => "unknown",
        }))
    }

    fn begin_drain(this: Obj<Self>) -> Result<(), Error> {
        this.check_frozen()?;
        this.core.borrow_mut().begin_drain();
        Ok(())
    }

    fn is_drained(&self) -> bool {
        self.core.borrow().is_drained()
    }
    const fn capacity() -> usize {
        CAPACITY
    }
    const fn lanes() -> usize {
        LANES
    }
}

const fn state_name(state: State) -> &'static str {
    match state {
        State::Queued => "queued",
        State::Running => "running",
        State::Cancelling => "cancelling",
        State::Complete => "complete",
        _ => "cancelled",
    }
}
