//! Private native entry point.
use crate::{CAPACITY, LANES, Stage, checked_dispatch, checked_reaped, classes, scope_of_id};
use magnus::{Error, Ruby, function, method, prelude::*};

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("PipelineMachines")?;
    classes::resolve(ruby, module)?;
    module.define_singleton_method("scope_of", function!(scope_of_id, 1))?;
    module.define_singleton_method("__dispatch", function!(checked_dispatch, 3))?;
    module.define_singleton_method("__reaped", function!(checked_reaped, 2))?;
    let class = module.define_class("Stage", ruby.class_object())?;
    class.undef_default_alloc_func();
    class.const_set("LANES", LANES)?;
    class.const_set("CAPACITY", CAPACITY)?;
    class.define_singleton_method("new", function!(Stage::new, 1))?;
    class.define_singleton_method("with_scope", function!(Stage::with_scope, 2))?;
    class.define_singleton_method("resumed", function!(Stage::resumed, 3))?;
    class.define_method("scope", method!(Stage::scope, 0))?;
    class.define_method("issued", method!(Stage::issued, 0))?;
    class.define_method("capacity", method!(Stage::capacity, 0))?;
    class.define_method("lanes", method!(Stage::lanes, 0))?;
    class.define_method("size", method!(Stage::size, 0))?;
    class.define_method("empty?", method!(Stage::is_empty, 0))?;
    class.define_method("draining?", method!(Stage::is_draining, 0))?;
    class.define_method("drained?", method!(Stage::is_drained, 0))?;
    class.define_method("state", method!(Stage::state, 1))?;
    class.define_method("submit", method!(Stage::submit, 2))?;
    class.define_method("fan_out", method!(Stage::fan_out, 2))?;
    class.define_method("dispatch", method!(Stage::dispatch, 1))?;
    class.define_method("cancel", method!(Stage::cancel, 1))?;
    class.define_method("complete", method!(Stage::complete, 1))?;
    class.define_method("confirm_cancel", method!(Stage::confirm_cancel, 1))?;
    class.define_method("reap", method!(Stage::reap, 1))?;
    class.define_method("join", method!(Stage::join, 1))?;
    class.define_method("begin_drain", method!(Stage::begin_drain, 0))?;
    Ok(())
}
