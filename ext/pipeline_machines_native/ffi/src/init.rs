//! Private native entry point.
use crate::Wrapper;
use magnus::{Error, Ruby, function, method, prelude::*};

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("PipelineMachines")?;
    module
        .define_class("Job", ruby.class_object())?
        .undef_default_alloc_func();
    let class = module.define_class("Stage", ruby.class_object())?;
    class.undef_default_alloc_func();
    class.define_singleton_method("new", function!(Wrapper::new, 1))?;
    class.define_method("submit", method!(Wrapper::submit, 2))?;
    class.define_method("fan_out", method!(Wrapper::fan_out, 2))?;
    class.define_method("dispatch", method!(Wrapper::dispatch, 0))?;
    class.define_method("cancel", method!(Wrapper::cancel, 1))?;
    class.define_method("complete", method!(Wrapper::complete, 1))?;
    class.define_method("confirm_cancel", method!(Wrapper::confirm_cancel, 1))?;
    class.define_method("reap", method!(Wrapper::reap, 1))?;
    class.define_method("join", method!(Wrapper::join, 1))?;
    class.define_method("begin_drain", method!(Wrapper::begin_drain, 0))?;
    class.define_method("drained?", method!(Wrapper::is_drained, 0))?;
    class.define_method("capacity", function!(Wrapper::capacity, 0))?;
    class.define_method("lanes", function!(Wrapper::lanes, 0))?;
    Ok(())
}
