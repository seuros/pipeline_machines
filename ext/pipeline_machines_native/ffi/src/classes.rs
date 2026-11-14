//! The gem's `Data` classes, resolved once when the extension loads.
use std::sync::OnceLock;

use magnus::{Error, RClass, RModule, Ruby, prelude::*, value::Opaque};

/// `PipelineMachines::Dispatch` and `Reaped`, resolved once at init:
/// swapping a constant later changes nothing the stage builds.
pub struct Classes {
    pub dispatch: Opaque<RClass>,
    pub reaped: Opaque<RClass>,
}

static CLASSES: OnceLock<Classes> = OnceLock::new();

/// Resolve and pin the `Data` classes Ruby defined before loading this.
pub fn resolve(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let class = |name: &str| -> Result<Opaque<RClass>, Error> {
        let class: RClass = module.const_get(name)?;
        // Pinned: compaction must not move what the static points at.
        ruby.gc_register_mark_object(class);
        Ok(class.into())
    };
    let classes = Classes {
        dispatch: class("Dispatch")?,
        reaped: class("Reaped")?,
    };
    CLASSES.set(classes).map_err(|_| {
        Error::new(
            ruby.exception_runtime_error(),
            "the pipeline_machines extension was initialised twice",
        )
    })
}

pub fn data(ruby: &Ruby, pick: fn(&Classes) -> Opaque<RClass>) -> Result<RClass, Error> {
    CLASSES
        .get()
        .map(|classes| ruby.get_inner(pick(classes)))
        .ok_or_else(|| {
            Error::new(
                ruby.exception_runtime_error(),
                "the pipeline_machines extension is not initialised",
            )
        })
}
