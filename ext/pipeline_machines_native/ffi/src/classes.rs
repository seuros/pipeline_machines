//! The gem's `Data` classes, resolved once when the extension loads.
use magnus::{Error, RModule, Ruby};
use matryona_machines::ruby::{Class, Registry};

/// `PipelineMachines::Dispatch` and `Reaped`, resolved once at init:
/// swapping a constant later changes nothing the stage builds.
pub struct Classes {
    pub dispatch: Class,
    pub reaped: Class,
}

static CLASSES: Registry<Classes> = Registry::new("pipeline_machines");

/// Resolve and pin the `Data` classes Ruby defined before loading this.
pub fn resolve(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let class = |name| Class::resolve(ruby, module, name);
    let classes = Classes {
        dispatch: class("Dispatch")?,
        reaped: class("Reaped")?,
    };
    CLASSES.set(ruby, classes).map(drop)
}

/// The classes resolved at init.
pub fn classes(ruby: &Ruby) -> Result<&'static Classes, Error> {
    CLASSES.get(ruby)
}
