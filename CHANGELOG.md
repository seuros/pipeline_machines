# Changelog

## [0.3.0](https://github.com/seuros/pipeline_machines/compare/pipeline_machines/v0.2.0...pipeline_machines/v0.3.0) (2026-11-14)


### ⚠ BREAKING CHANGES

* seal Dispatch and Reaped, resolve classes at load, re-check frozen before keeping

### Features

* seal Dispatch and Reaped, resolve classes at load, re-check frozen before keeping ([4bed337](https://github.com/seuros/pipeline_machines/commit/4bed3379156c750d3acaff788de552977f41df3f))

## [0.2.0](https://github.com/seuros/pipeline_machines/compare/pipeline_machines/v0.1.4...pipeline_machines/v0.2.0) (2026-11-13)


### ⚠ BREAKING CHANGES

* converted to the fleet conventions. One exhaustive Refusal per crate, Result from every operation that can refuse, a refusal changes nothing, provable idempotent repeats are honoured, Ruby mirrors the core. No compatibility aliases. Requires flow-machines >= 0.3 and priority-machines >= 0.4. JobId is a scoped u64 issued through periscope-machines; Ruby job ids are Integers.

### Features

* one Refusal, scoped job ids, atomic fan_out into a buffer, and lane eligibility in Ruby ([6ae6fd4](https://github.com/seuros/pipeline_machines/commit/6ae6fd428bea3f67463ce3bb49862fa7c59259c2))

## [0.1.4](https://github.com/seuros/pipeline_machines/compare/pipeline_machines/v0.1.3...pipeline_machines/v0.1.4) (2026-11-06)


### Bug Fixes

* extract has_queued lane check ([8c9510f](https://github.com/seuros/pipeline_machines/commit/8c9510fbd5987c2d2f9d4c653ad7aff983e30955))

## [0.1.3](https://github.com/seuros/pipeline_machines/compare/pipeline_machines/v0.1.2...pipeline_machines/v0.1.3) (2026-11-06)


### Bug Fixes

* crates.io keywords and categories for the core ([f308575](https://github.com/seuros/pipeline_machines/commit/f30857529c4ac61c145128710bb916770699f760))

## [0.1.2](https://github.com/seuros/pipeline_machines/compare/pipeline_machines/v0.1.1...pipeline_machines/v0.1.2) (2026-11-06)


### Bug Fixes

* keep test sources out of the packaged gem and crate ([cd46e0d](https://github.com/seuros/pipeline_machines/commit/cd46e0d8adb4868270b1278fb7cd24c001c89c72))

## [0.1.1](https://github.com/seuros/pipeline_machines/compare/pipeline_machines-v0.1.0...pipeline_machines/v0.1.1) (2026-11-06)


### Features

* initial extraction ([5e3f8da](https://github.com/seuros/pipeline_machines/commit/5e3f8dadd4cb7159e5fcbdf64a0c8d2de110c680))


### Bug Fixes

* name the gemspec block spec so the release workflow detects the extension ([4a86b80](https://github.com/seuros/pipeline_machines/commit/4a86b80992c296d0157d5488daea23c1295665c4))
