# frozen_string_literal: true

require_relative 'pipeline_machines/version'

module PipelineMachines
  # The core's `Dispatch`: one job handed out for execution. `id` is the job
  # id (an Integer) to resolve with `complete` or `confirm_cancel`, `payload`
  # the Integer that was submitted, `lane` the lane it was submitted to.
  Dispatch = Data.define(:id, :payload, :lane)

  # The core's `Reaped`: what `reap` took off the stage. `payload` is the
  # Integer that was submitted, `terminal` how the job ended, :complete or
  # :cancelled.
  Reaped = Data.define(:payload, :terminal)
end

# Native is required: no algorithm fallback. It builds the Data values above,
# so they must exist first.
require 'pipeline_machines_native/pipeline_machines_native'
