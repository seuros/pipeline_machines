# frozen_string_literal: true

require_relative 'pipeline_machines/version'

module PipelineMachines
  # The core's `Dispatch`: one job handed out for execution. `id` is the job
  # id (an Integer) to resolve with `complete` or `confirm_cancel`, `payload`
  # the Integer that was submitted, `lane` the lane it was submitted to.
  # Validated by the core's `Dispatch::new` whether the gem built it or you
  # did: TypeError for a non-Integer, RangeError for an id no stage issues
  # (sequence zero, out of range) or a lane past `Stage::LANES - 1`.
  Dispatch = Data.define(:id, :payload, :lane) do
    def initialize(id:, payload:, lane:)
      id, payload, lane = PipelineMachines.__send__(:__dispatch, id, payload, lane)
      super
    end
  end

  # The core's `Reaped`: what `reap` took off the stage. `payload` is the
  # Integer that was submitted, `terminal` how the job ended, :complete or
  # :cancelled. TypeError for a non-Integer payload or non-Symbol terminal,
  # RangeError for any other symbol or a payload out of range.
  Reaped = Data.define(:payload, :terminal) do
    def initialize(payload:, terminal:)
      payload, terminal = PipelineMachines.__send__(:__reaped, payload, terminal)
      super
    end
  end
end

# Native is required: no algorithm fallback. It validates and builds the Data
# values above and resolves their classes once at load, so they must exist
# first.
require 'pipeline_machines_native/pipeline_machines_native'

module PipelineMachines
  private_class_method :__dispatch, :__reaped

  # Sealed: no subclass can skip the check by overriding `initialize`, and no
  # bare `allocate` builds a value that never ran it. Sealing stops
  # accidents, not reflection; the stage never reads a value back (ids are
  # Integers), so a forged one cannot reach the core.
  [Dispatch, Reaped].each do |value|
    value.singleton_class.undef_method(:allocate)
    value.define_singleton_method(:inherited) { |_| raise TypeError, "#{value} cannot be subclassed" }
  end
end
