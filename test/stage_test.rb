# frozen_string_literal: true

require 'minitest/autorun'
require 'pipeline_machines'

class StageTest < Minitest::Test
  ALL = [true, true, true, true].freeze
  Reaped = PipelineMachines::Reaped

  def stage = PipelineMachines::Stage.new([1, 2, 1, 1])

  def test_fanout_lifecycle_and_reaping
    s = stage
    jobs = s.fan_out([0, 1], 42)
    assert_equal [1, 2], jobs
    assert_equal :pending, s.join(jobs)
    assert_equal [2, false], [s.size, s.empty?]
    2.times do
      work = s.dispatch(ALL)
      assert_instance_of PipelineMachines::Dispatch, work
      assert_equal 42, work.payload
      assert_equal jobs.index(work.id), work.lane
      assert_equal :running, s.state(work.id)
      assert_equal :completed, s.complete(work.id)
    end
    assert_nil s.dispatch(ALL)
    assert_equal :ready, s.join(jobs)
    assert_equal Reaped.new(42, :complete), s.reap(jobs.first)
    assert_equal :stale, s.join(jobs)
    assert_equal :stale, s.reap(jobs.first)
    assert_equal :stale, s.complete(jobs.first)
    assert_nil s.state(jobs.first)
    assert_equal :begun, s.begin_drain
    assert_equal :already_draining, s.begin_drain
    assert_equal [true, false], [s.draining?, s.drained?]
    assert_equal Reaped.new(42, :complete), s.reap(jobs.last)
    assert_predicate s, :drained?
    assert_predicate s, :empty?
    assert_equal :draining, s.submit(0, 1)
    assert_equal :draining, s.fan_out([0], 1)
  end

  def test_dispatch_takes_lane_eligibility
    s = stage
    a = s.submit(0, 10)
    b = s.submit(1, 20)
    assert_nil s.dispatch([false, false, true, true])
    work = s.dispatch([false, true, false, false])
    assert_equal [b, 20, 1], [work.id, work.payload, work.lane]
    assert_equal PipelineMachines::Dispatch.new(a, 10, 0), s.dispatch(ALL)
    assert_raises(TypeError) { s.dispatch([true, true, true]) }
    assert_raises(TypeError) { s.dispatch([true, true, true, nil]) }
    assert_raises(TypeError) { s.dispatch([true, true, true, 1]) }
    assert_raises(TypeError) { s.dispatch(true) }
    assert_raises(ArgumentError) { s.dispatch }
  end

  # Each lane's eligibility is exactly true or false, never truthiness, and
  # the refusal names the argument; nothing is dispatched.
  def test_dispatch_eligibility_is_exactly_a_boolean
    s = stage
    id = s.submit(0, 10)
    [nil, 1, 0, 'true', :true].each do |odd|
      error = assert_raises(TypeError) { s.dispatch([odd, true, true, true]) }
      assert_equal 'eligible must be true or false', error.message
    end
    assert_equal :queued, s.state(id)
  end

  def test_unresolved_cancel_keeps_capacity
    s = stage
    jobs = 64.times.map { |i| s.submit(0, i) }
    assert_equal [64, 4, 64], [s.capacity, s.lanes, s.size]
    assert_equal [4, 64], [PipelineMachines::Stage::LANES, PipelineMachines::Stage::CAPACITY]
    assert_equal :full, s.submit(0, 65)
    running = s.dispatch(ALL).id
    assert_equal :cancelling, s.cancel(running)
    assert_equal :already_cancelling, s.cancel(running)
    assert_equal :not_terminal, s.reap(running)
    assert_equal :pending, s.join([running])
    assert_equal :full, s.submit(0, 65)
    assert_equal :confirmed, s.confirm_cancel(running)
    assert_equal :already_cancelled, s.confirm_cancel(running)
    assert_equal :already_cancelled, s.cancel(running)
    assert_equal :concluded, s.complete(running)
    assert_equal :cancelled, s.join([running])
    assert_equal Reaped.new(0, :cancelled), s.reap(running)
    fresh = s.submit(0, 99)
    assert_equal :stale, s.complete(running)
    assert_equal :stale, s.cancel(running)
    assert_equal :not_running, s.complete(fresh)
    assert_equal :not_cancelling, s.confirm_cancel(fresh)
    assert_equal :cancelled, s.cancel(fresh)
    assert_equal :already_cancelled, s.confirm_cancel(fresh)
    assert_equal Reaped.new(99, :cancelled), s.reap(fresh)
    assert_equal :cancelled, s.cancel(jobs.last)
  end

  def test_completion_can_race_cancel
    s = stage
    s.submit(0, 1)
    job = s.dispatch(ALL).id
    assert_equal :cancelling, s.cancel(job)
    assert_equal :completed, s.complete(job)
    assert_equal :already_complete, s.complete(job)
    assert_equal :concluded, s.confirm_cancel(job)
    assert_equal :concluded, s.cancel(job)
    assert_equal :complete, s.state(job)
    assert_equal Reaped.new(1, :complete), s.reap(job)
  end

  def test_fan_out_is_all_lanes_or_none
    s = stage
    assert_equal :repeated_lane, s.fan_out([0, 0], 1)
    assert_equal :unknown_lane, s.fan_out([0, 4], 1)
    assert_equal :unknown_lane, s.fan_out([0, 1, 2, 3, 4], 1)
    assert_equal :repeated_lane, s.fan_out([0, 1, 2, 3, 0], 1)
    assert_equal :unknown_lane, s.submit(4, 1)
    assert_equal :unknown_lane, s.submit((2**64) - 1, 1)
    assert_equal [], s.fan_out([], 1)
    assert_equal [0, 0], [s.size, s.issued]
    63.times { |i| assert_equal i + 1, s.submit(0, i) }
    assert_equal :full, s.fan_out([1, 2], 99)
    assert_equal [63, 63], [s.size, s.issued]
    assert_equal 64, s.submit(3, (2**64) - 1)
    assert_equal :full, s.submit(0, 100)
  end

  def test_malformed_input_raises
    assert_raises(TypeError) { PipelineMachines::Stage.new([1, 1.5, 1, 1]) }
    assert_raises(TypeError) { PipelineMachines::Stage.new([1]) }
    assert_raises(TypeError) { PipelineMachines::Stage.new(1) }
    [0, 2**32].each { |v| assert_raises(RangeError) { PipelineMachines::Stage.new([v, 1, 1, 1]) } }
    assert_raises(TypeError) { PipelineMachines::Stage.allocate }
    s = stage
    [1.0, nil, '1', Float::INFINITY, Float::NAN].each do |v|
      assert_raises(TypeError) { s.submit(v, 1) }
      assert_raises(TypeError) { s.submit(0, v) }
      assert_raises(TypeError) { s.fan_out([0, v], 1) }
      assert_raises(TypeError) { s.fan_out([0], v) }
      assert_raises(TypeError) { s.join([v]) }
      %i[cancel complete confirm_cancel reap state].each { |m| assert_raises(TypeError) { s.public_send(m, v) } }
    end
    [-1, 2**64].each do |v|
      assert_raises(RangeError) { s.submit(0, v) }
      assert_raises(RangeError) { s.submit(v, 0) }
      assert_raises(RangeError) { s.cancel(v) }
    end
    assert_equal [0, 0], [s.size, s.issued] # nothing was admitted along the way
    assert_nil s.dispatch(ALL)
    assert_equal :ready, s.join([])
  end

  def test_array_subclasses_are_refused
    s = stage
    list = Class.new(Array)
    error = assert_raises(TypeError) { PipelineMachines::Stage.new(list[1, 1, 1, 1]) }
    assert_equal 'weights must be an Array of 4 Integers', error.message
    assert_raises(TypeError) { PipelineMachines::Stage.with_scope(list[1, 1, 1, 1], 0) }
    assert_raises(TypeError) { PipelineMachines::Stage.resumed(list[1, 1, 1, 1], 0, 0) }
    error = assert_raises(TypeError) { s.fan_out(list[0], 1) }
    assert_equal 'lanes must be an Array', error.message
    error = assert_raises(TypeError) { s.dispatch(list[true, true, true, true]) }
    assert_equal 'eligible must be an Array of 4 true or false', error.message
    error = assert_raises(TypeError) { s.join(list[]) }
    assert_equal 'ids must be an Array', error.message
    assert_equal [0, 0], [s.size, s.issued]
  end

  def test_the_extension_alone_is_a_name_error
    lib = File.expand_path('../lib', __dir__)
    script = 'begin; require "pipeline_machines_native/pipeline_machines_native"; ' \
             'rescue NameError => e; print e.class; end'
    output = IO.popen([RbConfig.ruby, '-I', lib, '-e', script], err: %i[child out], &:read)
    assert_equal 'NameError', output
  end

  def test_freeze_all_mutations
    s = stage
    job = s.submit(0, 1)
    s.freeze
    [-> { s.submit(0, 2) }, -> { s.fan_out([0], 2) }, -> { s.dispatch(ALL) },
     -> { s.cancel(job) }, -> { s.complete(job) }, -> { s.confirm_cancel(job) },
     -> { s.reap(job) }, -> { s.begin_drain }].each do |operation|
      assert_raises(FrozenError, &operation)
    end
    # Frozen is checked before the arguments: a bad one still says frozen.
    [-> { s.submit(nil, 2) }, -> { s.submit(0, -1) }, -> { s.fan_out(nil, 2) },
     -> { s.fan_out([0], 1.0) }, -> { s.dispatch(nil) }, -> { s.dispatch([true]) },
     -> { s.cancel(nil) }, -> { s.complete(-1) }, -> { s.confirm_cancel('1') },
     -> { s.reap(2**64) }].each do |operation|
      assert_raises(FrozenError, &operation)
    end
    assert_equal :pending, s.join([job])
    assert_equal :queued, s.state(job)
    assert_equal [1, 1], [s.size, s.issued]
    refute_predicate s, :drained?
  end

  def test_gc_stress_through_native_arrays
    previous = GC.stress
    GC.stress = true
    s = stage
    jobs = s.fan_out([0, 1, 2, 3], (2**64) - 1)
    4.times do
      work = s.dispatch(ALL)
      assert_equal (2**64) - 1, work.payload
      assert_equal :completed, s.complete(work.id)
    end
    GC.compact
    assert_equal :ready, s.join(jobs)
    jobs.each { |job| assert_equal Reaped.new((2**64) - 1, :complete), s.reap(job) }
  ensure
    GC.stress = previous
  end
end
