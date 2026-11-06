# frozen_string_literal: true
require 'minitest/autorun'
require 'pipeline_machines'

class StageTest < Minitest::Test
  def stage = PipelineMachines::Stage.new([1, 2, 1, 1])

  def test_fanout_lifecycle_and_reaping
    s = stage
    jobs = s.fan_out([0, 1], 42)
    assert_equal 2, jobs.size
    assert_equal :pending, s.join(jobs)
    2.times do
      job, payload, lane = s.dispatch
      assert_equal 42, payload
      assert_includes [0, 1], lane
      assert s.complete(job)
    end
    assert_nil s.dispatch
    assert_equal :ready, s.join(jobs)
    assert_equal [42, :complete], s.reap(jobs.first)
    assert_equal :unknown, s.join(jobs)
    assert_nil s.reap(jobs.first)
    refute s.complete(jobs.first)
    s.begin_drain
    refute s.drained?
    assert_equal [42, :complete], s.reap(jobs.last)
    assert s.drained?
    assert_nil s.submit(0, 1)
  end

  def test_unresolved_cancel_keeps_capacity
    s = stage
    jobs = 64.times.map { |i| s.submit(0, i) }
    assert_equal 64, s.capacity
    assert_equal 4, s.lanes
    assert_nil s.submit(0, 65)
    running, = s.dispatch
    assert s.cancel(running)
    assert_nil s.reap(running)
    assert_equal :pending, s.join([running])
    assert_nil s.submit(0, 65)
    assert s.confirm_cancel(running)
    assert_equal :cancelled, s.join([running])
    assert_equal [0, :cancelled], s.reap(running)
    fresh = s.submit(0, 99)
    refute s.complete(running)
    refute s.cancel(running)
    assert s.cancel(fresh)
    refute s.confirm_cancel(fresh)
    assert_equal [99, :cancelled], s.reap(fresh)
    assert s.cancel(jobs.last)
  end

  def test_completion_can_race_cancel
    s = stage
    s.submit(0, 1)
    job, = s.dispatch
    assert s.cancel(job)
    assert s.complete(job)
    refute s.confirm_cancel(job)
    assert_equal [1, :complete], s.reap(job)
  end

  def test_cross_owner_lifetime_and_gc
    a, b = stage, stage
    job = a.submit(0, 7)
    b.submit(0, 7)
    b.dispatch
    refute b.cancel(job)
    refute b.complete(job)
    refute b.confirm_cancel(job)
    assert_nil b.reap(job)
    assert_equal :unknown, b.join([job])
    assert_raises(TypeError) { PipelineMachines::Job.allocate }
    assert_raises(TypeError) { job.dup }
    a = nil
    GC.start
    refute stage.cancel(job)
  end

  def test_validation_is_atomic_and_strict
    assert_raises(TypeError) { PipelineMachines::Stage.new([1, 1.5, 1, 1]) }
    assert_raises(TypeError) { PipelineMachines::Stage.new([1]) }
    [0, 2**32].each { |v| assert_raises(ArgumentError) { PipelineMachines::Stage.new([v, 1, 1, 1]) } }
    s = stage
    [1.0, nil, '1', Float::INFINITY, Float::NAN].each do |v|
      assert_raises(TypeError) { s.submit(v, 1) }
      assert_raises(TypeError) { s.submit(0, v) }
      assert_raises(TypeError) { s.fan_out([0, v], 1) }
    end
    [-1, 2**64].each { |v| assert_raises(RangeError) { s.submit(0, v) } }
    assert_nil s.fan_out([0, 0], 1)
    assert_nil s.fan_out([0, 4], 1)
    assert_raises(ArgumentError) { s.fan_out([0, 1, 2, 3, 4], 1) }
    assert_nil s.dispatch
    assert_equal [], s.fan_out([], 1)
    assert_equal :ready, s.join([])
    assert_nil s.submit(4, 1)
    assert_raises(ArgumentError) { s.join(Array.new(65)) }
  end

  def test_freeze_all_mutations
    s = stage
    job = s.submit(0, 1)
    s.freeze
    [-> { s.submit(0, 2) }, -> { s.fan_out([0], 2) }, -> { s.dispatch },
     -> { s.cancel(job) }, -> { s.complete(job) }, -> { s.confirm_cancel(job) },
     -> { s.reap(job) }, -> { s.begin_drain }].each do |operation|
      assert_raises(FrozenError, &operation)
    end
    assert_equal :pending, s.join([job])
    refute s.drained?
  end

  def test_atomic_full_fanout_boundary
    s = stage
    63.times { |i| assert_instance_of PipelineMachines::Job, s.submit(0, i) }
    assert_nil s.fan_out([1, 2], 99)
    last = s.submit(3, 2**64 - 1)
    assert_instance_of PipelineMachines::Job, last
    assert_nil s.submit(0, 100)
  end

  def test_gc_stress_through_native_arrays
    previous = GC.stress
    GC.stress = true
    s = stage
    jobs = s.fan_out([0, 1, 2, 3], 2**64 - 1)
    4.times do
      job, payload, = s.dispatch
      assert_equal 2**64 - 1, payload
      assert s.complete(job)
    end
    GC.compact
    assert_equal :ready, s.join(jobs)
    jobs.each { |job| assert_equal [2**64 - 1, :complete], s.reap(job) }
  ensure
    GC.stress = previous
  end
end
