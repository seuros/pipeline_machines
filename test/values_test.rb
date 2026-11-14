# frozen_string_literal: true

require 'minitest/autorun'
require 'pipeline_machines'

# Values are valid or do not exist, and the binding never loses work.
class ValuesTest < Minitest::Test
  ALL = [true, true, true, true].freeze
  Dispatch = PipelineMachines::Dispatch
  Reaped = PipelineMachines::Reaped
  Stage = PipelineMachines::Stage

  def test_dispatch_holds_what_a_stage_could_dispatch
    work = Dispatch.new(1, 42, 3)
    assert_equal [1, 42, 3], [work.id, work.payload, work.lane]
    assert_equal work, Dispatch.new(id: 1, payload: 42, lane: 3)
    assert_equal Dispatch.new((7 << 48) | 1, (2**64) - 1, 0), Dispatch.new((7 << 48) | 1, (2**64) - 1, 0)
    assert_predicate work, :frozen?
    assert_equal Dispatch.new(1, 42, 2), work.with(lane: 2)
  end

  def test_dispatch_refuses_what_no_stage_dispatches
    [1.0, nil, '1', :one, [1]].each do |v|
      assert_raises(TypeError) { Dispatch.new(v, 0, 0) }
      assert_raises(TypeError) { Dispatch.new(1, v, 0) }
      assert_raises(TypeError) { Dispatch.new(1, 0, v) }
    end
    [-1, 2**64].each do |v|
      assert_raises(RangeError) { Dispatch.new(v, 0, 0) }
      assert_raises(RangeError) { Dispatch.new(1, v, 0) }
      assert_raises(RangeError) { Dispatch.new(1, 0, v) }
    end
    # Sequence zero is never issued, in any scope.
    [0, 7 << 48].each { |id| assert_raises(RangeError) { Dispatch.new(id, 0, 0) } }
    # A stage has LANES lanes.
    assert_raises(RangeError) { Dispatch.new(1, 0, Stage::LANES) }
    assert_raises(RangeError) { Dispatch.new(1, 0, 0).with(lane: 4) }
    assert_raises(ArgumentError) { Dispatch.new(1, 0) }
  end

  def test_reaped_holds_a_payload_and_a_terminal_state
    assert_equal [42, :complete], Reaped.new(42, :complete).to_h.values
    assert_equal Reaped.new(0, :cancelled), Reaped.new(payload: 0, terminal: :cancelled)
    [1.0, nil, '1', :one].each { |v| assert_raises(TypeError) { Reaped.new(v, :complete) } }
    [-1, 2**64].each { |v| assert_raises(RangeError) { Reaped.new(v, :complete) } }
    ['complete', nil, 1, true].each { |v| assert_raises(TypeError) { Reaped.new(1, v) } }
    %i[nope queued running cancelling completed].each { |v| assert_raises(RangeError) { Reaped.new(1, v) } }
    assert_raises(RangeError) { Reaped.new(1, :complete).with(terminal: :nope) }
    assert_raises(ArgumentError) { Reaped.new(1) }
  end

  def test_validation_hooks_are_private
    assert_raises(NoMethodError) { PipelineMachines.__dispatch(1, 0, 0) }
    assert_raises(NoMethodError) { PipelineMachines.__reaped(1, :complete) }
  end

  # A constructor that raises stands for anything that fails while the answer
  # is built: the job must still be there to dispatch.
  def test_a_failed_dispatch_answer_loses_no_job
    s = Stage.new([1, 1, 1, 1])
    job = s.submit(0, 7)
    other = s.submit(1, 8)
    failing(Dispatch) { assert_raises(IOError) { s.dispatch(ALL) } }
    assert_equal %i[queued queued], [s.state(job), s.state(other)]
    assert_equal Dispatch.new(job, 7, 0), s.dispatch(ALL)
    assert_equal Dispatch.new(other, 8, 1), s.dispatch(ALL)
  end

  def test_a_failed_reap_answer_keeps_the_job
    s = Stage.new([1, 1, 1, 1])
    job = s.submit(0, 7)
    assert_equal :cancelled, s.cancel(job)
    failing(Reaped) { assert_raises(IOError) { s.reap(job) } }
    assert_equal [:cancelled, 1], [s.state(job), s.size]
    assert_equal Reaped.new(7, :cancelled), s.reap(job)
    assert_predicate s, :empty?
  end

  # An answer built by code that itself changed the stage is not applied over
  # that change: the call raises and the nested work stands.
  def test_an_answer_that_changes_the_stage_is_not_applied_over_it
    s = Stage.new([1, 1, 1, 1])
    job = s.submit(0, 7)
    nested = nil
    replacing_new(Dispatch, ->(*args) { (nested = s.submit(1, 9)) && Dispatch.__new(*args) }) do
      assert_raises(RuntimeError) { s.dispatch(ALL) }
    end
    assert_equal %i[queued queued], [s.state(job), s.state(nested)]
    assert_equal 2, s.size
  end

  def test_an_answer_that_freezes_the_stage_is_not_applied
    s = Stage.new([1, 1, 1, 1])
    job = s.submit(0, 7)
    replacing_new(Dispatch, ->(*args) { s.freeze && Dispatch.__new(*args) }) do
      assert_raises(FrozenError) { s.dispatch(ALL) }
    end
    assert_equal [:queued, 1], [s.state(job), s.size]
    t = Stage.new([1, 1, 1, 1])
    done = t.submit(0, 7)
    assert_equal :cancelled, t.cancel(done)
    replacing_new(Reaped, ->(*args) { t.freeze && Reaped.__new(*args) }) do
      assert_raises(FrozenError) { t.reap(done) }
    end
    assert_equal [:cancelled, 1], [t.state(done), t.size]
  end

  def test_a_reap_answer_that_re_enters_is_not_applied
    s = Stage.new([1, 1, 1, 1])
    job = s.submit(0, 7)
    assert_equal :cancelled, s.cancel(job)
    replacing_new(Reaped, ->(*args) { s.submit(1, 9) && Reaped.__new(*args) }) do
      assert_raises(RuntimeError) { s.reap(job) }
    end
    assert_equal [:cancelled, 2], [s.state(job), s.size]
  end

  # The classes are resolved once at load: swapping a constant later changes
  # nothing the stage builds.
  def test_value_classes_are_resolved_at_load
    s = Stage.new([1, 1, 1, 1])
    s.submit(0, 7)
    GC.compact # the resolved classes are pinned
    swapping_const(:Dispatch, Data.define(:id, :payload, :lane)) do
      assert_instance_of Dispatch, s.dispatch(ALL)
    end
  end

  def test_values_are_sealed
    [Dispatch, Reaped].each do |value|
      assert_raises(NoMethodError) { value.allocate }
      assert_raises(TypeError) { Class.new(value) { def initialize(**) = nil } }
    end
    # A named subclass raises too, but Ruby binds the constant before
    # `inherited` runs, so the constant leaks; the stage never reads one.
    assert_raises(TypeError) { self.class.class_eval('class LeakedReaped < PipelineMachines::Reaped; end') }
    assert self.class.const_defined?(:LeakedReaped, false)
  end

  private

  def swapping_const(name, replacement)
    original = PipelineMachines.send(:remove_const, name)
    PipelineMachines.const_set(name, replacement)
    yield
  ensure
    PipelineMachines.send(:remove_const, name)
    PipelineMachines.const_set(name, original)
  end

  def failing(klass, &)
    replacing_new(klass, ->(*) { raise IOError, 'the answer could not be built' }, &)
  end

  def replacing_new(klass, replacement)
    singleton = klass.singleton_class
    singleton.alias_method(:__new, :new)
    klass.define_singleton_method(:new, &replacement)
    yield
  ensure
    singleton.remove_method(:new)
    singleton.alias_method(:new, :__new)
    singleton.remove_method(:__new)
  end
end
