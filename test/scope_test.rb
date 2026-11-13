# frozen_string_literal: true

require 'minitest/autorun'
require 'pipeline_machines'

class ScopeTest < Minitest::Test
  Stage = PipelineMachines::Stage
  WEIGHTS = [1, 1, 1, 1].freeze

  def test_ids_carry_the_stages_scope
    assert_equal 0, Stage.new(WEIGHTS).scope
    s = Stage.with_scope(WEIGHTS, 7)
    assert_equal [7, 0], [s.scope, s.issued]
    id = s.submit(0, 1)
    assert_equal (7 << 48) | 1, id
    assert_equal 7, PipelineMachines.scope_of(id)
    assert_equal 0, PipelineMachines.scope_of(Stage.new(WEIGHTS).submit(0, 1))
    assert_equal 1, s.issued
  end

  def test_an_id_from_another_scope_is_stale_everywhere
    a = Stage.with_scope(WEIGHTS, 1)
    b = Stage.with_scope(WEIGHTS, 2)
    foreign = a.submit(0, 7)
    mine = b.submit(0, 7)
    b.dispatch([true] * 4)
    [foreign, 0, (2 << 48) | 2].each do |id|
      %i[cancel complete confirm_cancel reap].each { |m| assert_equal :stale, b.public_send(m, id) }
      assert_equal :stale, b.join([mine, id])
      assert_nil b.state(id)
    end
    assert_equal :running, b.state(mine)
    assert_equal :queued, a.state(foreign)
  end

  def test_resumed_continues_the_sequence_and_refuses_earlier_ids
    s = Stage.with_scope(WEIGHTS, 5)
    before = s.submit(0, 1)
    resumed = Stage.resumed(WEIGHTS, 5, s.issued)
    assert_equal [5, 1, 0], [resumed.scope, resumed.issued, resumed.size]
    assert_equal :stale, resumed.cancel(before)
    after = resumed.submit(0, 1)
    assert_equal before + 1, after
    assert_equal :stale, resumed.cancel(before)
  end

  def test_a_spent_scope_is_exhausted_and_a_fan_out_takes_all_its_ids_or_none
    s = Stage.resumed(WEIGHTS, 1, (2**48) - 2)
    assert_equal :exhausted, s.fan_out([0, 1], 1)
    assert_equal [0, (2**48) - 2], [s.size, s.issued]
    last = s.submit(0, 1)
    assert_equal (1 << 48) | ((2**48) - 1), last
    assert_equal :exhausted, s.submit(0, 1)
    assert_equal :exhausted, s.fan_out([1], 1)
    assert_equal :cancelled, s.cancel(last)
  end

  def test_scope_and_issued_validation
    [1.5, '1', nil].each do |bad|
      assert_raises(TypeError) { Stage.with_scope(WEIGHTS, bad) }
      assert_raises(TypeError) { Stage.resumed(WEIGHTS, bad, 0) }
      assert_raises(TypeError) { Stage.resumed(WEIGHTS, 0, bad) }
      assert_raises(TypeError) { PipelineMachines.scope_of(bad) }
    end
    [-1, 65_536].each do |bad|
      assert_raises(RangeError) { Stage.with_scope(WEIGHTS, bad) }
      assert_raises(RangeError) { Stage.resumed(WEIGHTS, bad, 0) }
    end
    assert_raises(RangeError) { Stage.resumed(WEIGHTS, 0, 2**48) }
    assert_raises(TypeError) { Stage.with_scope([1], 0) }
    assert_raises(RangeError) { Stage.resumed([0, 1, 1, 1], 0, 0) }
    assert_equal 65_535, Stage.with_scope(WEIGHTS, 65_535).scope
  end
end
