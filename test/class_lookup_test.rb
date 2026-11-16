# frozen_string_literal: true

require 'minitest/autorun'
require 'pipeline_machines'
require 'open3'
require 'rbconfig'

# The class of the stage the extension wraps is fixed when it loads. Nothing
# a caller does later, redefining the constant or `Object.const_get`, reaches
# a stage being built. Each case runs in a fresh process: the class is fixed
# once per process, at load.
class ClassLookupTest < Minitest::Test
  LIB = File.expand_path('../lib', __dir__)

  PRELUDE = <<~RUBY
    require 'pipeline_machines'
    P = PipelineMachines
    weights = [1, 1, 1, 1]
  RUBY

  # Runs `body` after PRELUDE in a new Ruby; answers its last line of output.
  def isolated(body)
    out, err, status = Open3.capture3(RbConfig.ruby, '-I', LIB, '-e', PRELUDE + body)
    assert status.success?, "exited #{status.exitstatus}: #{err}"
    out.lines.last&.chomp
  end

  # Bug: a Stage constant redefined before the first stage made it an
  # instance of the new class, without any of the gem's methods.
  def test_a_redefined_stage_constant_does_not_change_the_stage_class
    assert_equal 'true true true 1 5 2', isolated(<<~RUBY)
      original = P::Stage
      P.send(:remove_const, :Stage)
      P.const_set(:Stage, Class.new)
      stages = [original.new(weights), original.with_scope(weights, 5), original.resumed(weights, 5, 2)]
      fresh, scoped, resumed = stages
      fresh.submit(0, 1)
      puts [*stages.map { |s| s.instance_of?(original) }, fresh.size, scoped.scope, resumed.issued].join(' ')
    RUBY
  end

  # Bug: building the first stage ran `Object.const_get`, a caller's code in
  # the middle of a native call.
  def test_building_a_stage_runs_no_const_get
    assert_equal '0 false Stage Stage Stage', isolated(<<~RUBY)
      hits = 0
      log = []
      Object.define_singleton_method(:const_get) do |*args|
        hits += 1
        log.freeze
        super(*args)
      end
      stages = [P::Stage.new(weights), P::Stage.with_scope(weights, 5), P::Stage.resumed(weights, 5, 2)]
      puts [hits, log.frozen?, *stages.map { |s| s.class.name.split('::').last }].join(' ')
    RUBY
  end

  # Bug: a redefined `Object.const_get` that raised hit an `unwrap` in the
  # extension: an uncatchable fatal, and the process exited.
  def test_a_raising_const_get_does_not_kill_the_process
    assert_equal '1 5 2', isolated(<<~RUBY)
      Object.define_singleton_method(:const_get) { |*| raise IOError, 'lookup ran mid-call' }
      fresh = P::Stage.new(weights)
      fresh.submit(0, 1)
      puts [fresh.size, P::Stage.with_scope(weights, 5).scope, P::Stage.resumed(weights, 5, 2).issued].join(' ')
    RUBY
  end
end
