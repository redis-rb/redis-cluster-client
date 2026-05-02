# frozen_string_literal: true

# This is a pure unit test for Cluster::Pipeline that does NOT require a
# running Redis. It exercises per-command block application on both redirected
# and non-redirected replies, and the deferred-raise behavior of
# `finalize_redirected_replies` (the path Pipeline#execute takes after
# work_group.each has classified each node-batch).
require 'minitest/autorun'
require 'redis-cluster-client'

class RedisClient
  class Cluster
    class TestPipeline < ::Minitest::Test
      DummyError = Class.new(::RedisClient::Error)

      # A minimal stub that satisfies the parts of Cluster::Router used by
      # Cluster::Pipeline#execute and its private helpers.
      class StubRouter
        attr_accessor :send_command_to_node_results

        def initialize(node_keys_for_commands:)
          @node_keys_for_commands = node_keys_for_commands
          @nodes_by_key = {}
          @send_command_to_node_calls = 0
          @send_command_to_node_results = []
        end

        def stub_node(node_key, node)
          @nodes_by_key[node_key] = node
        end

        def find_node_key(command, seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          @node_keys_for_commands.fetch(command)
        end

        def find_node(node_key)
          @nodes_by_key.fetch(node_key)
        end

        def assign_redirection_node(_err_msg)
          :redirected_node
        end

        def assign_asking_node(_err_msg)
          :asking_node
        end

        def renew_cluster_state; end

        def config
          nil
        end

        def send_command_to_node(_node, _method, _command, _args)
          raise 'no more stubbed redirect responses' if @send_command_to_node_calls >= @send_command_to_node_results.size

          v = @send_command_to_node_results[@send_command_to_node_calls]
          @send_command_to_node_calls += 1
          v
        end
      end

      def setup
        @worker = ::RedisClient::Cluster::ConcurrentWorker.create(model: :none)
        @pipeline = ::RedisClient::Cluster::Pipeline.new(
          nil, # router
          ::RedisClient::Cluster::NoopCommandBuilder,
          nil, # concurrent_worker
          exception: true
        )
      end

      def teardown
        @worker&.close
      end

      def test_blocks_applied_to_non_redirected_replies_when_redirection_occurs
        # Two commands routed to the same node. The first reply is normal; the
        # second triggers MOVED. Only the redirected one used to receive the
        # block; both should now.
        get_cmd = %w[GET k1]
        set_cmd = %w[SET movedkey x]
        node_key = 'node-a'

        router = StubRouter.new(
          node_keys_for_commands: {
            get_cmd => node_key,
            set_cmd => node_key
          }
        )
        router.stub_node(node_key, :stub_client)
        # The redirected command's value after redirection.
        router.send_command_to_node_results = ['OK']

        pipeline = ::RedisClient::Cluster::Pipeline.new(
          router,
          ::RedisClient::Cluster::NoopCommandBuilder,
          @worker,
          exception: true
        )
        pipeline.call_v(get_cmd) { |v| "block(#{v})" }
        pipeline.call_v(set_cmd) { |v| "block(#{v})" }

        # Stub do_pipelining to simulate the connection raising RedirectionNeeded
        # before _coerce! runs in send_pipeline.
        pipeline.define_singleton_method(:do_pipelining) do |_cli, pl|
          err = ::RedisClient::Cluster::Pipeline::RedirectionNeeded.new
          # First reply is the GET reply (raw), second is the MOVED error.
          err.replies = [
            'v1',
            ::RedisClient::CommandError.new('MOVED 1234 node-b:6379').tap { |e| e._set_command(pl._commands[1]) }
          ]
          err.indices = [1]
          err.first_exception = nil
          raise err
        end

        result = pipeline.execute

        assert_equal(2, result.size, 'should return one entry per outer command')
        assert_equal('block(v1)', result[0], 'block must be applied to non-redirected reply')
        assert_equal('block(OK)', result[1], 'block must be applied to redirected reply')
      end

      def test_blocks_applied_to_replies_on_stale_cluster_state
        get_cmd_a = %w[GET k1]
        get_cmd_b = %w[GET k2]
        node_key = 'node-a'

        router = StubRouter.new(
          node_keys_for_commands: {
            get_cmd_a => node_key,
            get_cmd_b => node_key
          }
        )
        router.stub_node(node_key, :stub_client)

        pipeline = ::RedisClient::Cluster::Pipeline.new(
          router,
          ::RedisClient::Cluster::NoopCommandBuilder,
          @worker,
          exception: false
        )
        pipeline.call_v(get_cmd_a) { |v| "block(#{v})" }
        pipeline.call_v(get_cmd_b) { |v| "block(#{v})" }

        pipeline.define_singleton_method(:do_pipelining) do |_cli, _pl|
          err = ::RedisClient::Cluster::Pipeline::StaleClusterState.new
          err.replies = %w[v1 v2]
          err.first_exception = nil
          raise err
        end

        result = pipeline.execute

        assert_equal(['block(v1)', 'block(v2)'], result, 'blocks must run on stale-cluster-state replies')
      end

      def test_no_double_block_application_for_redirected_replies
        # Verify that blocks are not invoked twice on the redirected reply
        # (regression guard for the redirect_command change).
        get_cmd = %w[GET k1]
        set_cmd = %w[SET movedkey x]
        node_key = 'node-a'

        router = StubRouter.new(
          node_keys_for_commands: {
            get_cmd => node_key,
            set_cmd => node_key
          }
        )
        router.stub_node(node_key, :stub_client)
        router.send_command_to_node_results = ['OK']

        pipeline = ::RedisClient::Cluster::Pipeline.new(
          router,
          ::RedisClient::Cluster::NoopCommandBuilder,
          @worker,
          exception: true
        )

        invocations = []
        block = lambda do |v|
          invocations << v
          "block(#{v})"
        end

        pipeline.call_v(get_cmd, &block)
        pipeline.call_v(set_cmd, &block)

        pipeline.define_singleton_method(:do_pipelining) do |_cli, pl|
          err = ::RedisClient::Cluster::Pipeline::RedirectionNeeded.new
          err.replies = [
            'v1',
            ::RedisClient::CommandError.new('MOVED 1234 node-b:6379').tap { |e| e._set_command(pl._commands[1]) }
          ]
          err.indices = [1]
          err.first_exception = nil
          raise err
        end

        pipeline.execute

        assert_equal(%w[v1 OK], invocations, 'block must be applied exactly once per command')
      end

      def test_finalize_redirected_replies_processes_all_batches_when_one_node_errors
        # Two redirection batches: the first carries a first_exception, the
        # second is a clean redirection. Without the deferred-raise fix, the
        # second batch's redirection handler would never run.
        node_key_a = '127.0.0.1:6379'
        node_key_b = '127.0.0.1:6380'

        pipeline_a = build_extended_pipeline(outer_indices: [0])
        pipeline_b = build_extended_pipeline(outer_indices: [1])
        @pipeline.instance_variable_set(:@pipelines, node_key_a => pipeline_a, node_key_b => pipeline_b)
        @pipeline.instance_variable_set(:@size, 2)

        first_error = DummyError.new('non-MOVED error from node A')
        batch_a = build_redirection(replies: [first_error], indices: [], first_exception: first_error)

        moved_error = ::RedisClient::CommandError.new('MOVED 0 127.0.0.1:6381')
        batch_b = build_redirection(replies: [moved_error], indices: [0], first_exception: nil)

        processed = []
        @pipeline.define_singleton_method(:process_redirection_batch) do |node_key, _batch, all_replies|
          processed << node_key
          all_replies[1] = :handled if node_key == node_key_b
          all_replies
        end

        required_redirections = { node_key_a => batch_a, node_key_b => batch_b }

        raised = assert_raises(DummyError) do
          @pipeline.send(:finalize_redirected_replies, required_redirections, nil, nil)
        end

        assert_same(first_error, raised, 'expected the first encountered exception to surface')
        assert_equal([node_key_b], processed, 'expected the non-erroring batch to still be processed')
      end

      def test_finalize_redirected_replies_processes_cluster_state_batches_when_one_node_errors
        node_key_a = '127.0.0.1:6379'
        node_key_b = '127.0.0.1:6380'

        pipeline_a = build_extended_pipeline(outer_indices: [0])
        pipeline_b = build_extended_pipeline(outer_indices: [1])
        @pipeline.instance_variable_set(:@pipelines, node_key_a => pipeline_a, node_key_b => pipeline_b)
        @pipeline.instance_variable_set(:@size, 2)

        first_error = DummyError.new('error from node A')
        batch_a = build_cluster_state(replies: [first_error], first_exception: first_error)
        batch_b = build_cluster_state(replies: ['OK'], first_exception: nil)

        processed = []
        @pipeline.define_singleton_method(:process_cluster_state_batch) do |node_key, batch, all_replies|
          processed << node_key
          all_replies[1] = batch.replies[0]
          all_replies
        end

        cluster_state_errors = { node_key_a => batch_a, node_key_b => batch_b }

        raised = assert_raises(DummyError) do
          @pipeline.send(:finalize_redirected_replies, nil, cluster_state_errors, nil)
        end

        assert_same(first_error, raised)
        assert_equal([node_key_b], processed, 'expected the non-erroring cluster-state batch to still be processed')
      end

      def test_finalize_redirected_replies_returns_replies_when_no_exception
        node_key = '127.0.0.1:6379'
        pipeline = build_extended_pipeline(outer_indices: [0])
        @pipeline.instance_variable_set(:@pipelines, node_key => pipeline)
        @pipeline.instance_variable_set(:@size, 1)

        moved_error = ::RedisClient::CommandError.new('MOVED 0 127.0.0.1:6381')
        batch = build_redirection(replies: [moved_error], indices: [0], first_exception: nil)

        @pipeline.define_singleton_method(:process_redirection_batch) do |_node_key, _batch, all_replies|
          all_replies[0] = :handled
          all_replies
        end

        result = @pipeline.send(:finalize_redirected_replies, { node_key => batch }, nil, nil)
        assert_equal([:handled], result)
      end

      def test_finalize_redirected_replies_keeps_first_exception_when_multiple_batches_error
        node_key_a = '127.0.0.1:6379'
        node_key_b = '127.0.0.1:6380'

        @pipeline.instance_variable_set(
          :@pipelines,
          node_key_a => build_extended_pipeline(outer_indices: [0]),
          node_key_b => build_extended_pipeline(outer_indices: [1])
        )
        @pipeline.instance_variable_set(:@size, 2)

        first_error = DummyError.new('first')
        second_error = DummyError.new('second')
        batch_a = build_redirection(replies: [first_error], indices: [], first_exception: first_error)
        batch_b = build_redirection(replies: [second_error], indices: [], first_exception: second_error)

        raised = assert_raises(DummyError) do
          @pipeline.send(:finalize_redirected_replies, { node_key_a => batch_a, node_key_b => batch_b }, nil, nil)
        end

        assert_same(first_error, raised, 'should preserve the first encountered exception')
      end

      private

      def build_extended_pipeline(outer_indices:)
        pipeline = ::RedisClient::Cluster::Pipeline::Extended.new(::RedisClient::Cluster::NoopCommandBuilder)
        outer_indices.each { |idx| pipeline.add_outer_index(idx) }
        pipeline
      end

      def build_redirection(replies:, indices:, first_exception:)
        err = ::RedisClient::Cluster::Pipeline::RedirectionNeeded.new
        err.replies = replies
        err.indices = indices
        err.first_exception = first_exception
        err
      end

      def build_cluster_state(replies:, first_exception:)
        err = ::RedisClient::Cluster::Pipeline::StaleClusterState.new
        err.replies = replies
        err.first_exception = first_exception
        err
      end
    end
  end
end
