# frozen_string_literal: true

# This is a pure unit test for Cluster::Pipeline that does NOT require a
# running Redis. It exercises the deferred-raise behavior of
# `finalize_redirected_replies` (the path Pipeline#execute takes after
# work_group.each has classified each node-batch).
require 'minitest/autorun'
require 'redis-cluster-client'

class RedisClient
  class Cluster
    class TestPipeline < ::Minitest::Test
      DummyError = Class.new(::RedisClient::Error)

      def setup
        @pipeline = ::RedisClient::Cluster::Pipeline.new(
          nil, # router
          ::RedisClient::Cluster::NoopCommandBuilder,
          nil, # concurrent_worker
          exception: true
        )
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
