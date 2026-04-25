# frozen_string_literal: true

require 'minitest/autorun'
require 'redis-cluster-client'

class RedisClient
  class Cluster
    class TestPubSub < Minitest::Test
      # Minimal fake router used by start_over. Only renew_cluster_state is exercised
      # because the rest of the loop body relies on @state_dict and @commands being
      # empty (the production initial state) so no command dispatch happens.
      class FakeRouter
        attr_reader :calls

        def initialize(error: nil, fail_times: 0)
          @error = error
          @fail_times = fail_times
          @calls = 0
        end

        def renew_cluster_state
          @calls += 1
          raise @error if @error && @calls <= @fail_times

          nil
        end
      end

      def setup
        @command_builder = ::RedisClient::CommandBuilder
      end

      def test_start_over_raises_after_exhausting_attempts
        router = FakeRouter.new(error: ::RedisClient::ConnectionError.new('boom'), fail_times: Float::INFINITY)
        pubsub = ::RedisClient::Cluster::PubSub.new(router, @command_builder)
        skip_sleep(pubsub)

        max_attempts = ::RedisClient::Cluster::PubSub.const_get(:RECOVERY_MAX_ATTEMPTS, false)

        err = assert_raises(::RedisClient::ConnectionError) { pubsub.send(:start_over) }
        assert_equal('boom', err.message)
        assert_equal(max_attempts, router.calls)
      end

      def test_start_over_raises_for_node_might_be_down_after_exhausting_attempts
        router = FakeRouter.new(error: ::RedisClient::Cluster::NodeMightBeDown.new, fail_times: Float::INFINITY)
        pubsub = ::RedisClient::Cluster::PubSub.new(router, @command_builder)
        skip_sleep(pubsub)

        max_attempts = ::RedisClient::Cluster::PubSub.const_get(:RECOVERY_MAX_ATTEMPTS, false)

        assert_raises(::RedisClient::Cluster::NodeMightBeDown) { pubsub.send(:start_over) }
        assert_equal(max_attempts, router.calls)
      end

      def test_start_over_succeeds_after_transient_failure
        max_attempts = ::RedisClient::Cluster::PubSub.const_get(:RECOVERY_MAX_ATTEMPTS, false)
        fail_times = max_attempts - 1
        router = FakeRouter.new(error: ::RedisClient::ConnectionError.new('transient'), fail_times: fail_times)
        pubsub = ::RedisClient::Cluster::PubSub.new(router, @command_builder)
        skip_sleep(pubsub)

        assert_nil(pubsub.send(:start_over))
        assert_equal(fail_times + 1, router.calls)
      end

      def test_start_over_succeeds_on_first_try
        router = FakeRouter.new
        pubsub = ::RedisClient::Cluster::PubSub.new(router, @command_builder)
        skip_sleep(pubsub)

        assert_nil(pubsub.send(:start_over))
        assert_equal(1, router.calls)
      end

      def test_recovery_interval_uses_exponential_backoff_capped_by_max
        router = FakeRouter.new
        pubsub = ::RedisClient::Cluster::PubSub.new(router, @command_builder)

        base = ::RedisClient::Cluster::PubSub.const_get(:RECOVERY_BASE_INTERVAL, false)
        max = ::RedisClient::Cluster::PubSub.const_get(:RECOVERY_MAX_INTERVAL, false)

        assert_equal(base, pubsub.send(:recovery_interval, 1))
        assert_equal(base * 2, pubsub.send(:recovery_interval, 2))
        assert_equal(base * 4, pubsub.send(:recovery_interval, 3))
        assert_equal(max, pubsub.send(:recovery_interval, 64))
      end

      private

      def skip_sleep(pubsub)
        # Avoid real sleeping in tests. Backoff math is verified by
        # test_recovery_interval_uses_exponential_backoff_capped_by_max.
        pubsub.define_singleton_method(:sleep) { |_seconds| nil }
      end
    end
  end
end
