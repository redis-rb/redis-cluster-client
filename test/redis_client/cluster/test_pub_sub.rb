# frozen_string_literal: true

require 'minitest/autorun'
require 'redis-cluster-client'

class RedisClient
  class Cluster
    class TestPubSub < Minitest::Test
      # Stub for the per-node pubsub client returned by `Node#pubsub`.
      # `PubSub::State#call` invokes `@client.call_v(command)`, so this is
      # the surface we need.
      class FakePubSubClient
        attr_reader :calls

        def initialize
          @calls = []
        end

        def call_v(command)
          @calls << command
        end

        def close; end
      end

      class FakeNode
        def initialize(client)
          @client = client
        end

        def pubsub
          @client
        end
      end

      # Fake router used by both routing tests and start_over tests.
      class FakeRouter
        attr_reader :calls

        def initialize(error: nil, fail_times: 0)
          @error = error
          @fail_times = fail_times
          @calls = 0
          @nodes = Hash.new { |h, k| h[k] = FakeNode.new(FakePubSubClient.new) }
        end

        def find_node_key(command)
          # Distinct keys per command kind keep call_to_single_state targeting
          # different nodes. Channel-name-based slot routing is not exercised
          # because tests inspect @commands, not @state_dict.
          case command.first.to_s.downcase
          when 'ssubscribe', 'sunsubscribe' then 'shard:0'
          else 'primary:0'
          end
        end

        def find_node(node_key)
          @nodes[node_key]
        end

        def renew_cluster_state
          @calls += 1
          raise @error if @error && @calls <= @fail_times

          nil
        end
      end

      def setup
        @command_builder = ::RedisClient::Cluster::NoopCommandBuilder
        @router = FakeRouter.new
        @pubsub = ::RedisClient::Cluster::PubSub.new(@router, @command_builder)
      end

      def commands
        @pubsub.instance_variable_get(:@commands)
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

      def test_subscribe_then_unsubscribe_prunes_commands
        @pubsub.call('subscribe', 'a', 'b', 'c')
        @pubsub.call('unsubscribe', 'b')

        assert_equal(1, commands.size)
        assert_equal(%w[subscribe a c], commands.first)
      end

      def test_unsubscribe_all_drops_subscribe
        @pubsub.call('subscribe', 'a', 'b')
        @pubsub.call('unsubscribe')

        assert_empty(commands)
      end

      def test_unsubscribe_drops_entry_when_all_channels_match
        @pubsub.call('subscribe', 'a', 'b')
        @pubsub.call('unsubscribe', 'a', 'b')

        assert_empty(commands)
      end

      def test_unsubscribe_unknown_channel_is_noop
        @pubsub.call('subscribe', 'a')
        @pubsub.call('unsubscribe', 'z')

        assert_equal(1, commands.size)
        assert_equal(%w[subscribe a], commands.first)
      end

      def test_psubscribe_independent_from_subscribe
        @pubsub.call('subscribe', 'a')
        @pubsub.call('psubscribe', 'p.*')
        @pubsub.call('unsubscribe', 'a')

        assert_equal(1, commands.size)
        assert_equal(%w[psubscribe p.*], commands.first)
      end

      def test_punsubscribe_targets_only_psubscribe_entries
        @pubsub.call('subscribe', 'a')
        @pubsub.call('psubscribe', 'p.*', 'q.*')
        @pubsub.call('punsubscribe', 'p.*')

        assert_equal(2, commands.size)
        assert_equal(%w[subscribe a], commands[0])
        assert_equal(%w[psubscribe q.*], commands[1])
      end

      def test_ssubscribe_handled
        @pubsub.call('ssubscribe', 's1', 's2')
        @pubsub.call('sunsubscribe', 's1')

        assert_equal(1, commands.size)
        assert_equal(%w[ssubscribe s2], commands.first)
      end

      def test_sunsubscribe_all_drops_ssubscribe
        @pubsub.call('ssubscribe', 's1', 's2')
        @pubsub.call('sunsubscribe')

        assert_empty(commands)
      end

      def test_unsubscribe_does_not_touch_ssubscribe
        @pubsub.call('ssubscribe', 's1')
        @pubsub.call('unsubscribe')

        assert_equal(1, commands.size)
        assert_equal(%w[ssubscribe s1], commands.first)
      end

      def test_command_name_is_case_insensitive
        @pubsub.call('SUBSCRIBE', 'a', 'b')
        @pubsub.call('UnSubScribe', 'a')

        assert_equal(1, commands.size)
        assert_equal(%w[SUBSCRIBE b], commands.first)
      end

      def test_non_pubsub_commands_are_stored
        @pubsub.call('ping')

        assert_equal(1, commands.size)
        assert_equal(%w[ping], commands.first)
      end

      def test_call_v_uses_remember
        @pubsub.call_v(%w[subscribe a b])
        @pubsub.call_v(%w[unsubscribe a])

        assert_equal(1, commands.size)
        assert_equal(%w[subscribe b], commands.first)
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
