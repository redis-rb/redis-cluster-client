# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/optimistic_locking'

class RedisClient
  class Cluster
    class TestOptimisticLocking < TestingWrapper
      # A fake router whose inner +handle_redirection+ always raises
      # +ConnectionError+ before invoking the user block. This mirrors a
      # persistent connection failure inside +Router#handle_redirection+ that
      # surfaces before the block can be reached, so +times_block_executed+
      # stays at 0 in the outer rescue. Each call increments +call_count+ so
      # the test can bound retry behaviour.
      class FakeRouter
        attr_reader :call_count

        def initialize(slot:)
          @slot = slot
          @call_count = 0
        end

        def find_slot_by_key(_key)
          @slot
        end

        def find_primary_node_by_slot(_slot)
          :fake_node
        end

        def handle_redirection(_node, _command, retry_count:)
          @call_count += 1
          # Raise before yielding so +times_block_executed+ stays 0 in the
          # caller. +retry_count+ is intentionally ignored because the bug
          # under test is in the OUTER deduction logic, not the inner retry.
          _ = retry_count
          raise ::RedisClient::ConnectionError, 'fake connection error'
        end

        def renew_cluster_state; end
      end

      def test_handle_redirection_bounds_retries_when_inner_block_never_executes
        router = FakeRouter.new(slot: 0)
        locking = ::RedisClient::Cluster::OptimisticLocking.new(router)

        assert_raises(::RedisClient::ConnectionError) do
          locking.watch(['key']) { |_c, _slot, _asking| flunk('block should never execute') }
        end

        # +retry_count+ defaults to 1 inside +OptimisticLocking#watch+. With
        # the fix, every iteration deducts at least 1 from +retry_count+, so
        # the loop terminates after at most +retry_count + 1+ attempts.
        # Without the fix this loop is infinite, so the test would hang.
        assert_operator(router.call_count, :<=, 2,
                        'Router#handle_redirection must not be invoked more than retry_count + 1 times')
        assert_operator(router.call_count, :>=, 1,
                        'Router#handle_redirection must be invoked at least once')
      end
    end
  end
end
