# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/transaction'

class RedisClient
  class Cluster
    class OptimisticLocking
      def initialize(router)
        @router = router
      end

      def watch(keys)
        slot = find_slot(keys)
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "unsafe watch: #{keys.join(' ')}" if slot.nil?

        node = @router.find_primary_node_by_slot(slot)
        @router.handle_redirection(node, retry_count: 1) do |nd|
          nd.with do |c|
            c.call('WATCH', *keys)
            yield(c, slot)
          rescue StandardError
            c.call('UNWATCH')
            raise
          end
        end
      end

      private

      def find_slot(keys)
        return if keys.empty?
        return if keys.any? { |k| k.nil? || k.empty? }

        slots = keys.map { |k| @router.find_slot_by_key(k) }
        return if slots.uniq.size != 1

        slots.first
      end
    end
  end
end
