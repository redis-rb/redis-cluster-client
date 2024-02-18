# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/transaction'

class RedisClient
  class Cluster
    class OptimisticLocking
      def initialize(router)
        @router = router
      end

      def watch(keys)
        ensure_safe_keys(keys)
        node = find_node(keys)
        cnt = 0 # We assume redirects occurred when incrementing it.

        @router.handle_redirection(node, retry_count: 1) do |nd|
          cnt += 1
          nd.with do |c|
            c.call('WATCH', *keys)
            reply = yield(c, cnt > 1)
            c.call('UNWATCH')
            reply
          end
        end
      end

      private

      def ensure_safe_keys(keys)
        return if safe?(keys)

        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "unsafe watch: #{keys.join(' ')}"
      end

      def safe?(keys)
        return false if keys.empty?

        slots = keys.map do |k|
          return false if k.nil? || k.empty?

          ::RedisClient::Cluster::KeySlotConverter.convert(k)
        end

        slots.uniq.size == 1
      end

      def find_node(keys)
        node_key = @router.find_primary_node_key(['WATCH', *keys])
        return @router.find_node(node_key) unless node_key.nil?

        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "couldn't determine the node"
      end
    end
  end
end
