# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/transaction'

class RedisClient
  class Cluster
    class OptimisticLocking
      def initialize(keys, router)
        @node = find_node!(keys, router)
        @keys = keys
      end

      def watch
        @node.with do |c|
          c.call('WATCH', *@keys)
          reply = yield(c)
          c.call('UNWATCH')
          reply
        end
      end

      private

      def find_node!(keys, router)
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "unsafe watch: #{keys.join(' ')}" unless safe?(keys)

        node_key = router.find_primary_node_key(['WATCH', *keys])
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "couldn't determine the node" if node_key.nil?

        router.find_node(node_key)
      end

      def safe?(keys)
        return false if keys.empty?

        slots = keys.map do |k|
          return false if k.nil? || k.empty?

          ::RedisClient::Cluster::KeySlotConverter.convert(k)
        end

        slots.uniq.size == 1
      end
    end
  end
end
