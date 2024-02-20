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

        # We have not yet selected a node for this transaction, initially, which means we can handle
        # redirections freely initially (i.e. for the first WATCH call)
        node = @router.find_primary_node_by_slot(slot)
        @router.handle_redirection(node, retry_count: 1) do |nd|
          nd.with do |c|
            c.ensure_connected_cluster_scoped(retryable: false) do
              c.call('WATCH', *keys)
              begin
                yield(c, slot)
              rescue ::RedisClient::ConnectionError
                # No need to unwatch on a connection error.
                raise
              rescue StandardError
                c.call('UNWATCH')
                raise
              end
            end
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
