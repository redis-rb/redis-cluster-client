# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/transaction'

class RedisClient
  class Cluster
    class OptimisticLocking
      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @slot = nil
        @conn = nil
      end

      def watch(keys, &block)
        if @conn
          # We're already watching, and the caller wants to watch additional keys
          add_to_watch(keys)
        else
          # First call to #watch
          start_watch(keys, &block)
        end
      end

      def unwatch
        @conn.call('UNWATCH')
      end

      def multi
        transaction = ::RedisClient::Cluster::Transaction.new(
          @router, @command_builder, node: @conn, slot: @slot
        )
        yield transaction
        transaction.execute
      end

      private

      def start_watch(keys)
        @slot = find_slot(keys)
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "unsafe watch: #{keys.join(' ')}" if @slot.nil?

        node = @router.find_primary_node_by_slot(@slot)
        @router.handle_redirection(node, retry_count: 1) do |nd|
          nd.with do |c|
            @conn = c
            @conn.call('WATCH', *keys)
            yield
          rescue StandardError
            unwatch
            raise
          end
        end
      end

      def add_to_watch(keys)
        slot = find_slot(keys)
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "inconsistent watch: #{keys.join(' ')}" if slot != @slot

        @conn.call('WATCH', *keys)
      end

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
