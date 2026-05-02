# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/transaction'

class RedisClient
  class Cluster
    class OptimisticLocking
      def initialize(router)
        @router = router
        @asking = false
      end

      def watch(keys) # rubocop:disable Metrics/AbcSize
        slot = find_slot(keys)
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "unsafe watch: #{keys.join(' ')}" if slot.nil?

        handle_redirection(slot, retry_count: 1) do |nd|
          nd.with do |c|
            c.ensure_connected_cluster_scoped(retryable: false) do
              c.call('asking') if @asking
              c.call('watch', *keys)
              begin
                yield(c, slot, @asking)
              rescue ::RedisClient::ConnectionError
                # No need to unwatch on a connection error.
                raise
              rescue StandardError
                c.call('unwatch')
                raise
              end
            rescue ::RedisClient::CommandError => e
              @router.renew_cluster_state if e.message.start_with?('CLUSTERDOWN')
              raise
            end
          rescue ::RedisClient::ConnectionError
            @router.renew_cluster_state
            raise
          end
        end
      end

      private

      def handle_redirection(slot, retry_count: 1, &blk)
        # We have not yet selected a node for this transaction, initially, which means we can handle
        # redirections freely initially (i.e. for the first WATCH call)
        node = @router.find_primary_node_by_slot(slot)
        times_block_executed = 0
        @router.handle_redirection(node, nil, retry_count: retry_count) do |nd|
          times_block_executed += 1
          handle_asking_once(nd, &blk)
        end
      rescue ::RedisClient::ConnectionError
        # Deduct the number of retries that happened _inside_ router#handle_redirection from our remaining
        # _external_ retries. Always deduct at least one in case handle_redirection raises without trying the block.
        retry_count -= [times_block_executed, 1].min
        raise if retry_count < 0

        retry
      end

      def handle_asking_once(node)
        yield node
      rescue ::RedisClient::CommandError => e
        raise unless ErrorIdentification.client_owns_error?(e, node)
        raise unless e.message.start_with?('ASK')

        node = @router.assign_asking_node(e.message)
        @asking = true
        yield node
      ensure
        @asking = false
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
