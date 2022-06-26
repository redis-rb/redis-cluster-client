# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    class Pipeline
      ReplySizeError = Class.new(::RedisClient::Error)

      def initialize(router)
        @router = router
        @grouped = Hash.new([].freeze)
        @size = 0
      end

      def call(*command, **kwargs)
        node_key = @router.find_node_key(*command, primary_only: true)
        @grouped[node_key] += [[@size, :call, command, kwargs]]
        @size += 1
      end

      def call_once(*command, **kwargs)
        node_key = @router.find_node_key(*command, primary_only: true)
        @grouped[node_key] += [[@size, :call_once, command, kwargs]]
        @size += 1
      end

      def blocking_call(timeout, *command, **kwargs)
        node_key = @router.find_node_key(*command, primary_only: true)
        @grouped[node_key] += [[@size, :blocking_call, timeout, command, kwargs]]
        @size += 1
      end

      def empty?
        @size.zero?
      end

      # TODO: https://github.com/redis-rb/redis-cluster-client/issues/37 handle redirections
      def execute # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
        all_replies = Array.new(@size)
        errors = {}
        threads = @grouped.map do |k, v|
          Thread.new(@router, k, v) do |router, node_key, rows|
            Thread.pass
            replies = router.find_node(node_key).pipelined do |pipeline|
              rows.each do |row|
                case row[1]
                when :call then pipeline.call(*row[2], **row[3])
                when :call_once then pipeline.call_once(*row[2], **row[3])
                when :blocking_call then pipeline.blocking_call(row[2], *row[3], **row[4])
                else raise NotImplementedError, row[1]
                end
              end
            end

            raise ReplySizeError, "commands: #{rows.size}, replies: #{replies.size}" if rows.size != replies.size

            rows.each_with_index { |row, idx| all_replies[row.first] = replies[idx] }
          rescue StandardError => e
            errors[node_key] = e
          end
        end

        threads.each(&:join)
        return all_replies if errors.empty?

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end
    end
  end
end
