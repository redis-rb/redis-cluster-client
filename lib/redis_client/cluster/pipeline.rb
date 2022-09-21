# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    class Pipeline
      ReplySizeError = Class.new(::RedisClient::Error)
      MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))

      def initialize(router, command_builder, seed: Random.new_seed)
        @router = router
        @command_builder = command_builder
        @grouped = {}
        @size = 0
        @seed = seed
      end

      def call(*args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        add_row(node_key, [@size, :call_v, command, block])
      end

      def call_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        add_row(node_key, [@size, :call_v, command, block])
      end

      def call_once(*args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        add_row(node_key, [@size, :call_once_v, command, block])
      end

      def call_once_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        add_row(node_key, [@size, :call_once_v, command, block])
      end

      def blocking_call(timeout, *args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        add_row(node_key, [@size, :blocking_call_v, timeout, command, block])
      end

      def blocking_call_v(timeout, args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        add_row(node_key, [@size, :blocking_call_v, timeout, command, block])
      end

      def empty?
        @size.zero?
      end

      # TODO: https://github.com/redis-rb/redis-cluster-client/issues/37 handle redirections
      def execute # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
        all_replies = errors = nil
        @grouped.each_slice(MAX_THREADS) do |chuncked_grouped|
          threads = chuncked_grouped.map do |k, v|
            Thread.new(@router, k, v) do |router, node_key, rows|
              Thread.pass
              replies = do_pipelining(router, node_key, rows)
              raise ReplySizeError, "commands: #{rows.size}, replies: #{replies.size}" if rows.size != replies.size

              Thread.current.thread_variable_set(:rows, rows)
              Thread.current.thread_variable_set(:replies, replies)
            rescue StandardError => e
              Thread.current.thread_variable_set(:node_key, node_key)
              Thread.current.thread_variable_set(:error, e)
            end
          end

          threads.each do |t|
            t.join
            if t.thread_variable?(:replies)
              all_replies ||= Array.new(@size)
              t.thread_variable_get(:rows).each_with_index { |r, i| all_replies[r.first] = t.thread_variable_get(:replies)[i] }
            elsif t.thread_variable?(:error)
              errors ||= {}
              errors[t.thread_variable_get(:node_key)] = t.thread_variable_get(:error)
            end
          end
        end

        return all_replies if errors.nil?

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end

      private

      def add_row(node_key, row)
        @grouped[node_key] = [] unless @grouped.key?(node_key)
        @grouped[node_key] << row
        @size += 1
      end

      def do_pipelining(router, node_key, rows)
        router.find_node(node_key).pipelined do |pipeline|
          rows.each do |row|
            case row.size
            when 4 then pipeline.send(row[1], row[2], &row[3])
            when 5 then pipeline.send(row[1], row[2], row[3], &row[4])
            end
          end
        end
      end
    end
  end
end
