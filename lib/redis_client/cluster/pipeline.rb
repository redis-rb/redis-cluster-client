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
        add_line(node_key, [@size, :call_v, command, block])
      end

      def call_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        add_line(node_key, [@size, :call_v, command, block])
      end

      def call_once(*args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        add_line(node_key, [@size, :call_once_v, command, block])
      end

      def call_once_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        add_line(node_key, [@size, :call_once_v, command, block])
      end

      def blocking_call(timeout, *args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        add_line(node_key, [@size, :blocking_call_v, timeout, command, block])
      end

      def blocking_call_v(timeout, args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        add_line(node_key, [@size, :blocking_call_v, timeout, command, block])
      end

      def empty?
        @size.zero?
      end

      # TODO: https://github.com/redis-rb/redis-cluster-client/issues/37 handle redirections
      def execute # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
        all_replies = Array.new(@size)
        errors = {}
        @grouped.each_slice(MAX_THREADS) do |chuncked_grouped|
          threads = chuncked_grouped.map do |k, v|
            Thread.new(@router, k, v) do |router, node_key, rows|
              Thread.pass
              replies = router.find_node(node_key).pipelined do |pipeline|
                rows.each do |(_size, *row, block)|
                  pipeline.send(*row, &block)
                end
              end

              raise ReplySizeError, "commands: #{rows.size}, replies: #{replies.size}" if rows.size != replies.size

              rows.each_with_index { |row, idx| all_replies[row.first] = replies[idx] }
            rescue StandardError => e
              errors[node_key] = e
            end
          end

          threads.each(&:join)
        end

        return all_replies if errors.empty?

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end

      private

      def add_line(node_key, line)
        @grouped[node_key] = [] unless @grouped.key?(node_key)
        @grouped[node_key] << line
        @size += 1
      end
    end
  end
end
