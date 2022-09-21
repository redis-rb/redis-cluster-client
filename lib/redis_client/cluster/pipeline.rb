# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/middlewares'
require 'redis_client/pooled'

class RedisClient
  class Cluster
    class Pipeline
      ReplySizeError = Class.new(::RedisClient::Error)
      MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))

      def initialize(router, command_builder, seed: Random.new_seed)
        @router = router
        @command_builder = command_builder
        @seed = seed
        @pipelines = {}
        @indices = {}
        @size = 0
      end

      def call(*args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).call_v(command, &block)
        index_pipeline(node_key)
      end

      def call_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).call_v(command, &block)
        index_pipeline(node_key)
      end

      def call_once(*args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).call_once_v(command, &block)
        index_pipeline(node_key)
      end

      def call_once_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).call_once_v(command, &block)
        index_pipeline(node_key)
      end

      def blocking_call(timeout, *args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).blocking_call_v(timeout, command, &block)
        index_pipeline(node_key)
      end

      def blocking_call_v(timeout, args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).blocking_call_v(timeout, command, &block)
        index_pipeline(node_key)
      end

      def empty?
        @size.zero?
      end

      # TODO: https://github.com/redis-rb/redis-cluster-client/issues/37 handle redirections
      def execute # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
        all_replies = errors = nil
        @pipelines.each_slice(MAX_THREADS) do |chuncked_pipelines|
          threads = chuncked_pipelines.map do |node_key, pipeline|
            node = @router.find_node(node_key)
            Thread.new(node, pipeline) do |nd, pl|
              Thread.pass
              Thread.current.thread_variable_set(:node_key, node_key)
              replies = do_pipelining(nd, pl)
              raise ReplySizeError, "commands: #{pipeline._size}, replies: #{replies.size}" if pipeline._size != replies.size

              Thread.current.thread_variable_set(:replies, replies)
            rescue StandardError => e
              Thread.current.thread_variable_set(:error, e)
            end
          end

          threads.each do |t|
            t.join
            if t.thread_variable?(:replies)
              all_replies ||= Array.new(@size)
              @indices[t.thread_variable_get(:node_key)].each_with_index { |gi, i| all_replies[gi] = t.thread_variable_get(:replies)[i] }
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

      def get_pipeline(node_key)
        @pipelines[node_key] ||= ::RedisClient::Pipeline.new(@command_builder)
      end

      def index_pipeline(node_key)
        @indices[node_key] ||= []
        @indices[node_key] << @size
        @size += 1
      end

      def do_pipelining(client, pipeline)
        case client
        when ::RedisClient then send_pipeline(client, pipeline)
        when ::RedisClient::Pooled then client.with { |cli| send_pipeline(cli, pipeline) }
        else raise NotImplementedError, "#{client.class.name}#pipelined for cluster client"
        end
      end

      def send_pipeline(client, pipeline)
        results = client.send(:ensure_connected, retryable: pipeline._retryable?) do |connection|
          commands = pipeline._commands
          ::RedisClient::Middlewares.call_pipelined(commands, client.config) do
            connection.call_pipelined(commands, pipeline._timeouts)
          end
        end

        pipeline._coerce!(results)
      end
    end
  end
end
