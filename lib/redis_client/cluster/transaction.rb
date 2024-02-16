# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/pipeline'

class RedisClient
  class Cluster
    class Transaction
      ConsistencyError = Class.new(::RedisClient::Error)

      def initialize(router, command_builder, watch)
        @router = router
        @command_builder = command_builder
        @watch = watch
        @pipeline = ::RedisClient::Pipeline.new(@command_builder)
        @node = nil
        @retryable = true
      end

      def call(*command, **kwargs, &block)
        command = @command_builder.generate(command, kwargs)
        prepare_once(command)
        @pipeline.call_v(command, &block)
      end

      def call_v(command, &block)
        command = @command_builder.generate(command)
        prepare_once(command)
        @pipeline.call_v(command, &block)
      end

      def call_once(*command, **kwargs, &block)
        @retryable = false
        command = @command_builder.generate(command, kwargs)
        prepare_once(command)
        @pipeline.call_once_v(command, &block)
      end

      def call_once_v(command, &block)
        @retryable = false
        command = @command_builder.generate(command)
        prepare_once(command)
        @pipeline.call_once_v(command, &block)
      end

      def execute
        case @node
        when ::RedisClient then send_pipeline(@node, @pipeline, @watch, @retryable)
        when ::RedisClient::Pooled then @node.with { |node| send_pipeline(node, @pipeline, @watch, @retryable) }
        when nil then raise ArgumentError, 'empty transaction'
        else raise NotImplementedError, "#{client.class.name}#multi for cluster client"
        end
      end

      private

      def prepare_once(command)
        return unless @node.nil?

        node_key = @router.find_primary_node_key(command)
        raise ConsistencyError, "cloud not find the node: #{command.join(' ')}" if node_key.nil?

        @node = @router.find_node(node_key)
        @pipeline.call('WATCH', *@watch) unless @watch.nil? || @watch.empty?
        @pipeline.call('MULTI')
      end

      def send_pipeline(client, pipeline, watch, retryable)
        pipeline.call('EXEC')
        pipeline.call('UNWATCH', *watch) unless watch.nil? || watch.empty?

        results = client.ensure_connected_cluster_scoped(retryable: retryable) do |connection|
          commands = pipeline._commands
          client.middlewares.call_pipelined(commands, client.config) do
            connection.call_pipelined(commands, nil)
          rescue ::RedisClient::CommandError => e
            handle_command_error!(commands, e)
          end
        end

        pipeline._coerce!(results)
        idx = watch.nil? || watch.empty? ? -1 : -2
        results[idx]
      end

      def handle_command_error!(commands, err)
        if err.message.start_with?('CROSSSLOT')
          raise ConsistencyError, err.message
        elsif err.message.start_with?('MOVED', 'ASK')
          handle_redirection(commands, err)
        else
          raise err
        end
      end

      def handle_redirection(commands, err)
        ensure_the_same_node!(commands)

        # TODO: fix the handling
        raise ConsistencyError, err.message
      end

      def ensure_the_same_node!(commands)
        commands.each do |command|
          node_key = @router.find_primary_node_key(command)
          next if node_key.nil?

          node = @router.find_node(node_key)
          raise ConsistencyError, 'the transaction should be executed to the same slot in the same node' if @node != node
        end
      end
    end
  end
end
