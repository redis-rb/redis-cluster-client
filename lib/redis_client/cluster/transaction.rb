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
        when ::RedisClient then settle(@node)
        when ::RedisClient::Pooled then @node.with { |node| settle(node) }
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
        @pipeline.call('WATCH', *@watch) if watch?
        @pipeline.call('MULTI')
      end

      def settle(client)
        @pipeline.call('EXEC')
        @pipeline.call('UNWATCH') if watch?
        send_pipeline(client)
      end

      def send_pipeline(client, redirect: true)
        results = client.ensure_connected_cluster_scoped(retryable: @retryable) do |connection|
          commands = @pipeline._commands
          client.middlewares.call_pipelined(commands, client.config) do
            connection.call_pipelined(commands, nil)
          rescue ::RedisClient::CommandError => e
            return handle_command_error!(commands, e) if redirect

            raise
          end
        end

        @pipeline._coerce!(results)
        results[watch? ? -2 : -1]
      end

      def handle_command_error!(commands, err)
        if err.message.start_with?('CROSSSLOT')
          raise ConsistencyError, err.message
        elsif err.message.start_with?('MOVED', 'ASK')
          ensure_the_same_node!(commands)
          handle_redirection(err)
        else
          raise err
        end
      end

      def ensure_the_same_node!(commands)
        commands.each do |command|
          node_key = @router.find_primary_node_key(command)
          next if node_key.nil?

          node = @router.find_node(node_key)
          next if @node == node

          raise ConsistencyError, 'the transaction should be executed to the same slot in the same node'
        end
      end

      def handle_redirection(err)
        if err.message.start_with?('MOVED')
          node = @router.assign_redirection_node(err.message)
          send_pipeline(node, redirect: false)
        elsif err.message.start_with?('ASK')
          node = @router.assign_asking_node(err.message)
          try_asking(node) ? send_pipeline(node, redirect: false) : err
        else
          raise err
        end
      end

      def try_asking(node)
        node.call('ASKING') == 'OK'
      rescue StandardError
        false
      end

      def watch?
        !@watch.nil? && !@watch.empty?
      end
    end
  end
end
