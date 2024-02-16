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
        @retryable = true
        @pipeline = ::RedisClient::Pipeline.new(@command_builder)
        @buffer = []
        @node = nil
      end

      def call(*command, **kwargs, &block)
        command = @command_builder.generate(command, kwargs)
        if prepare(command)
          @pipeline.call_v(command, &block)
        else
          @buffer << -> { @pipeline.call_v(command, &block) }
        end
      end

      def call_v(command, &block)
        command = @command_builder.generate(command)
        if prepare(command)
          @pipeline.call_v(command, &block)
        else
          @buffer << -> { @pipeline.call_v(command, &block) }
        end
      end

      def call_once(*command, **kwargs, &block)
        @retryable = false
        command = @command_builder.generate(command, kwargs)
        if prepare(command)
          @pipeline.call_once_v(command, &block)
        else
          @buffer << -> { @pipeline.call_once_v(command, &block) }
        end
      end

      def call_once_v(command, &block)
        @retryable = false
        command = @command_builder.generate(command)
        if prepare(command)
          @pipeline.call_once_v(command, &block)
        else
          @buffer << -> { @pipeline.call_once_v(command, &block) }
        end
      end

      def execute
        @buffer.each(&:call)

        raise ArgumentError, 'empty transaction' if @pipeline._empty?
        raise ConsistencyError, "couldn't determine the node: #{@pipeline._commands}" if @node.nil?

        case @node
        when ::RedisClient then settle(@node)
        when ::RedisClient::Pooled then @node.with { |nd| settle(nd) }
        else raise NotImplementedError, "#{client.class.name}#multi for cluster client"
        end
      end

      private

      def prepare(command)
        return true unless @node.nil?

        node_key = @router.find_primary_node_key(command)
        return false if node_key.nil?

        @node = @router.find_node(node_key)
        @node.call('WATCH', *@watch) if watch?
        @pipeline.call('MULTI')
        @buffer.each(&:call)
        @buffer.clear
        true
      end

      def settle(client)
        @pipeline.call('EXEC')
        @node.call('UNWATCH') if watch?
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
        results[-1]
      end

      def handle_command_error!(commands, err)
        if err.message.start_with?('CROSSSLOT')
          raise ConsistencyError, "#{err.message}: #{err.command}"
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

          raise ConsistencyError, "the transaction should be executed to a slot in a node: #{commands}"
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
