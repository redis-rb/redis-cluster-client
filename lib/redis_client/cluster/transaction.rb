# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/pipeline'
require 'redis_client/cluster/node_key'

class RedisClient
  class Cluster
    class Transaction
      ConsistencyError = Class.new(::RedisClient::Error)

      def initialize(router, command_builder, node = nil)
        @router = router
        @command_builder = command_builder
        @retryable = true
        @pipeline = ::RedisClient::Pipeline.new(@command_builder)
        @pending_commands = []
        @node = node
        prepare_tx unless @node.nil?
      end

      def call(*command, **kwargs, &block)
        command = @command_builder.generate(command, kwargs)
        if prepare(command)
          @pipeline.call_v(command, &block)
        else
          defer { @pipeline.call_v(command, &block) }
        end
      end

      def call_v(command, &block)
        command = @command_builder.generate(command)
        if prepare(command)
          @pipeline.call_v(command, &block)
        else
          defer { @pipeline.call_v(command, &block) }
        end
      end

      def call_once(*command, **kwargs, &block)
        @retryable = false
        command = @command_builder.generate(command, kwargs)
        if prepare(command)
          @pipeline.call_once_v(command, &block)
        else
          defer { @pipeline.call_once_v(command, &block) }
        end
      end

      def call_once_v(command, &block)
        @retryable = false
        command = @command_builder.generate(command)
        if prepare(command)
          @pipeline.call_once_v(command, &block)
        else
          defer { @pipeline.call_once_v(command, &block) }
        end
      end

      def execute
        @pending_commands.each(&:call)

        raise ArgumentError, 'empty transaction' if @pipeline._empty?
        raise ConsistencyError, "couldn't determine the node: #{@pipeline._commands}" if @node.nil?

        settle
      end

      private

      def defer(&block)
        @pending_commands << block
        nil
      end

      def prepare(command)
        return true unless @node.nil?

        node_key = @router.find_primary_node_key(command)
        return false if node_key.nil?

        @node = @router.find_node(node_key)
        prepare_tx
        true
      end

      def prepare_tx
        @pipeline.call('MULTI')
        @pending_commands.each(&:call)
        @pending_commands.clear
      end

      def settle
        @pipeline.call('EXEC')
        send_transaction(@node, redirect: true)
      end

      def send_transaction(client, redirect:)
        case client
        when ::RedisClient then send_pipeline(client, redirect: redirect)
        when ::RedisClient::Pooled then client.with { |c| send_pipeline(c, redirect: redirect) }
        else raise NotImplementedError, "#{client.class.name}#multi for cluster client"
        end
      end

      def send_pipeline(client, redirect:)
        replies = client.ensure_connected_cluster_scoped(retryable: @retryable) do |connection|
          commands = @pipeline._commands
          client.middlewares.call_pipelined(commands, client.config) do
            connection.call_pipelined(commands, nil)
          rescue ::RedisClient::CommandError => e
            return handle_command_error!(commands, e) if redirect

            raise
          end
        end

        return if replies.last.nil?

        coerce_results!(replies.last)
      end

      def coerce_results!(results, offset: 1)
        results.each_with_index do |result, index|
          if result.is_a?(::RedisClient::CommandError)
            result._set_command(@pipeline._commands[index + offset])
            raise result
          end

          next if @pipeline._blocks.nil?

          block = @pipeline._blocks[index + offset]
          next if block.nil?

          results[index] = block.call(result)
        end

        results
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

          next if NodeKey.build_from_client(@node) == node_key

          raise ConsistencyError, "the transaction should be executed to a slot in a node: #{commands}"
        end
      end

      def handle_redirection(err)
        if err.message.start_with?('MOVED')
          node = @router.assign_redirection_node(err.message)
          send_transaction(node, redirect: false)
        elsif err.message.start_with?('ASK')
          node = @router.assign_asking_node(err.message)
          try_asking(node) ? send_transaction(node, redirect: false) : err
        else
          raise err
        end
      end

      def try_asking(node)
        node.call('ASKING') == 'OK'
      rescue StandardError
        false
      end
    end
  end
end
