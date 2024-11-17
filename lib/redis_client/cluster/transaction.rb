# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/noop_command_builder'
require 'redis_client/cluster/pipeline'

class RedisClient
  class Cluster
    class Transaction
      ConsistencyError = Class.new(::RedisClient::Cluster::Error)

      MAX_REDIRECTION = 2
      EMPTY_ARRAY = [].freeze

      private_constant :MAX_REDIRECTION, :EMPTY_ARRAY

      def initialize(router, command_builder, node: nil, slot: nil, asking: false)
        @router = router
        @command_builder = command_builder
        @retryable = true
        @pipeline = ::RedisClient::Pipeline.new(::RedisClient::Cluster::NoopCommandBuilder)
        @pending_commands = []
        @node = node
        prepare_tx unless @node.nil?
        @watching_slot = slot
        @asking = asking
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

        return EMPTY_ARRAY if @pipeline._empty?
        raise ConsistencyError.new("couldn't determine the node: #{@pipeline._commands}").with_config(@router.config) if @node.nil?

        commit
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

      def commit
        @pipeline.call('EXEC')
        settle
      end

      def cancel
        @pipeline.call('DISCARD')
        settle
      end

      def settle
        # If we needed ASKING on the watch, we need ASKING on the multi as well.
        @node.call('ASKING') if @asking
        # Don't handle redirections at this level if we're in a watch (the watcher handles redirections
        # at the whole-transaction level.)
        send_transaction(@node, redirect: !!@watching_slot ? 0 : MAX_REDIRECTION)
      end

      def send_transaction(client, redirect:)
        case client
        when ::RedisClient then send_pipeline(client, redirect: redirect)
        when ::RedisClient::Pooled then client.with { |c| send_pipeline(c, redirect: redirect) }
        else raise NotImplementedError, "#{client.class.name}#multi for cluster client"
        end
      end

      def send_pipeline(client, redirect:) # rubocop:disable Metrics/AbcSize
        replies = client.ensure_connected_cluster_scoped(retryable: @retryable) do |connection|
          commands = @pipeline._commands
          client.middlewares.call_pipelined(commands, client.config) do
            connection.call_pipelined(commands, nil)
          rescue ::RedisClient::CommandError => e
            ensure_the_same_slot!(commands)
            return handle_command_error!(e, redirect: redirect) unless redirect.zero?

            raise
          end
        end

        return if replies.last.nil?

        coerce_results!(replies.last)
      rescue ::RedisClient::ConnectionError
        @router.renew_cluster_state if @watching_slot.nil?
        raise
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

      def handle_command_error!(err, redirect:) # rubocop:disable Metrics/AbcSize
        if err.message.start_with?('CROSSSLOT')
          raise ConsistencyError.new("#{err.message}: #{err.command}").with_config(@router.config)
        elsif err.message.start_with?('MOVED')
          node = @router.assign_redirection_node(err.message)
          send_transaction(node, redirect: redirect - 1)
        elsif err.message.start_with?('ASK')
          node = @router.assign_asking_node(err.message)
          try_asking(node) ? send_transaction(node, redirect: redirect - 1) : err
        elsif err.message.start_with?('CLUSTERDOWN')
          @router.renew_cluster_state if @watching_slot.nil?
          raise err
        else
          raise err
        end
      end

      def ensure_the_same_slot!(commands)
        slots = commands.map { |command| @router.find_slot(command) }.compact.uniq
        return if slots.size == 1 && @watching_slot.nil?
        return if slots.size == 1 && @watching_slot == slots.first

        raise ConsistencyError.new("the transaction should be executed to a slot in a node: #{commands}").with_config(@router.config)
      end

      def try_asking(node)
        node.call('ASKING') == 'OK'
      rescue StandardError
        false
      end
    end
  end
end
