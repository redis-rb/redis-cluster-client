# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    class Transaction
      ConsistencyError = Class.new(::RedisClient::Error)

      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @node_key = nil
        @node = nil
        @transaction_commands = []
      end

      def call(*command, **kwargs, &_)
        command = @command_builder.generate(command, kwargs)
        ensure_node(command)
        @transaction_commands << command
        nil
      end

      def call_v(command, &_)
        command = @command_builder.generate(command)
        ensure_node(command)
        @transaction_commands << command
        nil
      end
      alias call_once call
      alias call_once_v call_v

      def execute(watch: nil, &block)
        @router.force_primary do
          if watch&.any?
            execute_with_watch(watch: watch, &block)
          else
            execute_without_watch(&block)
          end
        end
      end

      private

      def execute_with_watch(watch:)
        # Validate that all keys to be watched are on the same node
        watch.each { |key| ensure_node(['WATCH', key]) }
        # n.b - wrapping this in #try_delegate means we retry the whole transaction on failure, and also
        # get the recover of e.g. detecting moved slots.
        @router.try_delegate(@node, :with) do |conn|
          commit_result = nil
          @router.try_send(conn, :call_once_v, ['WATCH', *watch], [], retry_count: 0)
          begin
            yield self
            commit_result = @router.try_delegate(conn, :multi, retry_count: 0) do |m|
              @transaction_commands.each do |cmd|
                m.call_once_v(cmd)
              end
            end
          ensure
            # unwatch, except if we committed (it's unnescessary) or the connection is broken anyway (it won't work)
            @router.try_send(conn, :call_once_v, ['UNWATCH'], [], retry_count: 0) unless commit_result&.any? || !conn.connected?
          end
        end
      end

      def execute_without_watch
        # We don't know what node is going to be executed on yet.
        yield self
        return [] if @node.nil?

        # Now we know. Accumulate the collected commands and send them to the right connection.
        @router.try_delegate(@node, :multi) do |m|
          @transaction_commands.each do |cmd|
            m.call_v(cmd)
          end
        end
      end

      def ensure_node(command)
        node_key = @router.find_primary_node_key(command)
        raise ConsistencyError, "Client couldn't determine the node to be executed the transaction by: #{command}" if node_key.nil?

        if @node.nil?
          @node = @router.find_node(node_key)
          @node_key = node_key
        elsif @node_key != node_key
          raise ConsistencyError, "The transaction should be done for single node: #{@node_key}, #{node_key}" if node_key != @node_key
        end
        nil
      end
    end
  end
end
