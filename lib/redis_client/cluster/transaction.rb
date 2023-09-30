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
      end

      def call(*command, **kwargs, &_)
        command = @command_builder.generate(command, kwargs)
        ensure_node_key(command)
      end

      def call_v(command, &_)
        command = @command_builder.generate(command)
        ensure_node_key(command)
      end

      def call_once(*command, **kwargs, &_)
        command = @command_builder.generate(command, kwargs)
        ensure_node_key(command)
      end

      def call_once_v(command, &_)
        command = @command_builder.generate(command)
        ensure_node_key(command)
      end

      def find_node
        yield self
        raise ArgumentError, 'empty transaction' if @node_key.nil?

        @router.find_node(@node_key)
      end

      private

      def ensure_node_key(command)
        node_key = @router.find_primary_node_key(command)
        raise ConsistencyError, "Client couldn't determine the node to be executed the transaction by: #{command}" if node_key.nil?

        @node_key ||= node_key
        raise ConsistencyError, "The transaction should be done for single node: #{@node_key}, #{node_key}" if node_key != @node_key

        nil
      end
    end
  end
end
