# frozen_string_literal: true

class RedisClient
  class Cluster
    class PinningNode
      def initialize(client)
        @client = client
      end

      def call(*args, **kwargs, &block)
        @client.call(*args, **kwargs, &block)
      end

      def call_v(args, &block)
        @client.call_v(args, &block)
      end

      def call_once(*args, **kwargs, &block)
        @client.call_once(*args, **kwargs, &block)
      end

      def call_once_v(args, &block)
        @client.call_once_v(args, &block)
      end

      def blocking_call(timeout, *args, **kwargs, &block)
        @client.blocking_call(timeout, *args, **kwargs, &block)
      end

      def blocking_call_v(timeout, args, &block)
        @client.blocking_call_v(timeout, args, &block)
      end
    end
  end
end
