# frozen_string_literal: true

class RedisClient
  class Cluster
    class PubSub
      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @pubsub = nil
      end

      def call(*args, **kwargs)
        close
        command = @command_builder.generate(args, kwargs)
        @pubsub = @router.assign_node(command).pubsub
        @pubsub.call_v(command)
      end

      def call_v(command)
        close
        command = @command_builder.generate(command)
        @pubsub = @router.assign_node(command).pubsub
        @pubsub.call_v(command)
      end

      def close
        @pubsub&.close
        @pubsub = nil
      end

      def next_event(timeout = nil)
        @pubsub&.next_event(timeout)
      end
    end
  end
end
