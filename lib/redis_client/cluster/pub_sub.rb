# frozen_string_literal: true

class RedisClient
  class Cluster
    class PubSub
      def initialize(router)
        @router = router
        @pubsub = nil
      end

      def call(*command, **kwargs)
        close
        @pubsub = @router.assign_node(*command, **kwargs).pubsub
        @pubsub.call(*command, **kwargs)
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
