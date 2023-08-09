# frozen_string_literal: true

class RedisClient
  class Cluster
    class PubSub
      MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))

      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @pubsub_states = {}
      end

      def call(*args, **kwargs)
        _call(@command_builder.generate(args, kwargs))
      end

      def call_v(command)
        _call(@command_builder.generate(command))
      end

      def close
        @pubsub_states.each_value(&:close)
        @pubsub_states.clear
      end

      def next_event(timeout = nil)
        return if @pubsub_states.empty?

        msgs = collect_messages(timeout).compact
        return msgs.first if msgs.size < 2

        msgs
      end

      private

      def _call(command)
        node_key = @router.find_node_key(command)
        pubsub = if @pubsub_states.key?(node_key)
                   @pubsub_states[node_key]
                 else
                   @pubsub_states[node_key] = @router.find_node(node_key).pubsub
                 end
        pubsub.call_v(command)
      end

      def collect_messages(timeout) # rubocop:disable Metrics/AbcSize
        @pubsub_states.each_slice(MAX_THREADS).each_with_object([]) do |chuncked_pubsub_states, acc|
          threads = chuncked_pubsub_states.map do |_, v|
            Thread.new(v) do |pubsub|
              Thread.current[:reply] = pubsub.next_event(timeout)
            rescue StandardError => e
              Thread.current[:reply] = e
            end
          end

          threads.each do |t|
            t.join
            acc << t[:reply] unless t[:reply].nil?
          end
        end
      end
    end
  end
end
