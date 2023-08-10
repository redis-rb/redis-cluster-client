# frozen_string_literal: true

class RedisClient
  class Cluster
    class PubSub
      class State
        def initialize(client)
          @client = client
          @worker = nil
        end

        def call(command)
          @client.call_v(command)
        end

        def close
          @worker.exit if @worker&.alive?
          @client.close
        end

        def take_message(timeout)
          @worker = subscribe(@client, timeout) if @worker.nil?
          return if @worker.join(0.01).nil?

          message = @worker[:reply]
          @worker = nil
          message
        end

        private

        def subscribe(client, timeout)
          Thread.new(client, timeout) do |pubsub, to|
            Thread.current[:reply] = pubsub.next_event(to)
          rescue StandardError => e
            Thread.current[:reply] = e
          end
        end
      end

      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @states = {}
      end

      def call(*args, **kwargs)
        _call(@command_builder.generate(args, kwargs))
      end

      def call_v(command)
        _call(@command_builder.generate(command))
      end

      def close
        @states.each_value(&:close)
        @states.clear
      end

      def next_event(timeout = nil)
        return if @states.empty?

        max_duration = calc_max_duration(timeout)
        starting = obtain_current_time
        loop do
          break if max_duration > 0 && obtain_current_time - starting > max_duration

          @states.each_value do |pubsub|
            message = pubsub.take_message(timeout)
            return message if message
          end
        end
      end

      private

      def _call(command)
        node_key = @router.find_node_key(command)
        @states[node_key] = State.new(@router.find_node(node_key).pubsub) unless @states.key?(node_key)
        @states[node_key].call(command)
      end

      def obtain_current_time
        Process.clock_gettime(Process::CLOCK_MONOTONIC, :microsecond)
      end

      def calc_max_duration(timeout)
        timeout.nil? || timeout < 0 ? 0 : timeout * 1_000_000
      end
    end
  end
end
