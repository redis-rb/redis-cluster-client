# frozen_string_literal: true

require 'redis_client'

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
          return if @worker.alive?

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
        @state_list = []
        @state_dict = {}
      end

      def call(*args, **kwargs)
        _call(@command_builder.generate(args, kwargs))
      end

      def call_v(command)
        _call(@command_builder.generate(command))
      end

      def close
        @state_list.each(&:close)
        @state_list.clear
        @state_dict.clear
      end

      def next_event(timeout = nil)
        return if @state_list.empty?

        max_duration = calc_max_duration(timeout)
        starting = obtain_current_time
        loop do
          break if max_duration > 0 && obtain_current_time - starting > max_duration

          @state_list.shuffle!
          @state_list.each do |pubsub|
            message = pubsub.take_message(timeout)
            return message if message
          end

          sleep 0.001
        end
      end

      private

      def _call(command)
        node_key = @router.find_node_key(command)
        try_call(node_key, command)
      end

      def try_call(node_key, command, retry_count: 1)
        add_state(node_key).call(command)
      rescue ::RedisClient::CommandError => e
        raise if !e.message.start_with?('MOVED') || retry_count <= 0

        # for sharded pub/sub
        node_key = e.message.split[2]
        retry_count -= 1
        retry
      end

      def add_state(node_key)
        return @state_dict[node_key] if @state_dict.key?(node_key)

        state = State.new(@router.find_node(node_key).pubsub)
        @state_list << state
        @state_dict[node_key] = state
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
