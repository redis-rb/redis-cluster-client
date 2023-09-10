# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    class PubSub
      class State
        def initialize(client, queue)
          @client = client
          @worker = nil
          @queue = queue
        end

        def call(command)
          @client.call_v(command)
        end

        def ensure_worker
          @worker = spawn_worker(@client, @queue) unless @worker&.alive?
        end

        def close
          @worker.exit if @worker&.alive?
          @client.close
        end

        private

        def spawn_worker(client, queue)
          # Ruby VM allocates 1 MB memory as a stack for a thread.
          # It is a fixed size but we can modify the size with some environment variables.
          # So it consumes memory 1 MB multiplied a number of workers.
          Thread.new(client, queue) do |pubsub, q|
            loop do
              q << pubsub.next_event
            rescue StandardError => e
              q << e
            end
          end
        end
      end

      BUF_SIZE = Integer(ENV.fetch('REDIS_CLIENT_PUBSUB_BUF_SIZE', 1024))

      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @queue = SizedQueue.new(BUF_SIZE)
        @state_dict = {}
      end

      def call(*args, **kwargs)
        _call(@command_builder.generate(args, kwargs))
      end

      def call_v(command)
        _call(@command_builder.generate(command))
      end

      def close
        @state_dict.each_value(&:close)
        @state_dict.clear
        @queue.clear
        @queue.close
        nil
      end

      def next_event(timeout = nil)
        @state_dict.each_value(&:ensure_worker)
        max_duration = calc_max_duration(timeout)
        starting = obtain_current_time

        loop do
          break if max_duration > 0 && obtain_current_time - starting > max_duration

          case event = @queue.pop(true)
          when StandardError then raise event
          when Array then break event
          end
        rescue ThreadError
          sleep 0.005
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

        state = State.new(@router.find_node(node_key).pubsub, @queue)
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
