# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/normalized_cmd_name'

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
        rescue ::RedisClient::ConnectionError
          # ignore
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

      private_constant :BUF_SIZE

      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @queue = SizedQueue.new(BUF_SIZE)
        @state_dict = {}
        @commands = []
      end

      def call(*args, **kwargs)
        command = @command_builder.generate(args, kwargs)
        _call(command)
        @commands << command
        nil
      end

      def call_v(command)
        command = @command_builder.generate(command)
        _call(command)
        @commands << command
        nil
      end

      def close
        @state_dict.each_value(&:close)
        @state_dict.clear
        @commands.clear
        @queue.clear
        @queue.close
        nil
      end

      def next_event(timeout = nil) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
        @state_dict.each_value(&:ensure_worker)
        max_duration = calc_max_duration(timeout)
        starting = obtain_current_time

        loop do
          break if max_duration > 0 && obtain_current_time - starting > max_duration

          case event = @queue.pop(true)
          when ::RedisClient::CommandError
            raise event unless event.message.start_with?('MOVED', 'CLUSTERDOWN Hash slot not served')

            break start_over
          when ::RedisClient::ConnectionError
            break start_over
          when StandardError then raise event
          when Array then break event
          end
        rescue ThreadError
          sleep 0.005
        end
      end

      private

      def _call(command)
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        when 'subscribe', 'psubscribe', 'ssubscribe' then call_to_single_state(command)
        when 'unsubscribe', 'punsubscribe' then call_to_all_states(command)
        when 'sunsubscribe' then call_for_sharded_states(command)
        else call_to_single_state(command)
        end
      end

      def call_to_single_state(command)
        node_key = @router.find_node_key(command)

        handle_connection_error(node_key) do
          @state_dict[node_key] ||= State.new(@router.find_node(node_key).pubsub, @queue)
          @state_dict[node_key].call(command)
        end
      end

      def call_to_all_states(command)
        @state_dict.each do |node_key, state|
          handle_connection_error(node_key, ignore: true) do
            state.call(command)
          end
        end
      end

      def call_for_sharded_states(command)
        if command.size == 1
          call_to_all_states(command)
        else
          call_to_single_state(command)
        end
      end

      def obtain_current_time
        Process.clock_gettime(Process::CLOCK_MONOTONIC, :microsecond)
      end

      def calc_max_duration(timeout)
        timeout.nil? || timeout < 0 ? 0 : timeout * 1_000_000
      end

      def handle_connection_error(node_key, ignore: false)
        yield
      rescue ::RedisClient::ConnectionError
        @state_dict[node_key]&.close
        @state_dict.delete(node_key)
        @router.renew_cluster_state
        raise unless ignore
      end

      def start_over
        loop do
          @router.renew_cluster_state
          @state_dict.each_value(&:close)
          @state_dict.clear
          @queue.clear
          @commands.each { |command| _call(command) }
          break
        rescue ::RedisClient::ConnectionError, ::RedisClient::Cluster::NodeMightBeDown
          sleep 1.0
        end
      end
    end
  end
end
