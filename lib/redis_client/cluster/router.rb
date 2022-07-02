# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/command'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/node'
require 'redis_client/cluster/node_key'

class RedisClient
  class Cluster
    class Router
      ZERO_CURSOR_FOR_SCAN = '0'

      attr_reader :node

      def initialize(config, pool: nil, **kwargs)
        @config = config.dup
        @pool = pool
        @client_kwargs = kwargs
        @node = fetch_cluster_info(@config, pool: @pool, **@client_kwargs)
        @command = ::RedisClient::Cluster::Command.load(@node)
        @command_builder = @config.command_builder
        @mutex = Mutex.new
      end

      def send_command(method, *args, **kwargs, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
        command = method == :blocking_call && args.size > 1 ? args[1..] : args
        command = @command_builder.generate!(command, kwargs)

        cmd = command.first.to_s.downcase
        case cmd
        when 'acl', 'auth', 'bgrewriteaof', 'bgsave', 'quit', 'save'
          @node.call_all(method, *args, **kwargs, &block).first
        when 'flushall', 'flushdb'
          @node.call_primaries(method, *args, **kwargs, &block).first
        when 'ping'     then @node.send_ping(method, *args, **kwargs, &block).first
        when 'wait'     then send_wait_command(method, *args, **kwargs, &block)
        when 'keys'     then @node.call_replicas(method, *args, **kwargs, &block).flatten.sort_by(&:to_s)
        when 'dbsize'   then @node.call_replicas(method, *args, **kwargs, &block).select { |e| e.is_a?(Integer) }.sum
        when 'scan'     then scan(*command, **kwargs)
        when 'lastsave' then @node.call_all(method, *args, **kwargs, &block).sort_by(&:to_i)
        when 'role'     then @node.call_all(method, *args, **kwargs, &block)
        when 'config'   then send_config_command(method, *args, **kwargs, &block)
        when 'client'   then send_client_command(method, *args, **kwargs, &block)
        when 'cluster'  then send_cluster_command(method, *args, **kwargs, &block)
        when 'readonly', 'readwrite', 'shutdown'
          raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, cmd
        when 'memory'   then send_memory_command(method, *args, **kwargs, &block)
        when 'script'   then send_script_command(method, *args, **kwargs, &block)
        when 'pubsub'   then send_pubsub_command(method, *args, **kwargs, &block)
        when 'discard', 'exec', 'multi', 'unwatch'
          raise ::RedisClient::Cluster::AmbiguousNodeError, cmd
        else
          node = assign_node(*command)
          try_send(node, method, *args, **kwargs, &block)
        end
      rescue ::RedisClient::Cluster::Node::ReloadNeeded
        update_cluster_info!
        raise ::RedisClient::Cluster::NodeMightBeDown
      rescue ::RedisClient::Cluster::ErrorCollection => e
        update_cluster_info! if e.errors.values.any? do |err|
          err.message.start_with?('CLUSTERDOWN Hash slot not served')
        end
        raise
      end

      # @see https://redis.io/topics/cluster-spec#redirection-and-resharding
      #   Redirection and resharding
      def try_send(node, method, *args, retry_count: 3, **kwargs, &block) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
        node.send(method, *args, **kwargs, &block)
      rescue ::RedisClient::CommandError => e
        raise if retry_count <= 0

        if e.message.start_with?('MOVED')
          node = assign_redirection_node(e.message)
          retry_count -= 1
          retry
        elsif e.message.start_with?('ASK')
          node = assign_asking_node(e.message)
          node.call('ASKING')
          retry_count -= 1
          retry
        elsif e.message.start_with?('CLUSTERDOWN Hash slot not served')
          update_cluster_info!
          retry_count -= 1
          retry
        else
          raise
        end
      rescue ::RedisClient::ConnectionError
        raise if retry_count <= 0

        update_cluster_info!
        retry_count -= 1
        retry
      end

      def scan(*command, **kwargs) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
        command = @command_builder.generate!(command, kwargs)

        command[1] = ZERO_CURSOR_FOR_SCAN if command.size == 1
        input_cursor = Integer(command[1])

        client_index = input_cursor % 256
        raw_cursor = input_cursor >> 8

        clients = @node.scale_reading_clients

        client = clients[client_index]
        return [ZERO_CURSOR_FOR_SCAN, []] unless client

        command[1] = raw_cursor.to_s

        result_cursor, result_keys = client.call(*command, **kwargs)
        result_cursor = Integer(result_cursor)

        client_index += 1 if result_cursor == 0

        [((result_cursor << 8) + client_index).to_s, result_keys]
      end

      def assign_node(*command, **kwargs)
        node_key = find_node_key(*command, **kwargs)
        find_node(node_key)
      end

      def find_node_key(*command, primary_only: false, **kwargs)
        command = @command_builder.generate!(command, kwargs)

        key = @command.extract_first_key(command)
        slot = key.empty? ? nil : ::RedisClient::Cluster::KeySlotConverter.convert(key)

        if @command.should_send_to_primary?(command) || primary_only
          @node.find_node_key_of_primary(slot) || @node.primary_node_keys.sample
        else
          @node.find_node_key_of_replica(slot) || @node.replica_node_keys.sample
        end
      end

      def find_node(node_key, retry_count: 3)
        @node.find_by(node_key)
      rescue ::RedisClient::Cluster::Node::ReloadNeeded
        raise ::RedieClient::Cluster::NodeMightBeDown if retry_count <= 0

        update_cluster_info!
        retry_count -= 1
        retry
      end

      def command_exists?(name)
        @command.exists?(name)
      end

      private

      def send_wait_command(method, *args, retry_count: 3, **kwargs, &block)
        @node.call_primaries(method, *args, **kwargs, &block).select { |r| r.is_a?(Integer) }.sum
      rescue ::RedisClient::Cluster::ErrorCollection => e
        raise if retry_count <= 0
        raise if e.errors.values.none? do |err|
          err.message.include?('WAIT cannot be used with replica instances')
        end

        update_cluster_info!
        retry_count -= 1
        retry
      end

      def send_config_command(method, *args, **kwargs, &block)
        command = method == :blocking_call && args.size > 1 ? args[1..] : args
        command = @command_builder.generate!(command, kwargs)

        case command[1].to_s.downcase
        when 'resetstat', 'rewrite', 'set'
          @node.call_all(method, *args, **kwargs, &block).first
        else assign_node(*command).send(method, *args, **kwargs, &block)
        end
      end

      def send_memory_command(method, *args, **kwargs, &block)
        command = method == :blocking_call && args.size > 1 ? args[1..] : args
        command = @command_builder.generate!(command, kwargs)

        case command[1].to_s.downcase
        when 'stats' then @node.call_all(method, *args, **kwargs, &block)
        when 'purge' then @node.call_all(method, *args, **kwargs, &block).first
        else assign_node(*command).send(method, *args, **kwargs, &block)
        end
      end

      def send_client_command(method, *args, **kwargs, &block)
        command = method == :blocking_call && args.size > 1 ? args[1..] : args
        command = @command_builder.generate!(command, kwargs)

        case command[1].to_s.downcase
        when 'list' then @node.call_all(method, *args, **kwargs, &block).flatten
        when 'pause', 'reply', 'setname'
          @node.call_all(method, *args, **kwargs, &block).first
        else assign_node(*command).send(method, *args, **kwargs, &block)
        end
      end

      def send_cluster_command(method, *args, **kwargs, &block) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
        command = method == :blocking_call && args.size > 1 ? args[1..] : args
        command = @command_builder.generate!(command, kwargs)
        subcommand = command[1].to_s.downcase

        case subcommand
        when 'addslots', 'delslots', 'failover', 'forget', 'meet', 'replicate',
             'reset', 'set-config-epoch', 'setslot'
          raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, ['cluster', subcommand]
        when 'saveconfig' then @node.call_all(method, *args, **kwargs, &block).first
        when 'getkeysinslot'
          raise ArgumentError, command.join(' ') if command.size != 4

          find_node(@node.find_node_key_of_replica(command[2])).send(method, *args, **kwargs, &block)
        else assign_node(*command).send(method, *args, **kwargs, &block)
        end
      end

      def send_script_command(method, *args, **kwargs, &block)
        command = method == :blocking_call && args.size > 1 ? args[1..] : args
        command = @command_builder.generate!(command, kwargs)

        case command[1].to_s.downcase
        when 'debug', 'kill'
          @node.call_all(method, *args, **kwargs, &block).first
        when 'flush', 'load'
          @node.call_primaries(method, *args, **kwargs, &block).first
        else assign_node(*command).send(method, *args, **kwargs, &block)
        end
      end

      def send_pubsub_command(method, *args, **kwargs, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        command = method == :blocking_call && args.size > 1 ? args[1..] : args
        command = @command_builder.generate!(command, kwargs)

        case command[1].to_s.downcase
        when 'channels' then @node.call_all(method, *args, **kwargs, &block).flatten.uniq.sort_by(&:to_s)
        when 'numsub'
          @node.call_all(method, *args, **kwargs, &block).reject(&:empty?).map { |e| Hash[*e] }
               .reduce({}) { |a, e| a.merge(e) { |_, v1, v2| v1 + v2 } }
        when 'numpat' then @node.call_all(method, *args, **kwargs, &block).select { |e| e.is_a?(Integer) }.sum
        else assign_node(*command).send(method, *args, **kwargs, &block)
        end
      end

      def assign_redirection_node(err_msg)
        _, slot, node_key = err_msg.split
        slot = slot.to_i
        @node.update_slot(slot, node_key)
        find_node(node_key)
      end

      def assign_asking_node(err_msg)
        _, _, node_key = err_msg.split
        find_node(node_key)
      end

      def fetch_cluster_info(config, pool: nil, **kwargs)
        node_info = ::RedisClient::Cluster::Node.load_info(config.per_node_key, **kwargs)
        node_addrs = node_info.map { |info| ::RedisClient::Cluster::NodeKey.hashify(info[:node_key]) }
        config.update_node(node_addrs)
        ::RedisClient::Cluster::Node.new(config.per_node_key,
                                         node_info: node_info, pool: pool, with_replica: config.use_replica?, **kwargs)
      end

      def update_cluster_info!
        @mutex.synchronize do
          begin
            @node.call_all(:close)
          rescue ::RedisClient::Cluster::ErrorCollection
            # ignore
          end

          @node = fetch_cluster_info(@config, pool: @pool, **@client_kwargs)
        end
      end
    end
  end
end
