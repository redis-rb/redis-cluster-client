# frozen_string_literal: true

require 'redis_client'
require 'redis_client/circuit_breaker'
require 'redis_client/cluster/command'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/node'
require 'redis_client/cluster/node_key'
require 'redis_client/cluster/normalized_cmd_name'

class RedisClient
  class Cluster
    class Router
      ZERO_CURSOR_FOR_SCAN = '0'
      METHODS_FOR_BLOCKING_CMD = %i[blocking_call_v blocking_call].freeze

      attr_reader :node

      def initialize(config, pool: nil, **kwargs)
        @config = config.dup
        @pool = pool
        @client_kwargs = kwargs
        @node = fetch_cluster_info(@config, pool: @pool, **@client_kwargs)
        @command = ::RedisClient::Cluster::Command.load(@node)
        @mutex = Mutex.new
        @command_builder = @config.command_builder
      end

      def send_command(method, command, *args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        cmd = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        case cmd
        when 'acl', 'auth', 'bgrewriteaof', 'bgsave', 'quit', 'save'
          @node.call_all(method, command, args, &block).first
        when 'flushall', 'flushdb'
          @node.call_primaries(method, command, args, &block).first
        when 'ping'     then @node.send_ping(method, command, args, &block).first
        when 'wait'     then send_wait_command(method, command, args, &block)
        when 'keys'     then @node.call_replicas(method, command, args, &block).flatten.sort_by(&:to_s)
        when 'dbsize'   then @node.call_replicas(method, command, args, &block).select { |e| e.is_a?(Integer) }.sum
        when 'scan'     then scan(command, seed: 1)
        when 'lastsave' then @node.call_all(method, command, args, &block).sort_by(&:to_i)
        when 'role'     then @node.call_all(method, command, args, &block)
        when 'config'   then send_config_command(method, command, args, &block)
        when 'client'   then send_client_command(method, command, args, &block)
        when 'cluster'  then send_cluster_command(method, command, args, &block)
        when 'readonly', 'readwrite', 'shutdown'
          raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, cmd
        when 'memory'   then send_memory_command(method, command, args, &block)
        when 'script'   then send_script_command(method, command, args, &block)
        when 'pubsub'   then send_pubsub_command(method, command, args, &block)
        when 'discard', 'exec', 'multi', 'unwatch'
          raise ::RedisClient::Cluster::AmbiguousNodeError, cmd
        else
          node = assign_node(command)
          try_send(node, method, command, args, &block)
        end
      rescue ::RedisClient::CircuitBreaker::OpenCircuitError
        raise
      rescue ::RedisClient::Cluster::Node::ReloadNeeded
        update_cluster_info!
        raise ::RedisClient::Cluster::NodeMightBeDown
      rescue ::RedisClient::Cluster::ErrorCollection => e
        raise if e.errors.any?(::RedisClient::CircuitBreaker::OpenCircuitError)

        update_cluster_info! if e.errors.values.any? do |err|
          err.message.start_with?('CLUSTERDOWN Hash slot not served')
        end

        raise
      end

      # @see https://redis.io/docs/reference/cluster-spec/#redirection-and-resharding Redirection and resharding
      def try_send(node, method, command, args, retry_count: 3, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        if args.empty?
          # prevent memory allocation for variable-length args
          node.public_send(method, command, &block)
        else
          node.public_send(method, *args, command, &block)
        end
      rescue ::RedisClient::CircuitBreaker::OpenCircuitError
        raise
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
      rescue ::RedisClient::ConnectionError => e
        raise if METHODS_FOR_BLOCKING_CMD.include?(method) && e.is_a?(RedisClient::ReadTimeoutError)
        raise if retry_count <= 0

        update_cluster_info!
        retry_count -= 1
        retry
      end

      def try_delegate(node, method, *args, retry_count: 3, **kwargs, &block) # rubocop:disable Metrics/AbcSize
        node.public_send(method, *args, **kwargs, &block)
      rescue ::RedisClient::CircuitBreaker::OpenCircuitError
        raise
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

      def scan(*command, seed: nil, **kwargs) # rubocop:disable Metrics/AbcSize
        command = @command_builder.generate(command, kwargs)

        command[1] = ZERO_CURSOR_FOR_SCAN if command.size == 1
        input_cursor = Integer(command[1])

        client_index = input_cursor % 256
        raw_cursor = input_cursor >> 8

        clients = @node.clients_for_scanning(seed: seed)

        client = clients[client_index]
        return [ZERO_CURSOR_FOR_SCAN, []] unless client

        command[1] = raw_cursor.to_s

        result_cursor, result_keys = client.call_v(command)
        result_cursor = Integer(result_cursor)

        client_index += 1 if result_cursor == 0

        [((result_cursor << 8) + client_index).to_s, result_keys]
      end

      def assign_node(command)
        node_key = find_node_key(command)
        find_node(node_key)
      end

      def find_node_key(command, seed: nil)
        key = @command.extract_first_key(command)
        slot = key.empty? ? nil : ::RedisClient::Cluster::KeySlotConverter.convert(key)

        if @command.should_send_to_primary?(command)
          @node.find_node_key_of_primary(slot) || @node.any_primary_node_key(seed: seed)
        else
          @node.find_node_key_of_replica(slot, seed: seed) || @node.any_replica_node_key(seed: seed)
        end
      end

      def find_node(node_key, retry_count: 3)
        @node.find_by(node_key)
      rescue ::RedisClient::Cluster::Node::ReloadNeeded
        raise ::RedisClient::Cluster::NodeMightBeDown if retry_count <= 0

        update_cluster_info!
        retry_count -= 1
        retry
      end

      def command_exists?(name)
        @command.exists?(name)
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

      private

      def send_wait_command(method, command, args, retry_count: 3, &block) # rubocop:disable Metrics/AbcSize
        @node.call_primaries(method, command, args, &block).select { |r| r.is_a?(Integer) }.sum
      rescue ::RedisClient::Cluster::ErrorCollection => e
        raise if e.errors.any?(::RedisClient::CircuitBreaker::OpenCircuitError)
        raise if retry_count <= 0
        raise if e.errors.values.none? do |err|
          err.message.include?('WAIT cannot be used with replica instances')
        end

        update_cluster_info!
        retry_count -= 1
        retry
      end

      def send_config_command(method, command, args, &block)
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'resetstat', 'rewrite', 'set'
          @node.call_all(method, command, args, &block).first
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_memory_command(method, command, args, &block)
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'stats' then @node.call_all(method, command, args, &block)
        when 'purge' then @node.call_all(method, command, args, &block).first
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_client_command(method, command, args, &block)
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'list' then @node.call_all(method, command, args, &block).flatten
        when 'pause', 'reply', 'setname'
          @node.call_all(method, command, args, &block).first
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_cluster_command(method, command, args, &block)
        case subcommand = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'addslots', 'delslots', 'failover', 'forget', 'meet', 'replicate',
             'reset', 'set-config-epoch', 'setslot'
          raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, ['cluster', subcommand]
        when 'saveconfig' then @node.call_all(method, command, args, &block).first
        when 'getkeysinslot'
          raise ArgumentError, command.join(' ') if command.size != 4

          find_node(@node.find_node_key_of_replica(command[2])).public_send(method, *args, command, &block)
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_script_command(method, command, args, &block)
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'debug', 'kill'
          @node.call_all(method, command, args, &block).first
        when 'flush', 'load'
          @node.call_primaries(method, command, args, &block).first
        when 'exists'
          @node.call_all(method, command, args, &block).transpose.map { |arr| arr.any?(&:zero?) ? 0 : 1 }
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_pubsub_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'channels' then @node.call_all(method, command, args, &block).flatten.uniq.sort_by(&:to_s)
        when 'numsub'
          @node.call_all(method, command, args, &block).reject(&:empty?).map { |e| Hash[*e] }
               .reduce({}) { |a, e| a.merge(e) { |_, v1, v2| v1 + v2 } }
        when 'numpat' then @node.call_all(method, command, args, &block).select { |e| e.is_a?(Integer) }.sum
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def fetch_cluster_info(config, pool: nil, **kwargs)
        node_info_list = ::RedisClient::Cluster::Node.load_info(config.per_node_key, **kwargs)
        node_addrs = node_info_list.map { |i| ::RedisClient::Cluster::NodeKey.hashify(i.node_key) }
        config.update_node(node_addrs)
        ::RedisClient::Cluster::Node.new(
          config.per_node_key,
          node_info_list: node_info_list,
          pool: pool,
          with_replica: config.use_replica?,
          replica_affinity: config.replica_affinity,
          **kwargs
        )
      end

      def update_cluster_info!
        return if @mutex.locked?

        @mutex.synchronize do
          begin
            @node.each(&:close)
          rescue ::RedisClient::Cluster::ErrorCollection
            # ignore
          end

          @node = fetch_cluster_info(@config, pool: @pool, **@client_kwargs)
        end
      end
    end
  end
end
