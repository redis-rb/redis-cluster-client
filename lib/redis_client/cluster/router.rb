# frozen_string_literal: true

require 'redis_client'
require 'redis_client/circuit_breaker'
require 'redis_client/cluster/command'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/node'
require 'redis_client/cluster/node_key'
require 'redis_client/cluster/normalized_cmd_name'
require 'redis_client/cluster/transaction'
require 'redis_client/cluster/optimistic_locking'
require 'redis_client/cluster/pipeline'
require 'redis_client/cluster/error_identification'

class RedisClient
  class Cluster
    class Router
      ZERO_CURSOR_FOR_SCAN = '0'
      TSF = ->(f, x) { f.nil? ? x : f.call(x) }.curry

      private_constant :ZERO_CURSOR_FOR_SCAN, :TSF

      def initialize(config, concurrent_worker, pool: nil, **kwargs)
        @config = config.dup
        @original_config = config.dup if config.connect_with_original_config
        @connect_with_original_config = config.connect_with_original_config
        @concurrent_worker = concurrent_worker
        @pool = pool
        @client_kwargs = kwargs
        @node = ::RedisClient::Cluster::Node.new(concurrent_worker, config: config, pool: pool, **kwargs)
        update_cluster_info!
        @command = ::RedisClient::Cluster::Command.load(@node.replica_clients.shuffle, slow_command_timeout: config.slow_command_timeout)
        @command_builder = @config.command_builder
      end

      def send_command(method, command, *args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        cmd = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        case cmd
        when 'ping'     then @node.send_ping(method, command, args).first.then(&TSF.call(block))
        when 'wait'     then send_wait_command(method, command, args, &block)
        when 'keys'     then @node.call_replicas(method, command, args).flatten.sort_by(&:to_s).then(&TSF.call(block))
        when 'dbsize'   then @node.call_replicas(method, command, args).select { |e| e.is_a?(Integer) }.sum.then(&TSF.call(block))
        when 'scan'     then scan(command, seed: 1)
        when 'lastsave' then @node.call_all(method, command, args).sort_by(&:to_i).then(&TSF.call(block))
        when 'role'     then @node.call_all(method, command, args, &block)
        when 'config'   then send_config_command(method, command, args, &block)
        when 'client'   then send_client_command(method, command, args, &block)
        when 'cluster'  then send_cluster_command(method, command, args, &block)
        when 'memory'   then send_memory_command(method, command, args, &block)
        when 'script'   then send_script_command(method, command, args, &block)
        when 'pubsub'   then send_pubsub_command(method, command, args, &block)
        when 'watch'    then send_watch_command(command, &block)
        when 'mset', 'mget', 'del'
          send_multiple_keys_command(cmd, method, command, args, &block)
        when 'acl', 'auth', 'bgrewriteaof', 'bgsave', 'quit', 'save'
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        when 'flushall', 'flushdb'
          @node.call_primaries(method, command, args).first.then(&TSF.call(block))
        when 'readonly', 'readwrite', 'shutdown'
          raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, cmd
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
          next false if ::RedisClient::Cluster::ErrorIdentification.identifiable?(err) && @node.none? { |c| ::RedisClient::Cluster::ErrorIdentification.client_owns_error?(err, c) }

          err.message.start_with?('CLUSTERDOWN Hash slot not served')
        end

        raise
      end

      # @see https://redis.io/docs/reference/cluster-spec/#redirection-and-resharding Redirection and resharding
      def try_send(node, method, command, args, retry_count: 3, &block)
        handle_redirection(node, retry_count: retry_count) do |on_node|
          if args.empty?
            # prevent memory allocation for variable-length args
            on_node.public_send(method, command, &block)
          else
            on_node.public_send(method, *args, command, &block)
          end
        end
      end

      def try_delegate(node, method, *args, retry_count: 3, **kwargs, &block)
        handle_redirection(node, retry_count: retry_count) do |on_node|
          on_node.public_send(method, *args, **kwargs, &block)
        end
      end

      def handle_redirection(node, retry_count:) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        yield node
      rescue ::RedisClient::CircuitBreaker::OpenCircuitError
        raise
      rescue ::RedisClient::CommandError => e
        raise unless ::RedisClient::Cluster::ErrorIdentification.client_owns_error?(e, node)

        if e.message.start_with?('MOVED')
          node = assign_redirection_node(e.message)
          retry_count -= 1
          retry if retry_count >= 0
        elsif e.message.start_with?('ASK')
          node = assign_asking_node(e.message)
          retry_count -= 1
          if retry_count >= 0
            node.call('ASKING')
            retry
          end
        elsif e.message.start_with?('CLUSTERDOWN Hash slot not served')
          update_cluster_info!
          retry_count -= 1
          retry if retry_count >= 0
        end
        raise
      rescue ::RedisClient::ConnectionError => e
        raise unless ::RedisClient::Cluster::ErrorIdentification.client_owns_error?(e, node)

        update_cluster_info!

        raise if retry_count <= 0

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

      def find_node_key_by_key(key, seed: nil, primary: false)
        if key && !key.empty?
          slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
          primary ? @node.find_node_key_of_primary(slot) : @node.find_node_key_of_replica(slot)
        else
          primary ? @node.any_primary_node_key(seed: seed) : @node.any_replica_node_key(seed: seed)
        end
      end

      def find_primary_node_by_slot(slot)
        node_key = @node.find_node_key_of_primary(slot)
        find_node(node_key)
      end

      def find_node_key(command, seed: nil)
        key = @command.extract_first_key(command)
        find_node_key_by_key(key, seed: seed, primary: @command.should_send_to_primary?(command))
      end

      def find_primary_node_key(command)
        key = @command.extract_first_key(command)
        return nil unless key&.size&.> 0

        find_node_key_by_key(key, primary: true)
      end

      def find_slot(command)
        find_slot_by_key(@command.extract_first_key(command))
      end

      def find_slot_by_key(key)
        return if key.empty?

        ::RedisClient::Cluster::KeySlotConverter.convert(key)
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

      def node_keys
        @node.node_keys
      end

      def close
        @node.each(&:close)
      end

      private

      def send_wait_command(method, command, args, retry_count: 3, &block) # rubocop:disable Metrics/AbcSize
        @node.call_primaries(method, command, args).select { |r| r.is_a?(Integer) }.sum.then(&TSF.call(block))
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
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_memory_command(method, command, args, &block)
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'stats' then @node.call_all(method, command, args, &block)
        when 'purge' then @node.call_all(method, command, args).first.then(&TSF.call(block))
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_client_command(method, command, args, &block)
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'list' then @node.call_all(method, command, args, &block).flatten
        when 'pause', 'reply', 'setname'
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_cluster_command(method, command, args, &block)
        case subcommand = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'addslots', 'delslots', 'failover', 'forget', 'meet', 'replicate',
             'reset', 'set-config-epoch', 'setslot'
          raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, ['cluster', subcommand]
        when 'saveconfig' then @node.call_all(method, command, args).first.then(&TSF.call(block))
        when 'getkeysinslot'
          raise ArgumentError, command.join(' ') if command.size != 4

          find_node(@node.find_node_key_of_replica(command[2])).public_send(method, *args, command, &block)
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_script_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'debug', 'kill'
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        when 'flush', 'load'
          @node.call_primaries(method, command, args).first.then(&TSF.call(block))
        when 'exists'
          @node.call_all(method, command, args).transpose.map { |arr| arr.any?(&:zero?) ? 0 : 1 }.then(&TSF.call(block))
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_pubsub_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        case ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_subcommand(command)
        when 'channels'
          @node.call_all(method, command, args).flatten.uniq.sort_by(&:to_s).then(&TSF.call(block))
        when 'shardchannels'
          @node.call_replicas(method, command, args).flatten.uniq.sort_by(&:to_s).then(&TSF.call(block))
        when 'numpat'
          @node.call_all(method, command, args).select { |e| e.is_a?(Integer) }.sum.then(&TSF.call(block))
        when 'numsub'
          @node.call_all(method, command, args).reject(&:empty?).map { |e| Hash[*e] }
               .reduce({}) { |a, e| a.merge(e) { |_, v1, v2| v1 + v2 } }.then(&TSF.call(block))
        when 'shardnumsub'
          @node.call_replicas(method, command, args).reject(&:empty?).map { |e| Hash[*e] }
               .reduce({}) { |a, e| a.merge(e) { |_, v1, v2| v1 + v2 } }.then(&TSF.call(block))
        else assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_watch_command(command)
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, 'A block required. And you need to use the block argument as a client for the transaction.' unless block_given?

        ::RedisClient::Cluster::OptimisticLocking.new(self).watch(command[1..]) do |c, slot, asking|
          transaction = ::RedisClient::Cluster::Transaction.new(
            self, @command_builder, node: c, slot: slot, asking: asking
          )
          yield transaction
          transaction.execute
        end
      end

      MULTIPLE_KEYS_COMMAND_TO_SINGLE = {
        'mget' => ['get', 1].freeze,
        'mset' => ['set', 2].freeze,
        'del' => ['del', 1].freeze
      }.freeze

      private_constant :MULTIPLE_KEYS_COMMAND_TO_SINGLE

      def send_multiple_keys_command(cmd, method, command, args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        # This implementation is prioritized performance rather than readability or so.
        single_key_cmd, keys_step = MULTIPLE_KEYS_COMMAND_TO_SINGLE.fetch(cmd)

        return try_send(assign_node(command), method, command, args, &block) if command.size <= keys_step + 1 || ::RedisClient::Cluster::KeySlotConverter.hash_tag_included?(command[1])

        seed = @config.use_replica? && @config.replica_affinity == :random ? nil : Random.new_seed
        pipeline = ::RedisClient::Cluster::Pipeline.new(self, @command_builder, @concurrent_worker, exception: true, seed: seed)

        single_command = Array.new(keys_step + 1)
        single_command[0] = single_key_cmd
        if keys_step == 1
          command[1..].each do |key|
            single_command[1] = key
            pipeline.call_v(single_command)
          end
        else
          command[1..].each_slice(keys_step) do |v|
            keys_step.times { |i| single_command[i + 1] = v[i] }
            pipeline.call_v(single_command)
          end
        end

        replies = pipeline.execute
        result = case cmd
                 when 'mset' then replies.first
                 when 'del' then replies.sum
                 else replies
                 end
        block_given? ? yield(result) : result
      end

      def update_cluster_info!
        @node.reload!
      end
    end
  end
end
