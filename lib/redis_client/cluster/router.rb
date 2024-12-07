# frozen_string_literal: true

require 'redis_client'
require 'redis_client/circuit_breaker'
require 'redis_client/cluster/command'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/node'
require 'redis_client/cluster/node_key'
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

      attr_reader :config

      Action = Struct.new(
        'RedisCommandRoutingAction',
        :method_name,
        :reply_transformer,
        keyword_init: true
      )

      def initialize(config, concurrent_worker, pool: nil, **kwargs)
        @config = config
        @concurrent_worker = concurrent_worker
        @pool = pool
        @client_kwargs = kwargs
        @node = ::RedisClient::Cluster::Node.new(concurrent_worker, config: config, pool: pool, **kwargs)
        @node.reload!
        @command = ::RedisClient::Cluster::Command.load(@node.replica_clients.shuffle, slow_command_timeout: config.slow_command_timeout)
        @command_builder = @config.command_builder
        @dedicated_actions = build_dedicated_actions
      rescue ::RedisClient::Cluster::InitialSetupError => e
        e.with_config(config)
        raise
      end

      def send_command(method, command, *args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        return assign_node_and_send_command(method, command, args, &block) unless @dedicated_actions.key?(command.first)

        action = @dedicated_actions[command.first]
        return send(action.method_name, method, command, args, &block) if action.reply_transformer.nil?

        reply = send(action.method_name, method, command, args)
        action.reply_transformer.call(reply).then(&TSF.call(block))
      rescue ::RedisClient::CircuitBreaker::OpenCircuitError
        raise
      rescue ::RedisClient::Cluster::Node::ReloadNeeded
        renew_cluster_state
        raise ::RedisClient::Cluster::NodeMightBeDown.new.with_config(@config)
      rescue ::RedisClient::ConnectionError
        renew_cluster_state
        raise
      rescue ::RedisClient::CommandError => e
        renew_cluster_state if e.message.start_with?('CLUSTERDOWN')
        raise
      rescue ::RedisClient::Cluster::ErrorCollection => e
        e.with_config(@config)
        raise if e.errors.any?(::RedisClient::CircuitBreaker::OpenCircuitError)

        renew_cluster_state if e.errors.values.any? do |err|
          next false if ::RedisClient::Cluster::ErrorIdentification.identifiable?(err) && @node.none? { |c| ::RedisClient::Cluster::ErrorIdentification.client_owns_error?(err, c) }

          err.message.start_with?('CLUSTERDOWN') || err.is_a?(::RedisClient::ConnectionError)
        end

        raise
      end

      # @see https://redis.io/docs/reference/cluster-spec/#redirection-and-resharding Redirection and resharding
      def assign_node_and_send_command(method, command, args, retry_count: 3, &block)
        node = assign_node(command)
        send_command_to_node(node, method, command, args, retry_count: retry_count, &block)
      end

      def send_command_to_node(node, method, command, args, retry_count: 3, &block)
        handle_redirection(node, command, retry_count: retry_count) do |on_node|
          if args.empty?
            # prevent memory allocation for variable-length args
            on_node.public_send(method, command, &block)
          else
            on_node.public_send(method, *args, command, &block)
          end
        end
      end

      def handle_redirection(node, command, retry_count:) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        yield node
      rescue ::RedisClient::CircuitBreaker::OpenCircuitError
        raise
      rescue ::RedisClient::CommandError => e
        raise unless ::RedisClient::Cluster::ErrorIdentification.client_owns_error?(e, node)

        retry_count -= 1
        if e.message.start_with?('MOVED')
          node = assign_redirection_node(e.message)
          retry if retry_count >= 0
        elsif e.message.start_with?('ASK')
          node = assign_asking_node(e.message)
          if retry_count >= 0
            node.call('asking')
            retry
          end
        elsif e.message.start_with?('CLUSTERDOWN')
          renew_cluster_state
          retry if retry_count >= 0
        end

        raise
      rescue ::RedisClient::ConnectionError => e
        raise unless ::RedisClient::Cluster::ErrorIdentification.client_owns_error?(e, node)

        retry_count -= 1
        renew_cluster_state

        if retry_count >= 0
          # Find the node to use for this command - if this fails for some reason, though, re-use
          # the old node.
          begin
            node = find_node(find_node_key(command)) if command
          rescue StandardError # rubocop:disable Lint/SuppressedException
          end
          retry
        end

        retry if retry_count >= 0
        raise
      end

      def scan(command, seed: nil) # rubocop:disable Metrics/AbcSize
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
      rescue ::RedisClient::ConnectionError
        renew_cluster_state
        raise
      end

      def scan_single_key(command, arity:, &block)
        node = assign_node(command)
        loop do
          cursor, values = handle_redirection(node, nil, retry_count: 3) { |n| n.call_v(command) }
          command[2] = cursor
          arity < 2 ? values.each(&block) : values.each_slice(arity, &block)
          break if cursor == ZERO_CURSOR_FOR_SCAN
        end
      end

      def assign_node(command)
        handle_node_reload_error do
          node_key = find_node_key(command)
          @node.find_by(node_key)
        end
      end

      def find_node_key_by_key(key, seed: nil, primary: false)
        if key && !key.empty?
          slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
          node_key = primary ? @node.find_node_key_of_primary(slot) : @node.find_node_key_of_replica(slot)
          if node_key.nil?
            renew_cluster_state
            raise ::RedisClient::Cluster::NodeMightBeDown.new.with_config(@config)
          end
          node_key
        else
          primary ? @node.any_primary_node_key(seed: seed) : @node.any_replica_node_key(seed: seed)
        end
      end

      def find_primary_node_by_slot(slot)
        handle_node_reload_error do
          node_key = @node.find_node_key_of_primary(slot)
          @node.find_by(node_key)
        end
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

      def find_node(node_key)
        handle_node_reload_error { @node.find_by(node_key) }
      end

      def command_exists?(name)
        @command.exists?(name)
      end

      def assign_redirection_node(err_msg)
        _, slot, node_key = err_msg.split
        slot = slot.to_i
        @node.update_slot(slot, node_key)
        handle_node_reload_error { @node.find_by(node_key) }
      end

      def assign_asking_node(err_msg)
        _, _, node_key = err_msg.split
        handle_node_reload_error { @node.find_by(node_key) }
      end

      def node_keys
        @node.node_keys
      end

      def renew_cluster_state
        @node.reload!
      rescue ::RedisClient::Cluster::InitialSetupError
        # ignore
      end

      def close
        @node.each(&:close)
      end

      private

      def build_dedicated_actions # rubocop:disable Metrics/AbcSize
        pick_first = ->(reply) { reply.first } # rubocop:disable Style/SymbolProc
        multiple_key_action = Action.new(method_name: :send_multiple_keys_command)
        all_node_first_action = Action.new(method_name: :send_command_to_all_nodes, reply_transformer: pick_first)
        primary_first_action = Action.new(method_name: :send_command_to_primaries, reply_transformer: pick_first)
        not_supported_action = Action.new(method_name: :fail_not_supported_command)
        keyless_action = Action.new(method_name: :fail_keyless_command)
        actions = {
          'ping' => Action.new(method_name: :send_ping_command, reply_transformer: pick_first),
          'wait' => Action.new(method_name: :send_wait_command),
          'keys' => Action.new(method_name: :send_command_to_replicas, reply_transformer: ->(reply) { reply.flatten.sort_by(&:to_s) }),
          'dbsize' => Action.new(method_name: :send_command_to_replicas, reply_transformer: ->(reply) { reply.select { |e| e.is_a?(Integer) }.sum }),
          'scan' => Action.new(method_name: :send_scan_command),
          'lastsave' => Action.new(method_name: :send_command_to_all_nodes, reply_transformer: ->(reply) { reply.sort_by(&:to_i) }),
          'role' => Action.new(method_name: :send_command_to_all_nodes),
          'config' => Action.new(method_name: :send_config_command),
          'client' => Action.new(method_name: :send_client_command),
          'cluster' => Action.new(method_name: :send_cluster_command),
          'memory' => Action.new(method_name: :send_memory_command),
          'script' => Action.new(method_name: :send_script_command),
          'pubsub' => Action.new(method_name: :send_pubsub_command),
          'watch' => Action.new(method_name: :send_watch_command),
          'mget' => multiple_key_action,
          'mset' => multiple_key_action,
          'del' => multiple_key_action,
          'acl' => all_node_first_action,
          'auth' => all_node_first_action,
          'bgrewriteaof' => all_node_first_action,
          'bgsave' => all_node_first_action,
          'quit' => all_node_first_action,
          'save' => all_node_first_action,
          'flushall' => primary_first_action,
          'flushdb' => primary_first_action,
          'readonly' => not_supported_action,
          'readwrite' => not_supported_action,
          'shutdown' => not_supported_action,
          'discard' => keyless_action,
          'exec' => keyless_action,
          'multi' => keyless_action,
          'unwatch' => keyless_action
        }.freeze
        actions.each_with_object({}) do |(k, v), acc|
          acc[k] = v
          acc[k.upcase] = v
        end.freeze
      end

      def send_command_to_all_nodes(method, command, args, &block)
        @node.call_all(method, command, args, &block)
      end

      def send_command_to_primaries(method, command, args, &block)
        @node.call_primaries(method, command, args, &block)
      end

      def send_command_to_replicas(method, command, args, &block)
        @node.call_replicas(method, command, args, &block)
      end

      def send_ping_command(method, command, args, &block)
        @node.send_ping(method, command, args, &block)
      end

      def send_scan_command(_method, command, _args, &_block)
        scan(command, seed: 1)
      end

      def fail_not_supported_command(_method, command, _args, &_block)
        raise ::RedisClient::Cluster::OrchestrationCommandNotSupported.from_command(command.first).with_config(@config)
      end

      def fail_keyless_command(_method, command, _args, &_block)
        raise ::RedisClient::Cluster::AmbiguousNodeError.from_command(command.first).with_config(@config)
      end

      def send_wait_command(method, command, args, retry_count: 1, &block) # rubocop:disable Metrics/AbcSize
        @node.call_primaries(method, command, args).select { |r| r.is_a?(Integer) }.sum.then(&TSF.call(block))
      rescue ::RedisClient::Cluster::ErrorCollection => e
        raise if e.errors.any?(::RedisClient::CircuitBreaker::OpenCircuitError)
        raise if retry_count <= 0
        raise if e.errors.values.none? { |err| err.message.include?('WAIT cannot be used with replica instances') }

        retry_count -= 1
        renew_cluster_state
        retry
      end

      def send_config_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize
        if command[1].casecmp('resetstat').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('rewrite').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('set').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        else
          assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_memory_command(method, command, args, &block)
        if command[1].casecmp('stats').zero?
          @node.call_all(method, command, args, &block)
        elsif command[1].casecmp('purge').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        else
          assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_client_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize
        if command[1].casecmp('list').zero?
          @node.call_all(method, command, args, &block).flatten
        elsif command[1].casecmp('pause').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('reply').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('setname').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        else
          assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_cluster_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        if command[1].casecmp('addslots').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('delslots').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('failover').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('forget').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('meet').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('replicate').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('reset').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('set-config-epoch').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('setslot').zero?
          fail_not_supported_command(method, command, args, &block)
        elsif command[1].casecmp('saveconfig').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('getkeysinslot').zero?
          raise ArgumentError, command.join(' ') if command.size != 4

          handle_node_reload_error do
            node_key = @node.find_node_key_of_replica(command[2])
            @node.find_by(node_key).public_send(method, *args, command, &block)
          end
        else
          assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_script_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        if command[1].casecmp('debug').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('kill').zero?
          @node.call_all(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('flush').zero?
          @node.call_primaries(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('load').zero?
          @node.call_primaries(method, command, args).first.then(&TSF.call(block))
        elsif command[1].casecmp('exists').zero?
          @node.call_all(method, command, args).transpose.map { |arr| arr.any?(&:zero?) ? 0 : 1 }.then(&TSF.call(block))
        else
          assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_pubsub_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        if command[1].casecmp('channels').zero?
          @node.call_all(method, command, args).flatten.uniq.sort_by(&:to_s).then(&TSF.call(block))
        elsif command[1].casecmp('shardchannels').zero?
          @node.call_replicas(method, command, args).flatten.uniq.sort_by(&:to_s).then(&TSF.call(block))
        elsif command[1].casecmp('numpat').zero?
          @node.call_all(method, command, args).select { |e| e.is_a?(Integer) }.sum.then(&TSF.call(block))
        elsif command[1].casecmp('numsub').zero?
          @node.call_all(method, command, args).reject(&:empty?).map { |e| Hash[*e] }
               .reduce({}) { |a, e| a.merge(e) { |_, v1, v2| v1 + v2 } }.then(&TSF.call(block))
        elsif command[1].casecmp('shardnumsub').zero?
          @node.call_replicas(method, command, args).reject(&:empty?).map { |e| Hash[*e] }
               .reduce({}) { |a, e| a.merge(e) { |_, v1, v2| v1 + v2 } }.then(&TSF.call(block))
        else
          assign_node(command).public_send(method, *args, command, &block)
        end
      end

      def send_watch_command(_method, command, _args, &_block)
        unless block_given?
          msg = 'A block required. And you need to use the block argument as a client for the transaction.'
          raise ::RedisClient::Cluster::Transaction::ConsistencyError.new(msg).with_config(@config)
        end

        ::RedisClient::Cluster::OptimisticLocking.new(self).watch(command[1..]) do |c, slot, asking|
          transaction = ::RedisClient::Cluster::Transaction.new(
            self, @command_builder, node: c, slot: slot, asking: asking
          )
          yield transaction
          transaction.execute
        end
      end

      def send_multiple_keys_command(method, command, args, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        # This implementation is prioritized performance rather than readability or so.
        cmd = command.first
        if cmd.casecmp('mget').zero?
          single_key_cmd = 'get'
          keys_step = 1
        elsif cmd.casecmp('mset').zero?
          single_key_cmd = 'set'
          keys_step = 2
        elsif cmd.casecmp('del').zero?
          single_key_cmd = 'del'
          keys_step = 1
        else
          raise NotImplementedError, cmd
        end

        return assign_node_and_send_command(method, command, args, &block) if command.size <= keys_step + 1 || ::RedisClient::Cluster::KeySlotConverter.hash_tag_included?(command[1])

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
        result = if cmd.casecmp('mset').zero?
                   replies.first
                 elsif cmd.casecmp('del').zero?
                   replies.sum
                 else
                   replies
                 end
        block_given? ? yield(result) : result
      end

      def handle_node_reload_error(retry_count: 1)
        yield
      rescue ::RedisClient::Cluster::Node::ReloadNeeded
        raise ::RedisClient::Cluster::NodeMightBeDown.new.with_config(@config) if retry_count <= 0

        retry_count -= 1
        renew_cluster_state
        retry
      end
    end
  end
end
