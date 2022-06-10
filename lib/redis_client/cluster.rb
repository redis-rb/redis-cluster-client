# frozen_string_literal: true

require 'redis_client/cluster/command'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/node'
require 'redis_client/cluster/node_key'

class RedisClient
  class Cluster
    def initialize(config, pool: nil, **kwargs)
      @config = config.dup
      @pool = pool
      @client_kwargs = kwargs
      @node = fetch_cluster_info!(@config, @pool, **@client_kwargs)
      @command = ::RedisClient::Cluster::CommandLoader.load(@node)
    end

    def call(*command, **kwargs, &block)
      send_command(:call, *command, **kwargs, &block)
    end

    def call_once(*command, **kwargs, &block)
      send_command(:call_once, *command, **kwargs, &block)
    end

    def blocking_call(timeout, *command, **kwargs, &block)
      node = assign_node(command)
      try_send(node, :blocking_call, *([timeout] + command), **kwargs, &block)
    end

    def scan(*args, **kwargs, &block)
      _scan(:scan, *args, **kwargs, &block)
    end

    def sscan(key, *args, **kwargs, &block)
      node = assign_node(['SSCAN', key])
      try_send(node, :sscan, *([key] + args), **kwargs, &block)
    end

    def hscan(key, *args, **kwargs, &block)
      node = assign_node(['HSCAN', key])
      try_send(node, :hscan, *([key] + args), **kwargs, &block)
    end

    def zscan(key, *args, **kwargs, &block)
      node = assign_node(['ZSCAN', key])
      try_send(node, :zscan, *([key] + args), **kwargs, &block)
    end

    def pipelined
      # TODO: impl
    end

    def multi
      # TODO: impl
    end

    def pubsub
      # TODO: impl
    end

    def size
      # TODO: impl
    end

    def with(options = {})
      # TODO: impl
    end
    alias then with

    def id
      @node.map(&:id).sort.join(' ')
    end

    def connected?
      @node.any?(&:connected?)
    end

    def close
      @node.each(&:disconnect)
      true
    end

    # TODO: remove
    def call_pipeline(pipeline)
      node_keys = pipeline.commands.filter_map { |cmd| find_node_key(cmd, primary_only: true) }.uniq
      if node_keys.size > 1
        raise(CrossSlotPipeliningError,
              pipeline.commands.map { |cmd| @command.extract_first_key(cmd) }.reject(&:empty?).uniq)
      end

      try_send(find_node(node_keys.first), :call_pipeline, pipeline)
    end

    # TODO: remove
    def process(commands, &block)
      if commands.size == 1 &&
         %w[unsubscribe punsubscribe].include?(commands.first.first.to_s.downcase) &&
         commands.first.size == 1

        # Node is indeterminate. We do just a best-effort try here.
        @node.process_all(commands, &block)
      else
        node = assign_node(commands.first)
        try_send(node, :process, commands, &block)
      end
    end

    private

    def fetch_cluster_info!(config, pool, **kwargs)
      node_info = ::RedisClient::Cluster::Node.load_info(config.per_node_key, **kwargs)
      node_addrs = node_info.map { |arr| ::RedisClient::Cluster::NodeKey.hashify(arr[1]) }
      config.update_node(node_addrs)
      ::RedisClient::Cluster::Node.new(config.per_node_key, node_info, pool, with_replica: config.use_replica?, **kwargs)
    end

    def send_command(method, *command, **kwargs, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength
      cmd = command.first.to_s.downcase
      case cmd
      when 'acl', 'auth', 'bgrewriteaof', 'bgsave', 'quit', 'save'
        @node.call_all(method, *command, **kwargs, &block).first
      when 'flushall', 'flushdb'
        @node.call_primary(method, *command, **kwargs, &block).first
      when 'wait'     then @node.call_primary(method, *command, **kwargs, &block).sum
      when 'keys'     then @node.call_replica(method, *command, **kwargs, &block).flatten.sort
      when 'dbsize'   then @node.call_replica(method, *command, **kwargs, &block).sum
      when 'scan'     then _scan(method, *command, **kwargs, &block)
      when 'lastsave' then @node.call_all(method, *command, **kwargs, &block).sort
      when 'role'     then @node.call_all(method, *command, **kwargs, &block)
      when 'config'   then send_config_command(method, *command, **kwargs, &block)
      when 'client'   then send_client_command(method, *command, **kwargs, &block)
      when 'cluster'  then send_cluster_command(method, *command, **kwargs, &block)
      when 'readonly', 'readwrite', 'shutdown'
        raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, cmd
      when 'memory'   then send_memory_command(method, *command, **kwargs, &block)
      when 'script'   then send_script_command(method, *command, **kwargs, &block)
      when 'pubsub'   then send_pubsub_command(method, *command, **kwargs, &block)
      when 'discard', 'exec', 'multi', 'unwatch'
        raise ::RedisClient::Cluster::AmbiguousNodeError, cmd
      else
        node = assign_node(command)
        try_send(node, method, *command, **kwargs, &block)
      end
    end

    def send_config_command(method, *command, **kwargs, &block)
      case command[1].to_s.downcase
      when 'resetstat', 'rewrite', 'set'
        @node.call_all(method, *command, **kwargs, &block).first
      else assign_node(command).send(method, *command, **kwargs, &block)
      end
    end

    def send_memory_command(method, *command, **kwargs, &block)
      case command[1].to_s.downcase
      when 'stats' then @node.call_all(method, *command, **kwargs, &block)
      when 'purge' then @node.call_all(method, *command, **kwargs, &block).first
      else assign_node(command).send(method, *command, **kwargs, &block)
      end
    end

    def send_client_command(method, *command, **kwargs, &block)
      case command[1].to_s.downcase
      when 'list' then @node.call_all(method, *command, **kwargs, &block).flatten
      when 'pause', 'reply', 'setname'
        @node.call_all(method, *command, **kwargs, &block).first
      else assign_node(command).send(method, *command, **kwargs, &block)
      end
    end

    def send_cluster_command(method, *command, **kwargs, &block)
      subcommand = command[1].to_s.downcase
      case subcommand
      when 'addslots', 'delslots', 'failover', 'forget', 'meet', 'replicate',
           'reset', 'set-config-epoch', 'setslot'
        raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, 'cluster', subcommand
      when 'saveconfig' then @node.call_all(method, *command, **kwargs, &block).first
      else assign_node(command).send(method, *command, **kwargs, &block)
      end
    end

    def send_script_command(method, *command, **kwargs, &block)
      case command[1].to_s.downcase
      when 'debug', 'kill'
        @node.call_all(method, *command, **kwargs, &block).first
      when 'flush', 'load'
        @node.call_primary(method, *command, **kwargs, &block).first
      else assign_node(command).send(method, *command, **kwargs, &block)
      end
    end

    def send_pubsub_command(method, *command, **kwargs, &block) # rubocop:disable Metircs/AbcSize, Metrics/CyclomaticComplexity
      case command[1].to_s.downcase
      when 'channels' then @node.call_all(method, *command, **kwargs, &block).flatten.uniq.sort
      when 'numsub'
        @node.call_all(method, *command, **kwargs, &block).reject(&:empty?).map { |e| Hash[*e] }
             .reduce({}) { |a, e| a.merge(e) { |_, v1, v2| v1 + v2 } }
      when 'numpat' then @node.call_all(method, *command, **kwargs, &block).sum
      else assign_node(command).send(method, *command, **kwargs, &block)
      end
    end

    # @see https://redis.io/topics/cluster-spec#redirection-and-resharding
    #   Redirection and resharding
    def try_send(node, method, *args, retry_count: 3, **kwargs, &block) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      node.send(method, *args, **kwargs, &block)
    rescue ::RedisClient::CommandError => e
      if e.message.start_with?('MOVED')
        raise if retry_count <= 0

        node = assign_redirection_node(e.message)
        retry_count -= 1
        retry
      elsif e.message.start_with?('ASK')
        raise if retry_count <= 0

        node = assign_asking_node(e.message)
        node.call(%w[ASKING])
        retry_count -= 1
        retry
      else
        raise
      end
    rescue ::RedisClient::ConnectionError
      update_cluster_info!
      raise
    end

    def _scan(method, *command, **kwargs, &block) # rubocop:disable Metrics/MethodLength
      input_cursor = Integer(command[1])

      client_index = input_cursor % 256
      raw_cursor = input_cursor >> 8

      clients = @node.scale_reading_clients

      client = clients[client_index]
      return ['0', []] unless client

      command[1] = raw_cursor.to_s

      result_cursor, result_keys = client.send(method, *command, **kwargs, &block)
      result_cursor = Integer(result_cursor)

      client_index += 1 if result_cursor == 0

      [((result_cursor << 8) + client_index).to_s, result_keys]
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

    def assign_node(command)
      node_key = find_node_key(command)
      find_node(node_key)
    end

    def find_node_key(command, primary_only: false)
      key = @command.extract_first_key(command)
      return if key.empty?

      slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
      return unless @node.slot_exists?(slot)

      if @command.should_send_to_primary?(command) || primary_only
        @node.find_node_key_of_primary(slot)
      else
        @node.find_node_key_of_replica(slot)
      end
    end

    def find_node(node_key)
      return @node.sample if node_key.nil?

      @node.find_by(node_key)
    rescue ::RedisClient::Cluster::Node::ReloadNeeded
      update_cluster_info!(node_key)
      @node.find_by(node_key)
    end

    def update_cluster_info!(node_key = nil)
      unless node_key.nil?
        host, port = ::RedisClient::Cluster::NodeKey.split(node_key)
        @config.add_node(host, port)
      end

      @node.map(&:disconnect)
      @node = fetch_cluster_info!(@config, @pool, **@client_kwargs)
    end
  end
end
