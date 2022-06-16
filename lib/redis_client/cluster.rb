# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/command'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/node'
require 'redis_client/cluster/node_key'

class RedisClient
  class Cluster
    class Pipeline
      ReplySizeError = Class.new(::RedisClient::Error)

      def initialize(client)
        @client = client
        @grouped = Hash.new([].freeze)
        @replies = []
        @size = 0
      end

      def call(*command, **kwargs)
        node_key = @client.send(:find_node_key, command, primary_only: true)
        @grouped[node_key] += [[@size, :call, command, kwargs]]
        @size += 1
      end

      def call_once(*command, **kwargs)
        node_key = @client.send(:find_node_key, command, primary_only: true)
        @grouped[node_key] += [[@size, :call_once, command, kwargs]]
        @size += 1
      end

      def blocking_call(timeout, *command, **kwargs)
        node_key = @client.send(:find_node_key, command, primary_only: true)
        @grouped[node_key] += [[@size, :blocking_call, timeout, command, kwargs]]
        @size += 1
      end

      def empty?
        @size.zero?
      end

      # TODO: use concurrency
      def execute # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength
        @grouped.each do |node_key, rows|
          replies = @client.send(:find_node, node_key).pipelined do |pipeline|
            rows.each do |row|
              case row[1]
              when :call then pipeline.call(*row[2], **row[3])
              when :call_once then pipeline.call_once(*row[2], **row[3])
              when :blocking_call then pipeline.blocking_call(row[2], *row[3], **row[4])
              else raise NotImplementedError, row[1]
              end
            end
          end

          raise ReplySizeError, "commands: #{rows.size}, replies: #{replies.size}" if rows.size != replies.size

          rows.each_with_index { |row, idx| @replies[row.first] = replies[idx] }
        end

        @replies
      end
    end

    ZERO_CURSOR_FOR_SCAN = '0'

    def initialize(config, pool: nil, **kwargs)
      @config = config.dup
      @pool = pool
      @client_kwargs = kwargs
      @node = fetch_cluster_info!(@config, pool: @pool, **@client_kwargs)
      @command = ::RedisClient::Cluster::Command.load(@node)
    end

    def inspect
      "#<#{self.class.name} #{@node.node_keys.join(', ')}>"
    end

    def call(*command, **kwargs)
      send_command(:call, *command, **kwargs)
    end

    def call_once(*command, **kwargs)
      send_command(:call_once, *command, **kwargs)
    end

    def blocking_call(timeout, *command, **kwargs)
      node = assign_node(*command)
      try_send(node, :blocking_call, timeout, *command, **kwargs)
    end

    def scan(*args, **kwargs, &block)
      raise ArgumentError, 'block required' unless block

      cursor = ZERO_CURSOR_FOR_SCAN
      loop do
        cursor, keys = _scan('SCAN', cursor, *args, **kwargs)
        keys.each(&block)
        break if cursor == ZERO_CURSOR_FOR_SCAN
      end
    end

    def sscan(key, *args, **kwargs, &block)
      node = assign_node('SSCAN', key)
      try_send(node, :sscan, key, *args, **kwargs, &block)
    end

    def hscan(key, *args, **kwargs, &block)
      node = assign_node('HSCAN', key)
      try_send(node, :hscan, key, *args, **kwargs, &block)
    end

    def zscan(key, *args, **kwargs, &block)
      node = assign_node('ZSCAN', key)
      try_send(node, :zscan, key, *args, **kwargs, &block)
    end

    def mset
      # TODO: impl
    end

    def mget
      # TODO: impl
    end

    def pipelined
      pipeline = ::RedisClient::Cluster::Pipeline.new(self)
      yield pipeline
      return [] if pipeline.empty? == 0

      pipeline.execute
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

    def close
      @node.each(&:close)
      nil
    end

    private

    def fetch_cluster_info!(config, pool: nil, **kwargs)
      node_info = ::RedisClient::Cluster::Node.load_info(config.per_node_key, **kwargs)
      node_addrs = node_info.map { |info| ::RedisClient::Cluster::NodeKey.hashify(info[:node_key]) }
      config.update_node(node_addrs)
      ::RedisClient::Cluster::Node.new(config.per_node_key,
                                       node_info: node_info, pool: pool, with_replica: config.use_replica?, **kwargs)
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
      when 'scan'     then _scan(*command, **kwargs)
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
        node = assign_node(*command)
        try_send(node, method, *command, **kwargs, &block)
      end
    end

    def send_config_command(method, *command, **kwargs, &block)
      case command[1].to_s.downcase
      when 'resetstat', 'rewrite', 'set'
        @node.call_all(method, *command, **kwargs, &block).first
      else assign_node(*command).send(method, *command, **kwargs, &block)
      end
    end

    def send_memory_command(method, *command, **kwargs, &block)
      case command[1].to_s.downcase
      when 'stats' then @node.call_all(method, *command, **kwargs, &block)
      when 'purge' then @node.call_all(method, *command, **kwargs, &block).first
      else assign_node(*command).send(method, *command, **kwargs, &block)
      end
    end

    def send_client_command(method, *command, **kwargs, &block)
      case command[1].to_s.downcase
      when 'list' then @node.call_all(method, *command, **kwargs, &block).flatten
      when 'pause', 'reply', 'setname'
        @node.call_all(method, *command, **kwargs, &block).first
      else assign_node(*command).send(method, *command, **kwargs, &block)
      end
    end

    def send_cluster_command(method, *command, **kwargs, &block)
      subcommand = command[1].to_s.downcase
      case subcommand
      when 'addslots', 'delslots', 'failover', 'forget', 'meet', 'replicate',
           'reset', 'set-config-epoch', 'setslot'
        raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, ['cluster', subcommand]
      when 'saveconfig' then @node.call_all(method, *command, **kwargs, &block).first
      else assign_node(*command).send(method, *command, **kwargs, &block)
      end
    end

    def send_script_command(method, *command, **kwargs, &block)
      case command[1].to_s.downcase
      when 'debug', 'kill'
        @node.call_all(method, *command, **kwargs, &block).first
      when 'flush', 'load'
        @node.call_primary(method, *command, **kwargs, &block).first
      else assign_node(*command).send(method, *command, **kwargs, &block)
      end
    end

    def send_pubsub_command(method, *command, **kwargs, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
      case command[1].to_s.downcase
      when 'channels' then @node.call_all(method, *command, **kwargs, &block).flatten.uniq.sort
      when 'numsub'
        @node.call_all(method, *command, **kwargs, &block).reject(&:empty?).map { |e| Hash[*e] }
             .reduce({}) { |a, e| a.merge(e) { |_, v1, v2| v1 + v2 } }
      when 'numpat' then @node.call_all(method, *command, **kwargs, &block).sum
      else assign_node(*command).send(method, *command, **kwargs, &block)
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
        node.call('ASKING')
        retry_count -= 1
        retry
      else
        raise
      end
    rescue ::RedisClient::ConnectionError
      update_cluster_info!
      raise
    end

    def _scan(*command, **kwargs) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
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

    def assign_node(*command)
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

      @node.each(&:close)
      @node = fetch_cluster_info!(@config, pool: @pool, **@client_kwargs)
    end
  end
end
