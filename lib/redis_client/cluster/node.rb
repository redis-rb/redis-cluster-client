# frozen_string_literal: true

require 'redis_client'
require 'redis_client/config'
require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    class Node
      include Enumerable

      SLOT_SIZE = 16_384
      MIN_SLOT = 0
      MAX_SLOT = SLOT_SIZE - 1
      MAX_STARTUP_SAMPLE = 37
      IGNORE_GENERIC_CONFIG_KEYS = %i[url host port path].freeze

      ReloadNeeded = Class.new(::RedisClient::Error)

      class Config < ::RedisClient::Config
        def initialize(scale_read: false, **kwargs)
          @scale_read = scale_read
          super(**kwargs)
        end

        private

        def build_connection_prelude
          prelude = super.dup
          prelude << ['READONLY'] if @scale_read
          prelude.freeze
        end
      end

      class << self
        def load_info(options, **kwargs) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
          startup_size = options.size > MAX_STARTUP_SAMPLE ? MAX_STARTUP_SAMPLE : options.size
          node_info_list = Array.new(startup_size)
          errors = Array.new(startup_size)
          startup_options = options.to_a.sample(MAX_STARTUP_SAMPLE).to_h
          startup_nodes = ::RedisClient::Cluster::Node.new(startup_options, **kwargs)
          threads = startup_nodes.each_with_index.map do |raw_client, idx|
            Thread.new(raw_client, idx) do |cli, i|
              Thread.pass
              reply = cli.call('CLUSTER', 'NODES')
              node_info_list[i] = parse_node_info(reply)
            rescue StandardError => e
              errors[i] = e
            ensure
              cli&.close
            end
          end
          threads.each(&:join)
          raise ::RedisClient::Cluster::InitialSetupError, errors if node_info_list.all?(&:nil?)

          grouped = node_info_list.compact.group_by do |rows|
            rows.sort_by { |row| row[:id] }
                .map { |r| "#{r[:id]}#{r[:node_key]}#{r[:role]}#{r[:primary_id]}#{r[:config_epoch]}" }
                .join
          end

          grouped.max_by { |_, v| v.size }[1].first
        end

        private

        # @see https://redis.io/commands/cluster-nodes/
        # @see https://github.com/redis/redis/blob/78960ad57b8a5e6af743d789ed8fd767e37d42b8/src/cluster.c#L4660-L4683
        def parse_node_info(info) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Metrics/MethodLength
          rows = info.split("\n").map(&:split)
          rows.each { |arr| arr[2] = arr[2].split(',') }
          rows.select! { |arr| arr[7] == 'connected' && (arr[2] & %w[fail? fail handshake noaddr noflags]).empty? }
          rows.each do |arr|
            arr[1] = arr[1].split('@').first
            arr[2] = (arr[2] & %w[master slave]).first
            if arr[8].nil?
              arr[8] = []
              next
            end

            arr[8] = arr[8].split(',').map { |r| r.split('-').map { |s| Integer(s) } }
            arr[8] = arr[8].map { |a| a.size == 1 ? a << a.first : a }.map(&:sort)
          end

          rows.map do |arr|
            { id: arr[0], node_key: arr[1], role: arr[2], primary_id: arr[3], ping_sent: arr[4],
              pong_recv: arr[5], config_epoch: arr[6], link_state: arr[7], slots: arr[8] }
          end
        end
      end

      def initialize(options, node_info: [], pool: nil, with_replica: false, **kwargs)
        @with_replica = with_replica
        @slots = build_slot_node_mappings(node_info)
        @replications = build_replication_mappings(node_info)
        @clients = build_clients(options, pool: pool, **kwargs)
        @mutex = Mutex.new
      end

      def inspect
        "#<#{self.class.name} #{node_keys.join(', ')}>"
      end

      def each(&block)
        @clients.each_value(&block)
      end

      def sample
        @clients.values.sample
      end

      def node_keys
        @clients.keys.sort
      end

      def primary_node_keys
        @clients.filter_map { |k, _| primary?(k) ? k : nil }.sort
      end

      def replica_node_keys
        return primary_node_keys if replica_disabled?

        @clients.filter_map { |k, _| replica?(k) ? k : nil }.sort
      end

      def find_by(node_key)
        raise ReloadNeeded if node_key.nil? || !@clients.key?(node_key)

        @clients.fetch(node_key)
      end

      def call_all(method, *args, **kwargs, &block)
        results, errors = try_map do |_, client|
          client.send(method, *args, **kwargs, &block)
        end

        return results.values if errors.empty?

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end

      def call_primaries(method, *args, **kwargs, &block)
        results, errors = try_map do |node_key, client|
          next if replica?(node_key)

          client.send(method, *args, **kwargs, &block)
        end

        return results.values if errors.empty?

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end

      def call_replicas(method, *args, **kwargs, &block)
        return call_primaries(method, *args, **kwargs, &block) if replica_disabled?

        replica_node_keys = @replications.values.map(&:sample)
        results, errors = try_map do |node_key, client|
          next if primary?(node_key) || !replica_node_keys.include?(node_key)

          client.send(method, *args, **kwargs, &block)
        end

        return results.values if errors.empty?

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end

      def send_ping(method, *args, **kwargs, &block)
        results, errors = try_map do |_, client|
          client.send(method, *args, **kwargs, &block)
        end

        return results.values if errors.empty?

        raise ReloadNeeded if errors.values.any?(::RedisClient::ConnectionError)

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end

      def scale_reading_clients
        keys = replica_disabled? ? @replications.keys : @replications.values.map(&:first)
        @clients.select { |k, _| keys.include?(k) }.values.sort_by do |client|
          ::RedisClient::Cluster::NodeKey.build_from_host_port(client.config.host, client.config.port)
        end
      end

      def find_node_key_of_primary(slot)
        return if slot.nil?

        slot = Integer(slot)
        return if slot < MIN_SLOT || slot > MAX_SLOT

        @slots[slot]
      end

      def find_node_key_of_replica(slot)
        return if slot.nil?

        slot = Integer(slot)
        return if slot < MIN_SLOT || slot > MAX_SLOT

        return @slots[slot] if replica_disabled? || @replications[@slots[slot]].size.zero?

        @replications[@slots[slot]].sample
      end

      def update_slot(slot, node_key)
        @mutex.synchronize { @slots[slot] = node_key }
      end

      def replicated?(primary_node_key, replica_node_key)
        return false if @replications.nil? || @replications.size.zero?

        @replications.fetch(primary_node_key).include?(replica_node_key)
      end

      private

      def replica_disabled?
        !@with_replica
      end

      def primary?(node_key)
        !replica?(node_key)
      end

      def replica?(node_key)
        return false if @replications.nil? || @replications.size.zero?

        !@replications.key?(node_key)
      end

      def build_slot_node_mappings(node_info)
        slots = Array.new(SLOT_SIZE)
        node_info.each do |info|
          next if info[:slots].nil? || info[:slots].empty?

          info[:slots].each { |start, last| (start..last).each { |i| slots[i] = info[:node_key] } }
        end

        slots
      end

      def build_replication_mappings(node_info) # rubocop:disable Metrics/AbcSize
        dict = node_info.to_h { |info| [info[:id], info] }
        node_info.each_with_object(Hash.new { |h, k| h[k] = [] }) do |info, acc|
          primary_info = dict[info[:primary_id]]
          acc[primary_info[:node_key]] << info[:node_key] unless primary_info.nil?
          acc[info[:node_key]] if info[:role] == 'master' # for the primary which have no replicas
        end
      end

      def build_clients(options, pool: nil, **kwargs)
        options.filter_map do |node_key, option|
          next if replica_disabled? && replica?(node_key)

          config = ::RedisClient::Cluster::Node::Config.new(
            scale_read: replica?(node_key),
            **option.merge(kwargs.reject { |k, _| IGNORE_GENERIC_CONFIG_KEYS.include?(k) })
          )
          client = pool.nil? ? config.new_client : config.new_pool(**pool)

          [node_key, client]
        end.to_h
      end

      def try_map # rubocop:disable Metrics/MethodLength
        results = {}
        errors = {}
        threads = @clients.map do |k, v|
          Thread.new(k, v) do |node_key, client|
            Thread.pass
            reply = yield(node_key, client)
            results[node_key] = reply unless reply.nil?
          rescue StandardError => e
            errors[node_key] = e
          end
        end

        threads.each(&:join)
        [results, errors]
      end
    end
  end
end
