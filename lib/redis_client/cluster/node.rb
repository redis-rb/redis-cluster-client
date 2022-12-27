# frozen_string_literal: true

require 'redis_client'
require 'redis_client/config'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/node/primary_only'
require 'redis_client/cluster/node/random_replica'
require 'redis_client/cluster/node/latency_replica'

class RedisClient
  class Cluster
    class Node
      include Enumerable

      SLOT_SIZE = 16_384
      MIN_SLOT = 0
      MAX_SLOT = SLOT_SIZE - 1
      MAX_STARTUP_SAMPLE = 37
      MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))
      IGNORE_GENERIC_CONFIG_KEYS = %i[url host port path].freeze

      ReloadNeeded = Class.new(::RedisClient::Error)

      Info = Struct.new(
        'RedisClusterNode',
        :id, :node_key, :role, :primary_id, :ping_sent,
        :pong_recv, :config_epoch, :link_state, :slots,
        keyword_init: true
      ) do
        def primary?
          role == 'master'
        end

        def replica?
          role == 'slave'
        end
      end

      class CharArray
        BASE = ''
        PADDING = '0'

        def initialize(size, elements)
          @elements = elements
          @string = String.new(BASE, encoding: Encoding::BINARY, capacity: size)
          size.times { @string << PADDING }
        end

        def [](index)
          raise IndexError if index < 0
          return if index >= @string.bytesize

          @elements[@string.getbyte(index)]
        end

        def []=(index, element)
          raise IndexError if index < 0
          return if index >= @string.bytesize

          pos = @elements.find_index(element) # O(N)
          if pos.nil?
            raise(RangeError, 'full of elements') if @elements.size >= 256

            pos = @elements.size
            @elements << element
          end

          @string.setbyte(index, pos)
        end
      end

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
        def load_info(options, **kwargs) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          startup_size = options.size > MAX_STARTUP_SAMPLE ? MAX_STARTUP_SAMPLE : options.size
          node_info_list = errors = nil
          startup_options = options.to_a.sample(MAX_STARTUP_SAMPLE).to_h
          startup_nodes = ::RedisClient::Cluster::Node.new(startup_options, **kwargs)
          startup_nodes.each_slice(MAX_THREADS).with_index do |chuncked_startup_nodes, chuncked_idx|
            threads = chuncked_startup_nodes.each_with_index.map do |raw_client, idx|
              Thread.new(raw_client, (MAX_THREADS * chuncked_idx) + idx) do |cli, i|
                Thread.current[:index] = i
                reply = cli.call('CLUSTER', 'NODES')
                Thread.current[:info] = parse_cluster_node_reply(reply)
              rescue StandardError => e
                Thread.current[:error] = e
              ensure
                cli&.close
              end
            end

            threads.each do |t|
              t.join
              if t.key?(:info)
                node_info_list ||= Array.new(startup_size)
                node_info_list[t[:index]] = t[:info]
              elsif t.key?(:error)
                errors ||= Array.new(startup_size)
                errors[t[:index]] = t[:error]
              end
            end
          end

          raise ::RedisClient::Cluster::InitialSetupError, errors if node_info_list.nil?

          grouped = node_info_list.compact.group_by do |info_list|
            info_list
              .sort_by(&:id)
              .map { |i| "#{i.id}#{i.node_key}#{i.role}#{i.primary_id}#{i.config_epoch}" }
              .join
          end

          grouped.max_by { |_, v| v.size }[1].first
        end

        private

        # @see https://redis.io/commands/cluster-nodes/
        # @see https://github.com/redis/redis/blob/78960ad57b8a5e6af743d789ed8fd767e37d42b8/src/cluster.c#L4660-L4683
        def parse_cluster_node_reply(reply) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          rows = reply.split("\n").map(&:split)
          rows.each { |arr| arr[2] = arr[2].split(',') }
          rows.select! { |arr| arr[7] == 'connected' && (arr[2] & %w[fail? fail handshake noaddr noflags]).empty? }
          rows.each do |arr|
            arr[1] = arr[1].split('@').first
            arr[2] = (arr[2] & %w[master slave]).first
            if arr[8].nil?
              arr[8] = []
              next
            end
            arr[8] = arr[8..].filter_map { |str| str.start_with?('[') ? nil : str.split('-').map { |s| Integer(s) } }
                             .map { |a| a.size == 1 ? a << a.first : a }.map(&:sort)
          end

          rows.map do |arr|
            ::RedisClient::Cluster::Node::Info.new(
              id: arr[0], node_key: arr[1], role: arr[2], primary_id: arr[3], ping_sent: arr[4],
              pong_recv: arr[5], config_epoch: arr[6], link_state: arr[7], slots: arr[8]
            )
          end
        end
      end

      def initialize(
        options,
        node_info_list: [],
        with_replica: false,
        replica_affinity: :random,
        pool: nil,
        **kwargs
      )

        @slots = build_slot_node_mappings(node_info_list)
        @replications = build_replication_mappings(node_info_list)
        @topology = make_topology_class(with_replica, replica_affinity).new(@replications, options, pool, **kwargs)
        @mutex = Mutex.new
      end

      def inspect
        "#<#{self.class.name} #{node_keys.join(', ')}>"
      end

      def each(&block)
        @topology.clients.each_value(&block)
      end

      def sample
        @topology.clients.values.sample
      end

      def node_keys
        @topology.clients.keys.sort
      end

      def find_by(node_key)
        raise ReloadNeeded if node_key.nil? || !@topology.clients.key?(node_key)

        @topology.clients.fetch(node_key)
      end

      def call_all(method, command, args, &block)
        call_multiple_nodes!(@topology.clients, method, command, args, &block)
      end

      def call_primaries(method, command, args, &block)
        call_multiple_nodes!(@topology.primary_clients, method, command, args, &block)
      end

      def call_replicas(method, command, args, &block)
        call_multiple_nodes!(@topology.replica_clients, method, command, args, &block)
      end

      def send_ping(method, command, args, &block)
        result_values, errors = call_multiple_nodes(@topology.clients, method, command, args, &block)
        return result_values if errors.nil? || errors.empty?

        raise ReloadNeeded if errors.values.any?(::RedisClient::ConnectionError)

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end

      def clients_for_scanning(seed: nil)
        @topology.clients_for_scanning(seed: seed).values.sort_by { |c| "#{c.config.host}-#{c.config.port}" }
      end

      def find_node_key_of_primary(slot)
        return if slot.nil?

        slot = Integer(slot)
        return if slot < MIN_SLOT || slot > MAX_SLOT

        @slots[slot]
      end

      def find_node_key_of_replica(slot, seed: nil)
        primary_node_key = find_node_key_of_primary(slot)
        @topology.find_node_key_of_replica(primary_node_key, seed: seed)
      end

      def any_primary_node_key(seed: nil)
        @topology.any_primary_node_key(seed: seed)
      end

      def any_replica_node_key(seed: nil)
        @topology.any_replica_node_key(seed: seed)
      end

      def update_slot(slot, node_key)
        return if @mutex.locked?

        @mutex.synchronize do
          @slots[slot] = node_key
        rescue RangeError
          @slots = Array.new(SLOT_SIZE) { |i| @slots[i] }
          @slots[slot] = node_key
        end
      end

      private

      def make_topology_class(with_replica, replica_affinity)
        if with_replica && replica_affinity == :random
          ::RedisClient::Cluster::Node::RandomReplica
        elsif with_replica && replica_affinity == :latency
          ::RedisClient::Cluster::Node::LatencyReplica
        else
          ::RedisClient::Cluster::Node::PrimaryOnly
        end
      end

      def build_slot_node_mappings(node_info_list)
        slots = make_array_for_slot_node_mappings(node_info_list)
        node_info_list.each do |info|
          next if info.slots.nil? || info.slots.empty?

          info.slots.each { |start, last| (start..last).each { |i| slots[i] = info.node_key } }
        end

        slots
      end

      def make_array_for_slot_node_mappings(node_info_list)
        return Array.new(SLOT_SIZE) if node_info_list.count(&:primary?) > 256

        primary_node_keys = node_info_list.select(&:primary?).map(&:node_key)
        ::RedisClient::Cluster::Node::CharArray.new(SLOT_SIZE, primary_node_keys)
      end

      def build_replication_mappings(node_info_list) # rubocop:disable Metrics/AbcSize
        dict = node_info_list.to_h { |info| [info.id, info] }
        node_info_list.each_with_object(Hash.new { |h, k| h[k] = [] }) do |info, acc|
          primary_info = dict[info.primary_id]
          acc[primary_info.node_key] << info.node_key unless primary_info.nil?
          acc[info.node_key] if info.primary? # for the primary which have no replicas
        end
      end

      def call_multiple_nodes(clients, method, command, args, &block)
        results, errors = try_map(clients) do |_, client|
          client.public_send(method, *args, command, &block)
        end

        [results&.values, errors]
      end

      def call_multiple_nodes!(clients, method, command, args, &block)
        result_values, errors = call_multiple_nodes(clients, method, command, args, &block)
        return result_values if errors.nil? || errors.empty?

        raise ::RedisClient::Cluster::ErrorCollection, errors
      end

      def try_map(clients) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        results = errors = nil
        clients.each_slice(MAX_THREADS) do |chuncked_clients|
          threads = chuncked_clients.map do |k, v|
            Thread.new(k, v) do |node_key, client|
              Thread.current[:node_key] = node_key
              reply = yield(node_key, client)
              Thread.current[:result] = reply
            rescue StandardError => e
              Thread.current[:error] = e
            end
          end

          threads.each do |t|
            t.join
            if t.key?(:result)
              results ||= {}
              results[t[:node_key]] = t[:result]
            elsif t.key?(:error)
              errors ||= {}
              errors[t[:node_key]] = t[:error]
            end
          end
        end

        [results, errors]
      end
    end
  end
end
