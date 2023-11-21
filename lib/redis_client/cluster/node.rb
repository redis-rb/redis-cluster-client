# frozen_string_literal: true

require 'redis_client'
require 'redis_client/config'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/node/primary_only'
require 'redis_client/cluster/node/random_replica'
require 'redis_client/cluster/node/random_replica_or_primary'
require 'redis_client/cluster/node/latency_replica'

class RedisClient
  class Cluster
    class Node
      include Enumerable

      # It affects to strike a balance between load and stability in initialization or changed states.
      MAX_STARTUP_SAMPLE = Integer(ENV.fetch('REDIS_CLIENT_MAX_STARTUP_SAMPLE', 3))

      # less memory consumption, but slow
      USE_CHAR_ARRAY_SLOT = Integer(ENV.fetch('REDIS_CLIENT_USE_CHAR_ARRAY_SLOT', 1)) == 1

      SLOT_SIZE = 16_384
      MIN_SLOT = 0
      MAX_SLOT = SLOT_SIZE - 1
      IGNORE_GENERIC_CONFIG_KEYS = %i[url host port path].freeze
      DEAD_FLAGS = %w[fail? fail handshake noaddr noflags].freeze
      ROLE_FLAGS = %w[master slave].freeze
      EMPTY_ARRAY = [].freeze

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
          super(**kwargs, client_implementation: Client)
        end

        private

        def build_connection_prelude
          prelude = super.dup
          prelude << ['READONLY'] if @scale_read
          prelude.freeze
        end
      end

      class Client < ::RedisClient
        # We need to be able to differentiate between timeout errors caused by blocking read timeouts
        # (which should NOT cause a cluster topology update) with normal read timeouts (which should)
        def blocking_call(timeout, *command, **kwargs)
          super
        rescue ::RedisClient::TimeoutError => e
          raise ::RedisClient::Cluster::BlockingReadTimeoutError, e.message
        end

        def blocking_call_v(timeout, command)
          super
        rescue ::RedisClient::TimeoutError => e
          raise ::RedisClient::Cluster::BlockingReadTimeoutError, e.message
        end
      end

      class << self
        def load_info(options, concurrent_worker, slow_command_timeout: -1, **kwargs) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          raise ::RedisClient::Cluster::InitialSetupError, [] if options.nil? || options.empty?

          startup_size = options.size > MAX_STARTUP_SAMPLE ? MAX_STARTUP_SAMPLE : options.size
          startup_options = options.to_a.sample(startup_size).to_h
          startup_nodes = ::RedisClient::Cluster::Node.new(startup_options, concurrent_worker, **kwargs)
          work_group = concurrent_worker.new_group(size: startup_size)

          startup_nodes.each_with_index do |raw_client, i|
            work_group.push(i, raw_client) do |client|
              regular_timeout = client.read_timeout
              client.read_timeout = slow_command_timeout > 0.0 ? slow_command_timeout : regular_timeout
              reply = client.call('CLUSTER', 'NODES')
              client.read_timeout = regular_timeout
              parse_cluster_node_reply(reply)
            rescue StandardError => e
              e
            ensure
              client&.close
            end
          end

          node_info_list = errors = nil

          work_group.each do |i, v|
            case v
            when StandardError
              errors ||= Array.new(startup_size)
              errors[i] = v
            else
              node_info_list ||= Array.new(startup_size)
              node_info_list[i] = v
            end
          end

          work_group.close

          raise ::RedisClient::Cluster::InitialSetupError, errors if node_info_list.nil?

          grouped = node_info_list.compact.group_by do |info_list|
            info_list.sort_by!(&:id)
            info_list.each_with_object(String.new(capacity: 128 * info_list.size)) do |e, a|
              a << e.id << e.node_key << e.role << e.primary_id << e.config_epoch
            end
          end

          grouped.max_by { |_, v| v.size }[1].first.freeze
        end

        private

        # @see https://redis.io/commands/cluster-nodes/
        # @see https://github.com/redis/redis/blob/78960ad57b8a5e6af743d789ed8fd767e37d42b8/src/cluster.c#L4660-L4683
        def parse_cluster_node_reply(reply) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          reply.each_line("\n", chomp: true).filter_map do |line|
            fields = line.split
            flags = fields[2].split(',')
            next unless fields[7] == 'connected' && (flags & DEAD_FLAGS).empty?

            slots = if fields[8].nil?
                      EMPTY_ARRAY
                    else
                      fields[8..].reject { |str| str.start_with?('[') }
                                 .map { |str| str.split('-').map { |s| Integer(s) } }
                                 .map { |a| a.size == 1 ? a << a.first : a }
                                 .map(&:sort)
                    end

            ::RedisClient::Cluster::Node::Info.new(
              id: fields[0],
              node_key: parse_node_key(fields[1]),
              role: (flags & ROLE_FLAGS).first,
              primary_id: fields[3],
              ping_sent: fields[4],
              pong_recv: fields[5],
              config_epoch: fields[6],
              link_state: fields[7],
              slots: slots
            )
          end
        end

        # As redirection node_key is dependent on `cluster-preferred-endpoint-type` config,
        # node_key should use hostname if present in CLUSTER NODES output.
        #
        # See https://redis.io/commands/cluster-nodes/ for details on the output format.
        # node_address matches fhe format: <ip:port@cport[,hostname[,auxiliary_field=value]*]>
        def parse_node_key(node_address)
          ip_chunk, hostname, _auxiliaries = node_address.split(',')
          ip_port_string = ip_chunk.split('@').first
          return ip_port_string if hostname.nil? || hostname.empty?

          port = ip_port_string.split(':')[1]
          "#{hostname}:#{port}"
        end
      end

      def initialize(
        options,
        concurrent_worker,
        node_info_list: [],
        with_replica: false,
        replica_affinity: :random,
        pool: nil,
        **kwargs
      )

        @concurrent_worker = concurrent_worker
        @slots = build_slot_node_mappings(node_info_list)
        @replications = build_replication_mappings(node_info_list)
        klass = make_topology_class(with_replica, replica_affinity)
        @topology = klass.new(@replications, options, pool, @concurrent_worker, **kwargs)
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

      def replica_clients
        @topology.replica_clients.values
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
        elsif with_replica && replica_affinity == :random_with_primary
          ::RedisClient::Cluster::Node::RandomReplicaOrPrimary
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
        return Array.new(SLOT_SIZE) if !USE_CHAR_ARRAY_SLOT || node_info_list.count(&:primary?) > 256

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

      def try_map(clients, &block) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
        return [{}, {}] if clients.empty?

        work_group = @concurrent_worker.new_group(size: clients.size)

        clients.each do |node_key, client|
          work_group.push(node_key, node_key, client, block) do |nk, cli, blk|
            blk.call(nk, cli)
          rescue StandardError => e
            e
          end
        end

        results = errors = nil

        work_group.each do |node_key, v|
          case v
          when StandardError
            errors ||= {}
            errors[node_key] = v
          else
            results ||= {}
            results[node_key] = v
          end
        end

        work_group.close

        [results, errors]
      end
    end
  end
end
