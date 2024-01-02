# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      class BaseTopology
        IGNORE_GENERIC_CONFIG_KEYS = %i[url host port path].freeze
        attr_reader :clients, :primary_clients, :replica_clients

        def initialize(pool, concurrent_worker, **kwargs)
          @pool = pool
          @clients = {}
          @client_options = kwargs.reject { |k, _| IGNORE_GENERIC_CONFIG_KEYS.include?(k) }
          @concurrent_worker = concurrent_worker
          @replications = EMPTY_HASH
          @primary_node_keys = EMPTY_ARRAY
          @replica_node_keys = EMPTY_ARRAY
          @primary_clients = EMPTY_ARRAY
          @replica_clients = EMPTY_ARRAY
        end

        def any_primary_node_key(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @primary_node_keys.sample(random: random)
        end

        def process_topology_update!(replications, options) # rubocop:disable Metrics/AbcSize
          @replications = replications.freeze
          @primary_node_keys = @replications.keys.sort.select { |k| options.key?(k) }.freeze
          @replica_node_keys = @replications.values.flatten.sort.select { |k| options.key?(k) }.freeze

          # Disconnect from nodes that we no longer want, and connect to nodes we're not connected to yet
          disconnect_from_unwanted_nodes(options)
          connect_to_new_nodes(options)

          @primary_clients, @replica_clients = @clients.partition { |k, _| @primary_node_keys.include?(k) }.map(&:to_h)
          @primary_clients.freeze
          @replica_clients.freeze
        end

        private

        def disconnect_from_unwanted_nodes(options)
          (@clients.keys - options.keys).each do |node_key|
            @clients.delete(node_key).close
          end
        end

        def connect_to_new_nodes(options)
          (options.keys - @clients.keys).each do |node_key|
            option = options[node_key].merge(@client_options)
            config = ::RedisClient::Cluster::Node::Config.new(scale_read: !@primary_node_keys.include?(node_key), **option)
            client = @pool.nil? ? config.new_client : config.new_pool(**@pool)
            @clients[node_key] = client
          end
        end
      end
    end
  end
end
