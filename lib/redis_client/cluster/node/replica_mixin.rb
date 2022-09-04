# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      module ReplicaMixin
        attr_reader :clients, :primary_node_keys, :replica_node_keys, :primary_clients

        EMPTY_ARRAY = [].freeze

        def initialize(replications, options, pool, **kwargs)
          @replications = replications
          @primary_node_keys = @replications.keys.sort
          @replica_node_keys = @replications.values.flatten.sort

          @clients = build_clients(@replica_node_keys, options, pool, **kwargs)
          @primary_clients = @clients.select { |k, _| @primary_node_keys.include?(k) }
        end

        private

        def build_clients(replica_node_keys, options, pool, **kwargs)
          options.filter_map do |node_key, option|
            option = option.merge(kwargs.reject { |k, _| ::RedisClient::Cluster::Node::IGNORE_GENERIC_CONFIG_KEYS.include?(k) })
            config = ::RedisClient::Cluster::Node::Config.new(scale_read: replica_node_keys.include?(node_key), **option)
            client = pool.nil? ? config.new_client : config.new_pool(**pool)
            [node_key, client]
          end.to_h
        end

        def select_first_clients(replications, clients)
          first_keys = replications.values.map(&:first)
          clients.select { |k, _| first_keys.include?(k) }
        end
      end
    end
  end
end
