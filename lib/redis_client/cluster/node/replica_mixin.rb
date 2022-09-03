# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      module ReplicaMixin
        attr_reader :clients, :primary_node_keys, :replica_node_keys, :fixed_clients, :primary_clients

        def initialize(replications, options, pool, **kwargs)
          @replications = replications
          @clients = build_clients(options, pool, **kwargs)
          @primary_node_keys = @replications.keys.sort
          @replica_node_keys = @replications.values.flatten.sort
          first_keys = @replications.values.map(&:first)
          @fixed_clients = @clients.select { |k, _| first_keys.include?(k) }
          @primary_clients = @clients.select { |k, _| @primary_node_keys.include?(k) }
        end

        private

        def build_clients(options, pool, **kwargs)
          options.filter_map do |node_key, option|
            config = ::RedisClient::Cluster::Node::Config.new(
              scale_read: replica?(node_key),
              **option.merge(kwargs.reject { |k, _| ::RedisClient::Cluster::Node::IGNORE_GENERIC_CONFIG_KEYS.include?(k) })
            )
            client = pool.nil? ? config.new_client : config.new_pool(**pool)

            [node_key, client]
          end.to_h
        end
      end
    end
  end
end
