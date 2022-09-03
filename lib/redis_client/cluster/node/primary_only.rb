# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      class PrimaryOnly
        attr_reader :clients, :primary_node_keys

        def initialize(replications, options, pool, **kwargs)
          @replications = replications
          @clients = build_clients(@replications, options, pool, **kwargs)
          @primary_node_keys = @replications.keys.sort
        end

        alias replica_node_keys primary_node_keys
        alias fixed_clients clients
        alias primary_clients clients
        alias replica_clients clients

        def find_node_key_of_replica(primary_node_key)
          primary_node_key
        end

        private

        def build_clients(replications, options, pool, **kwargs)
          options.filter_map do |node_key, option|
            next unless replications.key?(node_key)

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
