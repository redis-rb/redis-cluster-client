# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      class PrimaryOnly
        attr_reader :clients, :primary_node_keys

        def initialize(replications, options, pool, **kwargs)
          @replications = replications
          @primary_node_keys = @replications.keys.sort

          @clients = build_clients(@primary_node_keys, options, pool, **kwargs)
        end

        alias replica_node_keys primary_node_keys
        alias clients_for_scanning clients
        alias primary_clients clients
        alias replica_clients clients

        def find_node_key_of_replica(primary_node_key)
          primary_node_key
        end

        private

        def build_clients(primary_node_keys, options, pool, **kwargs)
          options.filter_map do |node_key, option|
            next if !primary_node_keys.empty? && !primary_node_keys.include?(node_key)

            option = option.merge(kwargs.reject { |k, _| ::RedisClient::Cluster::Node::IGNORE_GENERIC_CONFIG_KEYS.include?(k) })
            config = ::RedisClient::Cluster::Node::Config.new(**option)
            client = pool.nil? ? config.new_client : config.new_pool(**pool)
            [node_key, client]
          end.to_h
        end
      end
    end
  end
end
