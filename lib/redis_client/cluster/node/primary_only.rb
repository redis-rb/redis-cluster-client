# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      class PrimaryOnly
        attr_reader :clients

        def initialize(replications, options, pool, **kwargs)
          @primary_node_keys = replications.keys.sort
          @clients = build_clients(@primary_node_keys, options, pool, **kwargs)
        end

        alias primary_clients clients
        alias replica_clients clients

        def clients_for_scanning(random: Random) # rubocop:disable Lint/UnusedMethodArgument
          @clients
        end

        def find_node_key_of_replica(primary_node_key, random: Random) # rubocop:disable Lint/UnusedMethodArgument
          primary_node_key
        end

        def any_primary_node_key(random: Random)
          @primary_node_keys.sample(random: random)
        end

        alias any_replica_node_key any_primary_node_key

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
