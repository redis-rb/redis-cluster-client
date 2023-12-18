# frozen_string_literal: true

require 'redis_client/cluster/node/base_topology'

class RedisClient
  class Cluster
    class Node
      class PrimaryOnly < BaseTopology
        alias primary_clients clients
        alias replica_clients clients

        def clients_for_scanning(seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          @clients
        end

        def find_node_key_of_replica(primary_node_key, seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          primary_node_key
        end

        def any_primary_node_key(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @primary_node_keys.sample(random: random)
        end

        alias any_replica_node_key any_primary_node_key

        def process_topology_update!(replications, options)
          # Remove non-primary nodes from options (provided that we actually have any primaries at all)
          options = options.select { |node_key, _| replications.key?(node_key) } if replications.keys.any?
          super(replications, options)
        end
      end
    end
  end
end
