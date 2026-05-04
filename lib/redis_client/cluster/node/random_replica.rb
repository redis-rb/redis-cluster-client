# frozen_string_literal: true

require 'redis_client/cluster/node/base_topology'

class RedisClient
  class Cluster
    class Node
      class RandomReplica < BaseTopology
        def replica_clients
          keys = @replications.values.filter_map(&:sample)
          @clients.select { |k, _| keys.include?(k) }
        end

        def clients_for_scanning(seed: nil)
          random = make_random(seed)
          keys = @replications.map do |primary_node_key, replica_node_keys|
            replica_node_keys.empty? ? primary_node_key : replica_node_keys.sample(random: random)
          end

          clients.select { |k, _| keys.include?(k) }
        end

        def find_node_key_of_replica(primary_node_key, seed: nil)
          replica_node_keys = @replications.fetch(primary_node_key, EMPTY_ARRAY)
          replica_node_key = replica_node_keys.size <= 1 ? replica_node_keys.first : replica_node_keys.sample(random: make_random(seed))
          replica_node_key || primary_node_key
        end

        def any_replica_node_key(seed: nil)
          @replica_node_keys.sample(random: make_random(seed)) || any_primary_node_key(seed: seed)
        end
      end
    end
  end
end
