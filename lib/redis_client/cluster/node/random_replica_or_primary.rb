# frozen_string_literal: true

require 'redis_client/cluster/node/replica_mixin'

class RedisClient
  class Cluster
    class Node
      class RandomReplicaOrPrimary
        include ::RedisClient::Cluster::Node::ReplicaMixin

        def replica_clients
          keys = @replications.values.filter_map(&:sample)
          @clients.select { |k, _| keys.include?(k) }
        end

        def clients_for_scanning(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          keys = @replications.map do |primary_node_key, replica_node_keys|
            decide_use_primary?(random, replica_node_keys.size) ? primary_node_key : replica_node_keys.sample(random: random)
          end

          clients.select { |k, _| keys.include?(k) }
        end

        def find_node_key_of_replica(primary_node_key, seed: nil)
          random = seed.nil? ? Random : Random.new(seed)

          replica_node_keys = @replications.fetch(primary_node_key, EMPTY_ARRAY)
          if decide_use_primary?(random, replica_node_keys.size)
            primary_node_key
          else
            replica_node_keys.sample(random: random) || primary_node_key
          end
        end

        def any_replica_node_key(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @replica_node_keys.sample(random: random) || any_primary_node_key(seed: seed)
        end

        private

        # Randomly equally likely choose node to read between primary and all replicas
        # e.g. 1 primary + 1 replica = 50% probability to read from primary
        # e.g. 1 primary + 2 replica = 33% probability to read from primary
        # e.g. 1 primary + 0 replica = 100% probability to read from primary
        def decide_use_primary?(random, replica_nodes)
          primary_nodes = 1.0
          total = primary_nodes + replica_nodes
          random.rand < primary_nodes / total
        end
      end
    end
  end
end
