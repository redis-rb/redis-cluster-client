# frozen_string_literal: true

require 'redis_client/cluster/node/replica_mixin'

class RedisClient
  class Cluster
    class Node
      class RandomReplica
        include ::RedisClient::Cluster::Node::ReplicaMixin

        def replica_clients
          keys = @replications.values.filter_map(&:sample)
          @clients.select { |k, _| keys.include?(k) }
        end

        def clients_for_scanning(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          keys = @replications.map do |primary_node_key, replica_node_keys|
            replica_node_keys.empty? ? primary_node_key : replica_node_keys.sample(random: random)
          end

          clients.select { |k, _| keys.include?(k) }
        end

        def find_node_key_of_replica(primary_node_key, seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @replications.fetch(primary_node_key, EMPTY_ARRAY).sample(random: random) || primary_node_key
        end

        def any_replica_node_key(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @replica_node_keys.sample(random: random) || any_primary_node_key(seed: seed)
        end
      end
    end
  end
end
