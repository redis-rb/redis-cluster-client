# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      class RandomReplica
        include ::RedisClient::Cluster::Node::ReplicaMixin

        def replica_clients
          keys = @replications.values.map(&:sample)
          @clients.select { |k, _| keys.include?(k) }
        end

        def find_node_key_of_replica(primary_node_key)
          @replications[primary_node_key].sample
        end
      end
    end
  end
end
