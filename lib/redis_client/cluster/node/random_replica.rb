# frozen_string_literal: true

require 'redis_client/cluster/node/replica_mixin'

class RedisClient
  class Cluster
    class Node
      class RandomReplica
        include ::RedisClient::Cluster::Node::ReplicaMixin

        attr_reader :clients_for_scanning

        def initialize(replications, options, pool, **kwargs)
          super

          first_keys = @replications.values.map(&:first)
          @clients_for_scanning = @clients.select { |k, _| first_keys.include?(k) }
        end

        def replica_clients
          keys = @replications.values.map(&:sample)
          @clients.select { |k, _| keys.include?(k) }
        end

        def find_node_key_of_replica(primary_node_key)
          @replications.fetch(primary_node_key, EMPTY_ARRAY).sample
        end
      end
    end
  end
end
