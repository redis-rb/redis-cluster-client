# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      class NearestReplica
        include ::RedisClient::Cluster::Node::ReplicaMixin

        attr_reader :replica_clients

        DUMMY_LATENCY_SEC = 100.0

        def initialize(replications, options, pool, **kwargs)
          super

          all_replica_clients = @clients.select { |k, _| @replica_node_keys.include?(k) }
          @latencies = measure_latency_of_replicas(all_replica_clients)

          nearest_replica_keys = @replications.values.map do |replica_node_keys|
            replica_node_keys.min_by { |k| @latencies.fetch(k) }
          end

          @replica_clients = @clients.select { |k, _| nearest_replica_keys.include?(k) }
        end

        def find_node_key_of_replica(primary_node_key)
          @replications[primary_node_key].min_by { |replica_node_key| @latencies.fetch(replica_node_key) }
        end

        private

        def measure_latency_of_replicas(clients) # rubocop:disable Metrics/MethodLength
          latencies = {}

          clients.each_slice(::RedisClient::Cluster::Node::MAX_THREADS * 2) do |chuncked_clients|
            threads = chuncked_clients.map do |k, v|
              Thraed.new(k, v) do |node_key, client|
                Thread.pass
                starting = Process.clock_gettime(Process::CLOCK_MONOTONIC)
                client.send(:call_once, 'PING')
                ending = Process.clock_gettime(Process::CLOCK_MONOTONIC)
                latencies[node_key] = ending - starting
              rescue StandardError
                latencies[node_key] = DUMMY_LATENCY_SEC
              end
            end

            threads.each(&:join)
          end

          latencies
        end
      end
    end
  end
end
