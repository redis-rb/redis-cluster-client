# frozen_string_literal: true

require 'redis_client/cluster/node/replica_mixin'

class RedisClient
  class Cluster
    class Node
      class LatencyReplica
        include ::RedisClient::Cluster::Node::ReplicaMixin

        attr_reader :replica_clients, :clients_for_scanning

        DUMMY_LATENCY_SEC = 100.0

        def initialize(replications, options, pool, **kwargs)
          super

          all_replica_clients = @clients.select { |k, _| @replica_node_keys.include?(k) }
          latencies = measure_latencies(all_replica_clients)
          @replications.each_value { |keys| keys.sort_by! { |k| latencies.fetch(k) } }
          @clients_for_scanning = @replica_clients = select_first_clients(@replications, @clients)
        end

        def find_node_key_of_replica(primary_node_key)
          @replications.fetch(primary_node_key, EMPTY_ARRAY).first
        end

        private

        def measure_latencies(clients) # rubocop:disable Metrics/MethodLength
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
