# frozen_string_literal: true

require 'redis_client/cluster/node/replica_mixin'

class RedisClient
  class Cluster
    class Node
      class LatencyReplica
        include ::RedisClient::Cluster::Node::ReplicaMixin

        attr_reader :replica_clients

        DUMMY_LATENCY_SEC = 100.0
        MEASURE_ATTEMPT_COUNT = 10

        def initialize(replications, options, pool, **kwargs)
          super

          all_replica_clients = @clients.select { |k, _| @replica_node_keys.include?(k) }
          latencies = measure_latencies(all_replica_clients)
          @replications.each_value { |keys| keys.sort_by! { |k| latencies.fetch(k) } }
          @replica_clients = select_replica_clients(@replications, @clients)
          @clients_for_scanning = select_clients_for_scanning(@replications, @clients)
        end

        def clients_for_scanning(seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          @clients_for_scanning
        end

        def find_node_key_of_replica(primary_node_key, seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          @replications.fetch(primary_node_key, EMPTY_ARRAY).first || primary_node_key
        end

        def any_replica_node_key(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @replications.reject { |_, v| v.empty? }.values.sample(random: random).first
        end

        private

        def measure_latencies(clients) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
          latencies = {}

          clients.each_slice(::RedisClient::Cluster::Node::MAX_THREADS) do |chuncked_clients|
            threads = chuncked_clients.map do |k, v|
              Thread.new(k, v) do |node_key, client|
                Thread.pass
                min = DUMMY_LATENCY_SEC + 1.0
                MEASURE_ATTEMPT_COUNT.times do
                  starting = Process.clock_gettime(Process::CLOCK_MONOTONIC)
                  client.send(:call_once, 'PING')
                  duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - starting
                  min = duration if duration < min
                end
                latencies[node_key] = min
              rescue StandardError
                latencies[node_key] = DUMMY_LATENCY_SEC
              end
            end

            threads.each(&:join)
          end

          latencies
        end

        def select_replica_clients(replications, clients)
          keys = replications.values.filter_map(&:first)
          clients.select { |k, _| keys.include?(k) }
        end

        def select_clients_for_scanning(replications, clients)
          keys = replications.map do |primary_node_key, replica_node_keys|
            replica_node_keys.empty? ? primary_node_key : replica_node_keys.first
          end

          clients.select { |k, _| keys.include?(k) }
        end
      end
    end
  end
end
