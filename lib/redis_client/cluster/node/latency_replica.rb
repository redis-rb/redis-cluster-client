# frozen_string_literal: true

require 'redis_client/cluster/node/base_topology'

class RedisClient
  class Cluster
    class Node
      class LatencyReplica < BaseTopology
        DUMMY_LATENCY_MSEC = 100 * 1000 * 1000
        MEASURE_ATTEMPT_COUNT = 10

        private_constant :DUMMY_LATENCY_MSEC, :MEASURE_ATTEMPT_COUNT

        def clients_for_scanning(seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          @clients_for_scanning
        end

        def find_node_key_of_replica(primary_node_key, seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          @replications.fetch(primary_node_key, EMPTY_ARRAY).first || primary_node_key
        end

        def any_replica_node_key(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @existed_replicas.sample(random: random)&.first || any_primary_node_key(seed: seed)
        end

        def process_topology_update!(replications, options)
          super

          all_replica_clients = @clients.select { |k, _| @replica_node_keys.include?(k) }
          latencies = measure_latencies(all_replica_clients, @concurrent_worker)
          @replications.each_value { |keys| keys.sort_by! { |k| latencies.fetch(k) } }
          @replica_clients = select_replica_clients(@replications, @clients)
          @clients_for_scanning = select_clients_for_scanning(@replications, @clients)
          @existed_replicas = @replications.values.reject(&:empty?)
        end

        private

        def measure_latencies(clients, concurrent_worker) # rubocop:disable Metrics/AbcSize
          return {} if clients.empty?

          work_group = concurrent_worker.new_group(size: clients.size)

          clients.each do |node_key, client|
            work_group.push(node_key, client) do |cli|
              min = DUMMY_LATENCY_MSEC
              MEASURE_ATTEMPT_COUNT.times do
                starting = obtain_current_time
                cli.call_once('PING')
                duration = obtain_current_time - starting
                min = duration if duration < min
              end

              min
            rescue StandardError
              DUMMY_LATENCY_MSEC
            end
          end

          latencies = {}
          work_group.each { |node_key, v| latencies[node_key] = v }
          work_group.close
          latencies
        end

        def obtain_current_time
          Process.clock_gettime(Process::CLOCK_MONOTONIC, :microsecond)
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
