# frozen_string_literal: true

require 'benchmark_helper'
require 'benchmark_mixin'

class BenchCommand
  class PrimaryOnly < BenchmarkWrapper
    include BenchmarkMixin

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      ).new_client
    end
  end

  class ScaleReadRandom < BenchmarkWrapper
    include BenchmarkMixin

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        replica: true,
        replica_affinity: :random,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      ).new_client
    end
  end

  class ScaleReadLatency < BenchmarkWrapper
    include BenchmarkMixin

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        replica: true,
        replica_affinity: :latency,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      ).new_client
    end
  end

  class Pooled < BenchmarkWrapper
    include BenchmarkMixin

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      ).new_pool(timeout: TEST_TIMEOUT_SEC, size: 2)
    end
  end

  class Envoy < BenchmarkWrapper
    include BenchmarkMixin

    def setup
      @client = new_test_client
      @cluster_client = new_cluster_client
      @cluster_client.call('FLUSHDB')
      wait_for_replication
    end

    def teardown
      @cluster_client.call('FLUSHDB')
      wait_for_replication
      @cluster_client&.close
      @client&.close
    end

    # https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_protocols/redis#supported-commands
    def bench_echo
      skip('Envoy does not support ECHO command.')
    end

    def bench_pipeline_echo
      skip('Envoy does not support ECHO command.')
    end

    private

    def new_test_client
      ::RedisClient.config(**TEST_GENERIC_OPTIONS.merge(BENCH_PROXY_OPTIONS)).new_client
    end

    def new_cluster_client
      ::RedisClient.cluster(nodes: TEST_NODE_URIS, fixed_hostname: TEST_FIXED_HOSTNAME, **TEST_GENERIC_OPTIONS).new_client
    end

    def wait_for_replication
      client_side_timeout = TEST_TIMEOUT_SEC + 1.0
      server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
      @cluster_client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
    end
  end
end
