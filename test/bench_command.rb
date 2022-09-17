# frozen_string_literal: true

require 'benchmark_helper'
require 'benchmark_mixin'

class BenchCommand
  class PrimaryOnly < BenchmarkWrapper
    include BenchmarkMixin

    private

    def new_test_client
      config = ::RedisClient::ClusterConfig.new(
        nodes: TEST_NODE_URIS,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      )
      ::RedisClient::Cluster.new(config)
    end
  end

  class ScaleReadRandom < BenchmarkWrapper
    include BenchmarkMixin

    private

    def new_test_client
      config = ::RedisClient::ClusterConfig.new(
        nodes: TEST_NODE_URIS,
        replica: true,
        replica_affinity: :random,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      )
      ::RedisClient::Cluster.new(config)
    end
  end

  class ScaleReadLatency < BenchmarkWrapper
    include BenchmarkMixin

    private

    def new_test_client
      config = ::RedisClient::ClusterConfig.new(
        nodes: TEST_NODE_URIS,
        replica: true,
        replica_affinity: :latency,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      )
      ::RedisClient::Cluster.new(config)
    end
  end

  class Pooled < BenchmarkWrapper
    include BenchmarkMixin

    private

    def new_test_client
      config = ::RedisClient::ClusterConfig.new(
        nodes: TEST_NODE_URIS,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      )
      ::RedisClient::Cluster.new(config, pool: { timeout: TEST_TIMEOUT_SEC, size: 2 })
    end
  end

  class Envoy
    include BenchmarkMixin

    private

    def new_test_client
      cfg = TEST_GENERIC_OPTIONS.merge(port: 7000, protocol: 2)
      ::RedisClient.config(**cfg).new_client
    end
  end
end
