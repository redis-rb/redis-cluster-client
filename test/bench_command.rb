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
    include BenchmarkMixinForProxy

    # https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_protocols/redis#supported-commands
    def bench_echo
      skip('Envoy does not support ECHO command.')
    end

    def bench_pipeline_echo
      skip('Envoy does not support ECHO command.')
    end

    private

    def new_test_client
      ::RedisClient.config(**TEST_GENERIC_OPTIONS.merge(BENCH_ENVOY_OPTIONS)).new_client
    end
  end

  class RedisClusterProxy < BenchmarkWrapper
    include BenchmarkMixin
    include BenchmarkMixinForProxy

    private

    def new_test_client
      ::RedisClient.config(**TEST_GENERIC_OPTIONS.merge(BENCH_REDIS_CLUSTER_PROXY_OPTIONS)).new_client
    end
  end
end
