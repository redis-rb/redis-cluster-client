# frozen_string_literal: true

require 'benchmark_helper'

class BenchCommand
  module Mixin
    def setup
      @client = new_test_client
      @client.call('FLUSHDB')
      wait_for_replication
    end

    def teardown
      @client.call('FLUSHDB')
      wait_for_replication
      @client&.close
    end

    def bench_ping
      assert_performance_linear do |n|
        n.times do
          @client.call('PING')
        end
      end
    end

    def bench_set
      assert_performance_linear do |n|
        n.times do |i|
          @client.call('SET', "key#{i}", i)
        end
      end
    end

    def bench_get
      assert_performance_linear do |n|
        n.times do |i|
          @client.call('GET', "key#{i}")
        end
      end
    end

    def bench_pipeline_ping
      assert_performance_linear do |n|
        @client.pipelined do |pi|
          n.times do
            pi.call('PING')
          end
        end
      end
    end

    def bench_pipeline_set
      assert_performance_linear do |n|
        @client.pipelined do |pi|
          n.times do |i|
            pi.call('SET', "key#{i}", i)
          end
        end
      end
    end

    def bench_pipeline_get
      assert_performance_linear do |n|
        @client.pipelined do |pi|
          n.times do |i|
            pi.call('GET', "key#{i}")
          end
        end
      end
    end

    private

    def wait_for_replication
      client_side_timeout = TEST_TIMEOUT_SEC + 1.0
      server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
      @client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
    end
  end

  class PrimaryOnly < BenchmarkWrapper
    include Mixin

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
    include Mixin

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
    include Mixin

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
    include Mixin

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
end
