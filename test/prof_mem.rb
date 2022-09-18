# frozen_string_literal: true

require 'memory_profiler'
require 'redis_cluster_client'
require 'testing_constants'

module ProfMem
  module_function

  ATTEMPT_COUNT = 1000

  def run
    %w[primary_only scale_read_random scale_read_latency pooled].each do |cli_type|
      p cli_type

      profile do
        send("new_#{cli_type}_client".to_sym).pipelined do |pi|
          ATTEMPT_COUNT.times { |i| pi.call('SET', "key#{i}", i) }
          ATTEMPT_COUNT.times { |i| pi.call('GET', "key#{i}") }
        end
      end
    end
  end

  def profile(&block)
    # https://github.com/SamSaffron/memory_profiler
    report = ::MemoryProfiler.report(top: 10, &block)
    report.pretty_print(color_output: true, normalize_paths: true)
  end

  def new_primary_only_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def new_scale_read_random_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      replica_affinity: :random,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def new_scale_read_latency_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      replica_affinity: :latency,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def new_pooled_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_pool(
      timeout: TEST_TIMEOUT_SEC,
      size: 2
    )
  end
end

ProfMem.run
