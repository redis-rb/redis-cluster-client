# frozen_string_literal: true

require 'memory_profiler'
require 'redis_cluster_client'
require 'testing_constants'

module ProfMem
  module_function

  ATTEMPT_COUNT = 1000
  MAX_PIPELINE_SIZE = 30
  SLICED_NUMBERS = Array.new(ATTEMPT_COUNT) { |i| i }.each_slice(MAX_PIPELINE_SIZE).freeze
  CLI_TYPES = %w[primary_only scale_read_random scale_read_latency pooled].freeze
  MODES = {
    single: lambda do |cli|
      ATTEMPT_COUNT.times { |i| cli.call('SET', i, i) }
      ATTEMPT_COUNT.times { |i| cli.call('GET', i) }
    end,
    excessive_pipelining: lambda do |cli|
      cli.pipelined do |pi|
        ATTEMPT_COUNT.times { |i| pi.call('SET', i, i) }
      end

      cli.pipelined do |pi|
        ATTEMPT_COUNT.times { |i| pi.call('GET', i) }
      end
    end,
    pipelining_in_moderation: lambda do |cli|
      SLICED_NUMBERS.each do |numbers|
        cli.pipelined do |pi|
          numbers.each { |i| pi.call('SET', i, i) }
        end

        cli.pipelined do |pi|
          numbers.each { |i| pi.call('GET', i) }
        end
      end
    end
  }.freeze

  def run
    mode = ENV.fetch('MEMORY_PROFILE_MODE', :single).to_sym
    subject = MODES.fetch(mode)

    CLI_TYPES.each do |cli_type|
      prepare
      print_letter(mode, cli_type)
      client = send("new_#{cli_type}_client".to_sym)
      profile { subject.call(client) }
    end
  end

  def prepare
    ::RedisClient::Cluster::NormalizedCmdName.instance.clear
  end

  def print_letter(title, sub_titile)
    print "################################################################################\n"
    print "# #{title}: #{sub_titile}\n"
    print "################################################################################\n"
    print "\n"
  end

  def profile(&block)
    # https://github.com/SamSaffron/memory_profiler
    report = ::MemoryProfiler.report(top: 20, &block)
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
