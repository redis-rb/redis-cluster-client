# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'

module BenchPipeline
  module_function

  ATTEMPTS = 100

  def run
    on_demand = make_client(:on_demand)
    pooled = make_client(:pooled)
    prepare(on_demand, pooled)
    bench(on_demand, pooled)
  end

  def make_client(model)
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      concurrent_worker_model: model,
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def prepare(on_demand, pooled)
    on_demand.pipelined do |pi|
      ATTEMPTS.times { |i| pi.call('SET', "key#{i}", "val#{i}") }
    end

    pooled.pipelined do |pi|
      ATTEMPTS.times { |i| pi.call('SET', "key#{i}", "val#{i}") }
    end
  end

  def bench(on_demand, pooled)
    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      x.report('on_demand') do
        on_demand.pipelined { |pi| ATTEMPTS.times { |i| pi.call('GET', "key#{i}") } }
      end

      x.report('pooled') do
        pooled.pipelined { |pi| ATTEMPTS.times { |i| pi.call('GET', "key#{i}") } }
      end

      x.compare!
    end
  end
end

BenchPipeline.run
