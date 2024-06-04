# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'

module IpsPipeline
  module_function

  ATTEMPTS = 100

  def run
    on_demand = make_client(:on_demand)
    pooled = make_client(:pooled)
    none = make_client(:none)
    actor = make_client(:actor)
    envoy = make_client_for_envoy
    cluster_proxy = make_client_for_cluster_proxy
    prepare(on_demand, pooled, none, actor, envoy, cluster_proxy)
    print_letter('pipelined')
    bench(
      ondemand: on_demand,
      pooled: pooled,
      none: none,
      actor: actor,
      envoy: envoy,
      cproxy: cluster_proxy
    )
  end

  def make_client(model)
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      replica_affinity: :random,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      concurrency: { model: model },
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def make_client_for_envoy
    ::RedisClient.config(
      **TEST_GENERIC_OPTIONS.merge(BENCH_ENVOY_OPTIONS)
    ).new_client
  end

  def make_client_for_cluster_proxy
    ::RedisClient.config(
      **TEST_GENERIC_OPTIONS.merge(BENCH_REDIS_CLUSTER_PROXY_OPTIONS)
    ).new_client
  end

  def print_letter(title)
    print "################################################################################\n"
    print "# #{title}\n"
    print "################################################################################\n"
    print "\n"
  end

  def prepare(*clients)
    clients.each do |client|
      client.pipelined do |pi|
        ATTEMPTS.times do |i|
          pi.call('SET', "key#{i}", "val#{i}")
        end
      end
    end
  end

  def bench(**kwargs)
    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      kwargs.each do |key, client|
        x.report("pipelined: #{key}") do
          client.pipelined do |pi|
            ATTEMPTS.times do |i|
              pi.call('GET', "key#{i}")
            end
          end
        end
      end

      x.compare!
    end
  end
end

IpsPipeline.run
