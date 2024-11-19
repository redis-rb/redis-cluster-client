# frozen_string_literal: true

require 'async/redis/cluster_client'
require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'

module IpsSingle
  module_function

  ATTEMPTS = 10

  def run
    cli = make_client
    envoy = make_client_for_envoy
    cluster_proxy = make_client_for_cluster_proxy
    prepare(cli, envoy, cluster_proxy)
    print_letter('single')
    bench(
      cli: cli,
      envoy: envoy,
      cproxy: cluster_proxy
    )
    async_bench(make_async_client)
  end

  def make_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      replica_affinity: :random,
      fixed_hostname: TEST_FIXED_HOSTNAME,
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

  def make_async_client
    endpoints = TEST_NODE_URIS.map { |e| ::Async::Redis::Endpoint.parse(e) }
    ::Async::Redis::ClusterClient.new(endpoints)
  end

  def print_letter(title)
    print "################################################################################\n"
    print "# #{title}\n"
    print "################################################################################\n"
    print "\n"
  end

  def prepare(*clients)
    clients.each do |client|
      ATTEMPTS.times do |i|
        client.call('set', "key#{i}", "val#{i}")
      end
    end
  end

  def bench(**kwargs)
    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      kwargs.each do |key, client|
        x.report("single: #{key}") do
          ATTEMPTS.times do |i|
            client.call('get', "key#{i}")
          end
        end
      end

      x.compare!
    end
  end

  def async_bench(cluster)
    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      x.report('single: async') do
        Async do
          ATTEMPTS.times do |i|
            key = "key#{i}"
            slot = cluster.slot_for(key)
            client = cluster.client_for(slot)
            client.get(key)
          end
        end
      end

      x.compare!
    end
  end
end

IpsSingle.run
