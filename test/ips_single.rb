# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'
require 'async/redis/cluster_client'
require 'valkey'

module IpsSingle
  module_function

  ATTEMPTS = 10

  def run
    print_letter('single')
    cli = make_client
    prepare(cli)
    bench(
      cli: cli,
      envoy: make_client_for_envoy,
      cproxy: make_client_for_cluster_proxy
    )
    async_bench(make_async_client)
    valkey_bench(make_valkey_client)
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

  def make_valkey_client
    ::Valkey.new(
      nodes: [{ host: TEST_REDIS_HOST, port: TEST_REDIS_PORT }],
      timeout: TEST_TIMEOUT_SEC,
      cluster_mode: true,
      protocol: 3
    )
  end

  def print_letter(title)
    print "################################################################################\n"
    print "# #{title}\n"
    print "################################################################################\n"
    print "\n"
  end

  def prepare(client)
    ATTEMPTS.times do |i|
      client.call('set', "key#{i}", "val#{i}")
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
    Async do
      Benchmark.ips do |x|
        x.time = 5
        x.warmup = 1

        x.report('single: async') do
          ATTEMPTS.times do |i|
            key = "key#{i}"
            slot = cluster.slot_for(key)
            client = cluster.client_for(slot)
            client.get(key)
          end
        end

        x.compare!
      end
    end
  end

  def valkey_bench(client)
    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      x.report('single: valkey') do
        ATTEMPTS.times do |i|
          client.get("key#{i}")
        end
      end

      x.compare!
    end
  end
end

IpsSingle.run
