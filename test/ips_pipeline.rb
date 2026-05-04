# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'
require 'async/redis/cluster_client'
require 'valkey'

module IpsPipeline
  module_function

  ATTEMPTS = 100

  def run
    print "################################################################################\n"
    print "# Pipeline\n"
    print "################################################################################\n"
    print "\n"

    client = make_client(:none)

    client.pipelined do |pi|
      ATTEMPTS.times { |i| pi.call('set', "key#{i}", "val#{i}") }
    end

    clients = {
      none: client,
      pooled: make_client(:pooled),
      ondemand: make_client(:on_demand),
      envoy: make_client_for_envoy,
      cproxy: make_client_for_cluster_proxy
    }
    valkey = make_valkey_client

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      clients.each do |key, cli|
        x.report(key) do
          cli.pipelined do |pi|
            ATTEMPTS.times { |i| pi.call('get', "key#{i}") }
          end
        end
      end

      x.report('valkey') do
        valkey.pipelined do |pi|
          ATTEMPTS.times { |i| pi.get("key#{i}") }
        end
      end

      x.compare!
    end
  end

  def make_client(model)
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
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
end

IpsPipeline.run
