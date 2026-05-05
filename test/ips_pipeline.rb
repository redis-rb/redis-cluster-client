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

    keys = Array.new(ATTEMPTS) { |i| "key#{i}" }
    client = make_client(:none)

    client.pipelined do |pi|
      keys.each { |key| pi.call('set', key, key) }
    end

    clients = {
      none: client,
      pooled: make_client(:pooled),
      ondemand: make_client(:on_demand),
      envoy: make_client_for_envoy,
      cproxy: make_client_for_cluster_proxy
    }
    valkey = make_valkey_client
    async = make_async_client

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      clients.each do |sbj, cli|
        x.report(sbj) do
          cli.pipelined do |pi|
            keys.each { |key| pi.call('get', key) }
          end
        end
      end

      x.report('valkey') do
        valkey.pipelined do |pi|
          keys.each { |key| pi.get(key) }
        end
      end

      x.compare!
    end

    Async do
      Benchmark.ips do |x|
        x.time = 5
        x.warmup = 1

        x.report('async') do
          # TODO: aggregate results along the order
          keys.group_by { |key| async.client_for(async.slot_for(key)) }.each do |client, keys|
            client.pipeline do |context|
              keys.each { |key| context.get(key) }
              context.collect
            end
          end
        end

        x.compare!
      end
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
