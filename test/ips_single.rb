# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'
require 'hiredis-client'
require 'async/redis/cluster_client'
require 'valkey'

module IpsSingle
  module_function

  ATTEMPTS = 10

  def run
    print "################################################################################\n"
    print "# Single\n"
    print "################################################################################\n"
    print "\n"

    keys = Array.new(ATTEMPTS) { |i| "key#{i}" }
    client = make_client

    keys.each { |key| client.call('set', key, key) }

    clients = {
      client: client,
      envoy: make_client_for_envoy,
      cproxy: make_client_for_cluster_proxy
    }.freeze
    valkey = make_valkey_client

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      clients.each do |sbj, cli|
        x.report(sbj) do
          keys.each { |key| cli.call('get', key) }
        end
      end

      x.report('valkey') do
        keys.each { |key| valkey.get(key) }
      end

      x.compare!
    end

    Sync do |parent_task|
      async = make_async_client

      Benchmark.ips do |x|
        x.time = 5
        x.warmup = 1

        x.report('async') do
          keys.each do |key|
            parent_task.async do |_child_task|
              async.get(key)
            end.wait
          end
        end

        x.compare!
      end
    end
  end

  def make_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
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
end

IpsSingle.run
