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
    print_letter('pipelined')
    cli = make_client(:none)
    prepare(cli)
    bench(
      none: cli,
      pooled: make_client(:pooled),
      ondemand: make_client(:on_demand),
      envoy: make_client_for_envoy,
      cproxy: make_client_for_cluster_proxy
    )
    # async_bench(make_async_client)
    valkey_bench(make_valkey_client)
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
    client.pipelined do |pi|
      ATTEMPTS.times do |i|
        pi.call('set', "key#{i}", "val#{i}")
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
              pi.call('get', "key#{i}")
            end
          end
        end
      end

      x.compare!
    end
  end

  def async_bench(client)
    Async do
      Benchmark.ips do |x|
        x.time = 5
        x.warmup = 1

        x.report('pipelined: async') do
          # FIXME: supported or not?
          client.pipeline do |pi|
            ATTEMPTS.times do |i|
              pi.call('get', "key#{i}")
            end
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

      x.report('pipelined: valkey') do
        client.pipelined do |pi|
          ATTEMPTS.times do |i|
            pi.get("key#{i}")
          end
        end
      end

      x.compare!
    end
  end
end

IpsPipeline.run
