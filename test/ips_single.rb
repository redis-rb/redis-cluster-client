# frozen_string_literal: true

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

  def print_letter(title)
    print "################################################################################\n"
    print "# #{title}\n"
    print "################################################################################\n"
    print "\n"
  end

  def prepare(*clients)
    clients.each do |client|
      ATTEMPTS.times do |i|
        client.call('SET', "key#{i}", "val#{i}")
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
            client.call('GET', "key#{i}")
          end
        end
      end

      x.compare!
    end
  end
end

IpsSingle.run
