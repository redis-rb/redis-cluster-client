# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'

module IpsMget
  module_function

  ATTEMPTS = 40

  def run
    print "################################################################################\n"
    print "# MGET\n"
    print "################################################################################\n"
    print "\n"

    client = make_client

    ATTEMPTS.times do |i|
      client.call('set', "{key}#{i}", "val#{i}")
      client.call('set', "key#{i}", "val#{i}")
    end

    original = ['mget'] + Array.new(ATTEMPTS) { |i| "{key}#{i}" }
    emulated = ['mget'] + Array.new(ATTEMPTS) { |i| "key#{i}" }
    single_get = ['mget', '']

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      x.report('MGET: original') { client.call_v(original) }
      x.report('MGET: emulated') { client.call_v(emulated) }
      x.report('MGET: single') do
        ATTEMPTS.times do |i|
          single_get[1] = "key#{i}"
          client.call_v(single_get)
        end
      end

      x.compare!
    end
  end

  def make_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      replica_affinity: :random,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      concurrency: { model: :on_demand },
      **TEST_GENERIC_OPTIONS
    ).new_client
  end
end

IpsMget.run
