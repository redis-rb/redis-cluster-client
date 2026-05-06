# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'
require 'hiredis-client'

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
    keys = Array.new(ATTEMPTS) { |i| "key#{i}" }
    emulated = ['mget'] + keys
    single = ['mget', '']

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      x.report('original') { client.call_v(original) }
      x.report('emulated') { client.call_v(emulated) }
      x.report('single') do
        keys.each do |key|
          single[1] = key
          client.call_v(single)
        end
      end

      x.compare!
    end
  end

  def make_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
  end
end

IpsMget.run
