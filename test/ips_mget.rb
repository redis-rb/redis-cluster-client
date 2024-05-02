# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'

module IpsMget
  module_function

  ATTEMPTS = 40

  def run
    cli = make_client
    prepare(cli)
    print_letter('MGET')
    bench('MGET', cli)
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

  def print_letter(title)
    print "################################################################################\n"
    print "# #{title}\n"
    print "################################################################################\n"
    print "\n"
  end

  def prepare(client)
    ATTEMPTS.times do |i|
      client.call('SET', "{key}#{i}", "val#{i}")
      client.call('SET', "key#{i}", "val#{i}")
    end
  end

  def bench(cmd, client)
    original = [cmd] + Array.new(ATTEMPTS) { |i| "{key}#{i}" }
    emulated = [cmd] + Array.new(ATTEMPTS) { |i| "key#{i}" }

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1
      x.report("#{cmd}: original") { client.call_v(original) }
      x.report("#{cmd}: emulated") { client.call_v(emulated) }
      x.compare!
    end
  end
end

IpsMget.run
