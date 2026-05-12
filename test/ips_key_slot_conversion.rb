# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'

module IpsKeySlotConversion
  module_function

  def run
    print "################################################################################\n"
    print "# Key Slot Conversion\n"
    print "################################################################################\n"
    print "\n"

    key = 'foo_app:users:6379'

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      x.report('base') do
        ::RedisClient::Cluster::KeySlotConverter.convert(key)
      end

      x.compare!
    end
  end
end

IpsKeySlotConversion.run
