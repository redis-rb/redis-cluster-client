# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'

module HashtagExtraction
  module_function

  def run
    print "################################################################################\n"
    print "# Hashtag Extraction\n"
    print "################################################################################\n"
    print "\n"

    key = 'aaaa{aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}aaaaaaaa'

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      x.report('base') do
        ::RedisClient::Cluster::KeySlotConverter.extract_hash_tag(key)
      end

      x.compare!
    end
  end
end

HashtagExtraction.run
