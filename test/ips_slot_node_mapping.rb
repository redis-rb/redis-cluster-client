# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'

module IpsSlotNodeMapping
  ELEMENTS = %w[foo bar baz].freeze
  SIZE = 16_384

  module_function

  def run
    print "################################################################################\n"
    print "# Mappings between slots and nodes\n"
    print "################################################################################\n"
    print "\n"

    ca = ::RedisClient::Cluster::Node::CharArray.new(SIZE, ELEMENTS)
    arr = Array.new(SIZE)
    hs = {}

    fullfill(ca)
    fullfill(arr)
    fullfill(hs)

    subjects = {
      ca.class.name.split('::').last => ca,
      arr.class.name => arr,
      hs.class.name => hs
    }.freeze

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      subjects.each do |subject, arr|
        x.report(subject) { arr[0] }
      end

      x.compare!
    end
  end

  def fullfill(arr)
    SIZE.times { |i| arr[i] = ELEMENTS[i % ELEMENTS.size] }
  end
end

IpsSlotNodeMapping.run
