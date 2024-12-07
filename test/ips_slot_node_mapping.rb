# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'

module IpsSlotNodeMapping
  ELEMENTS = %w[foo bar baz].freeze
  SIZE = 16_384

  module_function

  def run
    ca = ::RedisClient::Cluster::Node::CharArray.new(SIZE, ELEMENTS)
    arr = Array.new(SIZE)
    hs = {}

    print_letter('Mappings between slots and nodes')
    fullfill(ca)
    fullfill(arr)
    fullfill(hs)
    bench(
      {
        ca.class.name.split('::').last => ca,
        arr.class.name => arr,
        hs.class.name => hs
      }
    )
  end

  def print_letter(title)
    print "################################################################################\n"
    print "# #{title}\n"
    print "################################################################################\n"
    print "\n"
  end

  def fullfill(arr)
    SIZE.times { |i| arr[i] = ELEMENTS[i % ELEMENTS.size] }
  end

  def bench(subjects)
    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      subjects.each do |subtitle, arr|
        x.report(subtitle) do
          arr[0]
        end
      end

      x.compare!
    end
  end
end

IpsSlotNodeMapping.run
