# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'

module IpsSlotArray
  ELEMENTS = %w[foo bar baz].freeze
  SIZE = 256

  module_function

  def run
    ca = ::RedisClient::Cluster::Node::CharArray.new(::RedisClient::Cluster::Node::SLOT_SIZE, ELEMENTS)
    arr = Array.new(::RedisClient::Cluster::Node::SLOT_SIZE)

    print_letter('CharArray vs Array')
    fullfill(ca)
    fullfill(arr)
    bench({ ca.class.name.split('::').last => ca, arr.class.name => arr })
  end

  def make_worker(model)
    ::RedisClient::Cluster::ConcurrentWorker.create(model: model, size: WORKER_SIZE)
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
          arr[0] = arr[1]
        end
      end

      x.compare!
    end
  end
end

IpsSlotArray.run
