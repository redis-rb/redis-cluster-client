# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'

module IpsConcurrentWorker
  TASK_SIZE = 40
  WORKER_SIZE = 5

  module_function

  def run
    on_demand = make_worker(:on_demand)
    pooled = make_worker(:pooled)
    none = make_worker(:none)

    [0.0, 0.001, 0.003].each do |duration|
      print_letter('concurrent worker', "sleep: #{duration}")
      bench(duration, ondemand: on_demand, pooled: pooled, none: none)
    end
  end

  def make_worker(model)
    ::RedisClient::Cluster::ConcurrentWorker.create(model: model, size: WORKER_SIZE)
  end

  def print_letter(title, subtitle)
    print "################################################################################\n"
    print "# #{title}: #{subtitle}\n"
    print "################################################################################\n"
    print "\n"
  end

  def bench(duration, **kwargs)
    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      kwargs.each do |key, worker|
        x.report("model: #{key}") do
          group = worker.new_group(size: TASK_SIZE)

          TASK_SIZE.times do |i|
            group.push(i, i) do |n|
              sleep duration
              2**n
            end
          end

          sum = 0
          group.each do |_, n|
            sum += n
          end

          group.close
        end
      end

      x.compare!
    end
  end
end

IpsConcurrentWorker.run
