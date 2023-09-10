# frozen_string_literal: true

require 'redis_client/cluster/concurrent_worker/on_demand'
require 'redis_client/cluster/concurrent_worker/pooled'

class RedisClient
  class Cluster
    module ConcurrentWorker
      MAX_WORKERS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))
      NotEnoughTasks = Class.new(::RedisClient::Error)

      class Group
        Task = Struct.new(
          'RedisClusterClientConcurrentWorkerTask',
          :id, :queue, :args, :kwargs, :block, :result,
          keyword_init: true
        ) do
          def exec
            self[:result] = block&.call(*args, **kwargs)
          rescue StandardError => e
            self[:result] = e
          ensure
            queue&.push(self)
          end
        end

        def initialize(queue:, size:)
          raise ArgumentError, "the size must be positive: #{size} given" unless size.positive?

          @task_queue = queue
          @result_queue = SizedQueue.new(size)
          @count = 0
        end

        def push(id, *args, **kwargs, &block)
          @task_queue << Task.new(id: id, queue: @result_queue, args: args, kwargs: kwargs, block: block)
          @count += 1
          nil
        end

        def each
          raise NotEnoughTasks, "expected: #{@result_queue.max}, actual: #{@count}" if @count != @result_queue.max

          @result_queue.max.times do
            task = @result_queue.pop
            yield(task.id, task.result)
          end

          nil
        end

        def close
          @result_queue.clear
          @result_queue.close
          @count = 0
          nil
        end
      end

      module_function

      def create(model: :on_demand)
        case model
        when :on_demand then ::RedisClient::Cluster::ConcurrentWorker::OnDemand.new
        when :pooled then ::RedisClient::Cluster::ConcurrentWorker::Pooled.new
        else raise ArgumentError, "Unknown model: #{model}"
        end
      end
    end
  end
end
