# frozen_string_literal: true

require 'redis_client/cluster/concurrent_worker/on_demand'
require 'redis_client/cluster/concurrent_worker/pooled'

class RedisClient
  class Cluster
    module ConcurrentWorker
      MAX_WORKERS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))
      InvalidNumberOfTasks = Class.new(::RedisClient::Error)

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
            done
          end

          def done
            queue&.push(self)
          rescue ClosedQueueError
            # something was wrong
          end
        end

        def initialize(worker:, size:)
          raise ArgumentError, "the size must be positive: #{size} given" unless size.positive?

          @worker = worker
          @result_queue = SizedQueue.new(size)
          @count = 0
        end

        def push(id, *args, **kwargs, &block)
          raise InvalidNumberOfTasks, "expected: #{@result_queue.max}, actual: #{@count + 1}" if @count + 1 > @result_queue.max

          task = Task.new(id: id, queue: @result_queue, args: args, kwargs: kwargs, block: block)
          @worker.push(task)
          @count += 1
          nil
        end

        def each
          raise InvalidNumberOfTasks, "expected: #{@result_queue.max}, actual: #{@count}" if @count != @result_queue.max

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
        when :on_demand, nil then ::RedisClient::Cluster::ConcurrentWorker::OnDemand.new
        when :pooled then ::RedisClient::Cluster::ConcurrentWorker::Pooled.new
        else raise ArgumentError, "Unknown model: #{model}"
        end
      end

      def size
        MAX_WORKERS.positive? ? MAX_WORKERS : 5
      end
    end
  end
end
