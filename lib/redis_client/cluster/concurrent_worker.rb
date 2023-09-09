# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      MAX_WORKERS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))

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
          @task_queue = queue
          @result_queue = SizedQueue.new(size)
        end

        def push(id, *args, **kwargs, &block)
          @task_queue << Task.new(id: id, queue: @result_queue, args: args, kwargs: kwargs, block: block)
          nil
        end

        def each
          @result_queue.max.times do
            task = @result_queue.pop
            yield(task.id, task.result)
          end
        end

        def close
          @result_queue.clear
          @result_queue.close
        end
      end
    end
  end
end
