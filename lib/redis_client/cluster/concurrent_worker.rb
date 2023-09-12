# frozen_string_literal: true

require 'redis_client/cluster/concurrent_worker/on_demand'
require 'redis_client/cluster/concurrent_worker/pooled'
require 'redis_client/cluster/concurrent_worker/none'

class RedisClient
  class Cluster
    module ConcurrentWorker
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

        def initialize(worker:, queue:, size:)
          @worker = worker
          @queue = queue
          @size = size
          @count = 0
        end

        def push(id, *args, **kwargs, &block)
          raise InvalidNumberOfTasks, "max size reached: #{@count}" if @count == @size

          task = Task.new(id: id, queue: @queue, args: args, kwargs: kwargs, block: block)
          @worker.push(task)
          @count += 1
          nil
        end

        def each
          raise InvalidNumberOfTasks, "expected: #{@size}, actual: #{@count}" if @count != @size

          @size.times do
            task = @queue.pop
            yield(task.id, task.result)
          end

          nil
        end

        def close
          @queue.clear
          @queue.close if @queue.respond_to?(:close)
          @count = 0
          nil
        end
      end

      module_function

      def create(model: :on_demand, size: 5)
        size = size.positive? ? size : 5

        case model
        when :on_demand, nil then ::RedisClient::Cluster::ConcurrentWorker::OnDemand.new(size: size)
        when :pooled then ::RedisClient::Cluster::ConcurrentWorker::Pooled.new(size: size)
        when :none then ::RedisClient::Cluster::ConcurrentWorker::None.new
        else raise ArgumentError, "Unknown model: #{model}"
        end
      end
    end
  end
end
