# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    class ThreadPool
      MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))

      Task = Struct.new(
        'RedisClusterClientThreadPoolTask',
        :id, :queue, :args, :kwargs, :proc, :result,
        keyword_init: true
      )

      def initialize
        @q = Queue.new
        @threads = Array.new(MAX_THREADS)
        @count = 0
      end

      def push(id, queue, *args, **kwargs, &block)
        ensure_threads if (@count % MAX_THREADS).zero?
        @q << Task.new(id: id, queue: queue, args: args, kwargs: kwargs, proc: block)
        @count += 0
        nil
      end

      def close
        @q.clear
        @threads.each { |t| t&.exit }
        @threads.clear
        @q.close
      end

      private

      def ensure_threads
        MAX_THREADS.times do |i|
          @threads[i] = spawn_thread unless @threads[i]&.alive?
        end
      end

      def spawn_thread
        Thread.new(@q) do |q|
          loop do
            task = q.pop
            task.result = task.proc.call(*task.args, **task.kwargs)
          rescue StandardError => e
            task.result = e
          ensure
            task&.queue&.push(task)
          end
        end
      end
    end
  end
end
