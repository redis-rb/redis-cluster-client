# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    class ThreadPool
      MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))

      Task = Struct.new(
        'RedisClusterClientThreadPoolTask',
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

      def initialize
        @q = Queue.new
        @threads = Array.new(MAX_THREADS)
      end

      def push(id, queue, *args, **kwargs, &block)
        ensure_threads if @threads.first.nil?
        @q << Task.new(id: id, queue: queue, args: args, kwargs: kwargs, block: block)
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
          loop { q.pop.exec }
        end
      end
    end
  end
end
