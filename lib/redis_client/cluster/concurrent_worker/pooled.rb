# frozen_string_literal: true

require 'redis_client/pid_cache'

class RedisClient
  class Cluster
    module ConcurrentWorker
      # This class is just an experimental implementation.
      # Ruby VM allocates 1 MB memory as a stack for a thread.
      # It is a fixed size but we can modify the size with some environment variables.
      # So it consumes memory 1 MB multiplied a number of workers.
      class Pooled
        def initialize
          setup
        end

        def new_group(size:)
          reset if @pid != ::RedisClient::PIDCache.pid
          ensure_workers if @workers.first.nil?
          ::RedisClient::Cluster::ConcurrentWorker::Group.new(worker: self, size: size)
        end

        def push(task)
          @q << task
        end

        def close
          @q.clear
          @workers.each { |t| t&.exit }
          @workers.clear
          @q.close
          @pid = nil
          nil
        end

        private

        def setup
          @q = Queue.new
          @workers = Array.new(::RedisClient::Cluster::ConcurrentWorker.size)
          @pid = ::RedisClient::PIDCache.pid
        end

        def reset
          close
          setup
        end

        def ensure_workers
          @workers.size.times do |i|
            @workers[i] = spawn_worker unless @workers[i]&.alive?
          end
        end

        def spawn_worker
          Thread.new(@q) { |q| loop { q.pop.exec } }
        end
      end
    end
  end
end
