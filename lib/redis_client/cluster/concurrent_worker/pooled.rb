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
        IO_ERROR_NEVER = { IOError => :never }.freeze
        IO_ERROR_ON_BLOCKING = { IOError => :on_blocking }.freeze
        private_constant :IO_ERROR_NEVER, :IO_ERROR_ON_BLOCKING

        def initialize(size:)
          raise ArgumentError, "size must be positive: #{size}" unless size.positive?

          @size = size
          setup
        end

        def new_group(size:)
          reset if @pid != ::RedisClient::PIDCache.pid
          ensure_workers if @workers.first.nil?
          ::RedisClient::Cluster::ConcurrentWorker::Group.new(
            worker: self,
            queue: SizedQueue.new(size),
            size: size
          )
        end

        def push(task)
          @q << task
        end

        def close
          @q.clear
          workers = @workers.compact
          workers.each(&:exit)
          workers.each(&:join)
          @workers.clear
          @q.close
          @pid = nil
          nil
        end

        def inspect
          "#<#{self.class.name} tasks: #{@q.size}, workers: #{@size}>"
        end

        private

        def setup
          @q = Queue.new
          @workers = Array.new(@size)
          @pid = ::RedisClient::PIDCache.pid
        end

        def reset
          close
          setup
        end

        def ensure_workers
          @size.times do |i|
            @workers[i] = spawn_worker unless @workers[i]&.alive?
          end
        end

        def spawn_worker
          Thread.new(@q) do |q|
            Thread.handle_interrupt(IO_ERROR_NEVER) do
              loop do
                Thread.handle_interrupt(IO_ERROR_ON_BLOCKING) do
                  q.pop.exec
                end
              end
            end
          rescue IOError
            # stream closed in another thread
          end
        end
      end
    end
  end
end
