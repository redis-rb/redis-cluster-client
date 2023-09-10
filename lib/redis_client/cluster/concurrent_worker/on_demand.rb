# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      class OnDemand
        def initialize
          @q = Queue.new
          size = ::RedisClient::Cluster::ConcurrentWorker::MAX_WORKERS
          size = size.positive? ? size : 5
          @buffer = SizedQueue.new(size)
          @manager = nil
        end

        def new_group(size:)
          raise ArgumentError, "size must be positive: #{size} given" unless size.positive?

          @manager = spawn_manager unless @manager&.alive?
          ::RedisClient::Cluster::ConcurrentWorker::Group.new(queue: @q, size: size)
        end

        def close
          @q.clear
          @buffer.clear
          @manager&.exit
          @buffer.close
          @q.close
          nil
        end

        private

        def spawn_manager
          Thread.new(@q, @buffer) do |q, b|
            loop do
              b << spawn_worker(q.pop, b)
            end
          end
        end

        def spawn_worker(task, buffer)
          Thread.new(task, buffer) do |t, b|
            t.exec
            b.pop
          end
        end
      end
    end
  end
end
