# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      class OnDemand
        def initialize
          size = ::RedisClient::Cluster::ConcurrentWorker::MAX_WORKERS
          size = size.positive? ? size : 5
          @q = SizedQueue.new(size)
        end

        def new_group(size:)
          raise ArgumentError, "size must be positive: #{size} given" unless size.positive?

          ::RedisClient::Cluster::ConcurrentWorker::Group.new(worker: self, size: size)
        end

        def push(task)
          @q << spawn_worker(task, @q)
        end

        def close
          @q.clear
          @q.close
          nil
        end

        private

        def spawn_worker(task, queue)
          Thread.new(task, queue) do |t, q|
            t.exec
            q.pop
          end
        end
      end
    end
  end
end
