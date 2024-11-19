# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      class OnDemand
        def initialize(size:)
          raise ArgumentError, "size must be positive: #{size}" unless size.positive?

          @q = SizedQueue.new(size)
        end

        def new_group(size:)
          ::RedisClient::Cluster::ConcurrentWorker::Group.new(
            worker: self,
            queue: SizedQueue.new(size),
            size: size
          )
        end

        def push(task)
          @q << spawn_worker(task, @q)
        end

        def close
          @q.clear
          @q.close
          nil
        end

        def inspect
          "#<#{self.class.name} active: #{@q.size}, max: #{@q.max}>"
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
