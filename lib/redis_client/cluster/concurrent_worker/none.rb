# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      class None
        def new_group(size:)
          ::RedisClient::Cluster::ConcurrentWorker::Group.new(
            worker: self,
            queue: [],
            size: size
          )
        end

        def push(task)
          task.exec
        end

        def close; end

        def inspect
          "#<#{self.class.name} main thread only>"
        end
      end
    end
  end
end
