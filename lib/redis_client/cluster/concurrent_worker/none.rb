# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      class None
        def new_group(size:)
          ::RedisClient::Cluster::ConcurrentWorker::Group.new(
            worker: self,
            queue: Array.new(size),
            size: size
          )
        end

        def push(task)
          task.exec
        end

        def close; end
      end
    end
  end
end
