# frozen_string_literal: true

require 'redis_client/cluster/concurrent_worker'

class RedisClient
  class Cluster
    module ConcurrentWorker
      class Pooled
        def initialize
          @q = Queue.new
          size = ::RedisClient::Cluster::ConcurrentWorker::MAX_WORKERS
          size = size.positive? ? size : 5
          @workers = Array.new(size)
        end

        def new_group(size:)
          raise ArgumentError, "size must be positive: #{size} given" unless size.positive?

          ensure_workers if @workers.first.nil?
          ::RedisClient::Cluster::ConcurrentWorker::Group.new(queue: @q, size: size)
        end

        def close
          @q.clear
          @workers.each { |t| t&.exit }
          @workers.clear
          @q.close
        end

        private

        def ensure_workers
          @workers.size.times do |i|
            @workers[i] = spawn_worker unless @workers[i]&.alive?
          end
        end

        def spawn_worker
          Thread.new(@q) do |q|
            loop { q.pop.exec }
          end
        end
      end
    end
  end
end
