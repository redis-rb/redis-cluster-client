# frozen_string_literal: true

require 'redis_client/cluster/concurrent_worker'

class RedisClient
  class Cluster
    module ConcurrentWorker
      class OnDemand
        def initialize
          @q = Queue.new
          @buffer = SizedQueue.new(::RedisClient::Cluster::ConcurrentWorker::MAX_WORKERS - 1)
          @manager = nil
        end

        def new_group(size:)
          @manager = spawn_manager if @manager.nil?
          ::RedisClient::Cluster::ConcurrentWorker::Group.new(queue: @q, size: size)
        end

        def close
          @q.clear
          @buffer.clear
          @manager&.exit
          @buffer.close
          @q.close
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
