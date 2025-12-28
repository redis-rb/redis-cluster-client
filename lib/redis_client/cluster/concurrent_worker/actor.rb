# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      class Actor
        class Group
          Task = Struct.new('RedisClusterClientRactorTask', :id, :ractor, keyword_init: true)

          def initialize(size:)
            @tasks = Array.new(size)
            @size = size
            @count = 0
          end

          def push(id, *args, **kwargs, &block)
            raise InvalidNumberOfTasks, "max size reached: #{@count}" if @count == @size

            @tasks[@count] = ::RedisClient::Cluster::ConcurrentWorker::Actor::Group::Task.new(
              id: id,
              ractor: Ractor.new(*args, **kwargs, &block) # FIXME: How to disable report_on_exception?
            )

            @count += 1
            nil
          end

          def each
            raise InvalidNumberOfTasks, "expected: #{@size}, actual: #{@count}" if @count != @size

            @tasks.each do |task|
              result = begin
                task.ractor.value
              rescue Ractor::Error => e
                e.cause
              end

              yield(task.id, result)
            end

            nil
          end

          def close
            @tasks.clear
            @count = 0
            nil
          end

          def inspect
            "#<#{self.class.name} size: #{@size}>"
          end
        end

        def new_group(size:)
          ::RedisClient::Cluster::ConcurrentWorker::Actor::Group.new(size: size)
        end

        def close; end

        def inspect
          "#<#{self.class.name} size: #{Ractor.count - 1}>"
        end
      end
    end
  end
end
