# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      module None
        class Group
          Task = Struct.new(
            'RedisClusterClientSingleThreadTask',
            :id, :result, keyword_init: true
          )

          def initialize(size:)
            @tasks = Array.new(size)
            @idx = 0
          end

          def push(id, *args, **kwargs, &block)
            raise InvalidNumberOfTasks, "max size reached: #{@idx}" if @idx == @tasks.size

            result = exec(*args, **kwargs, &block)
            @tasks[@idx] = Task.new(id: id, result: result)
            @idx += 1
            nil
          end

          def each
            raise InvalidNumberOfTasks, "expected: #{@tasks.size}, actual: #{@idx}" if @idx != @tasks.size

            @tasks.each { |task| yield(task.id, task.result) }
            nil
          end

          def close
            @idx = 0
            @tasks.clear
            nil
          end

          def inspect
            "#<#{self.class.name} size: #{@idx}, max: #{@tasks.size}>"
          end

          private

          def exec(*args, **kwargs)
            yield(*args, **kwargs) if block_given?
          rescue StandardError => e
            e
          end
        end

        module_function

        def new_group(size:)
          Group.new(size: size)
        end

        def close; end

        def inspect
          "#<#{self.class.name} main thread only>"
        end
      end
    end
  end
end
