# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      class Actor
        class Group
          Message = Struct.new('RactorMessage', :id, :result, keyword_init: true)

          def initialize(size:)
            @ractors = Array.new(size)
            @size = size
            @count = 0
          end

          def push(id, *args, **kwargs, &block)
            raise InvalidNumberOfTasks, "max size reached: #{@count}" if @count == @size

            @ractors[@count] = Ractor.new(id, block, *args, **kwargs) do |r_id, r_block, *r_args, **r_kwargs|
              result = begin
                r_block&.call(*r_args, **r_kwargs)
              rescue StandardError => e
                e
              end

              Ractor.yield(Message.new(id: r_id, result: result))
            end

            @count += 1
            nil
          end

          def each
            raise InvalidNumberOfTasks, "expected: #{@size}, actual: #{@count}" if @count != @size

            @ractors.each do |ractor|
              message = ractor.take
              yield(message.id, message.result)
            end

            nil
          end

          def close
            @ractors.clear
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
