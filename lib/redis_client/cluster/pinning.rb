# frozen_string_literal: true

require 'redis_client'
require 'delegate'

class RedisClient
  class Cluster
    module Pinning
      EMPTY_ARRAY = [].freeze

      class BaseDelegator < SimpleDelegator
        def initialize(conn, slot, router, command_builder)
          @conn = conn
          @slot = slot
          @router = router
          @command_builder = command_builder
          super(@conn)
        end

        private

        def ensure_key_not_cross_slot!(key, method_name)
          slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
          return unless slot != @slot

          raise ::RedisClient::Cluster::Transaction::ConsistencyError.new,
                "Connection pinned to slot #{@slot} but method #{method_name} called with key #{key} in slot #{slot}"
        end

        def ensure_command_not_cross_slot!(command)
          command_keys = @router.command_extract_all_keys(command)
          command_keys.each do |key|
            slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
            if slot != @slot
              raise ::RedisClient::Cluster::Transaction::ConsistencyError.new,
                    "Connection pinned to slot #{@slot} but command #{command.inspect} includes key #{key} in slot #{slot}"
            end
          end
        end
      end

      module CallDelegator
        def call(*command, **kwargs)
          command = @command_builder.generate(command, kwargs)
          ensure_command_not_cross_slot! command
          super
        end
        alias call_once call

        def call_v(command)
          command = @command_builder.generate(command)
          ensure_command_not_cross_slot! command
          super
        end
        alias call_once_v call_v
      end

      module BlockingCallDelegator
        def blocking_call(timeout, *command, **kwargs)
          command = @command_builder.generate(command, kwargs)
          ensure_command_not_cross_slot! command
          super
        end

        def blocking_call_v(timeout, command)
          command = @command_builder.generate(command)
          ensure_command_not_cross_slot! command
          super
        end
      end

      class PipelineProxy < BaseDelegator
        include CallDelegator
        include BlockingCallDelegator
      end

      class MultiProxy < BaseDelegator
        include CallDelegator
      end

      class ConnectionProxy < BaseDelegator
        include CallDelegator
        include BlockingCallDelegator

        def sscan(key, *args, **kwargs, &block)
          ensure_key_not_cross_slot! key, :sscan
          super
        end

        def hscan(key, *args, **kwargs, &block)
          ensure_key_not_cross_slot! key, :hscan
          super
        end

        def zscan(key, *args, **kwargs, &block)
          ensure_key_not_cross_slot! key, :zscan
          super
        end

        def pipelined
          @conn.pipelined do |conn_pipeline|
            yield PipelineProxy.new(conn_pipeline, @slot, @router, @command_builder)
          end
        end

        def multi(watch: nil)
          call('WATCH', *watch) if watch
          begin
            @conn.pipelined do |conn_pipeline|
              txn = MultiProxy.new(conn_pipeline, @slot, @router, @command_builder)
              txn.call('MULTI')
              yield txn
              txn.call('EXEC')
            end.last
          rescue StandardError
            call('UNWATCH') if connected? && watch
            raise
          end
        end
      end
    end
  end
end
