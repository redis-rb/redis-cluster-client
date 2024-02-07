# frozen_string_literal: true

require 'delegate'
class RedisClient
  class Cluster
    class PinnedConnectionDelegator < SimpleDelegator
      METHOD_WRAPPERS = {
        # These methods return a Pipeline or Multi to the yielded block which itself
        # requires wrapping.
        %i[pipelined multi] => lambda do |*args, **kwargs, &block|
          super(*args, **kwargs, &wrap_yielded_object(block))
        end,
        # These are various commanding methods, each of which has a different signature
        # which we need to understand to extract the commands properly
        %i[call call_once] => lambda do |*command, **kwargs, &block|
          validate_slot! command
          super(*command, **kwargs, &block)
        end,
        %i[call_v call_once_v] => lambda do |command, &block|
          validate_slot! command
          super command, &block
        end,
        %i[blocking_call] => lambda do |timeout, *command, **kwargs, &block|
          validate_slot! command
          super timeout, *command, **kwargs, &block
        end,
        %i[blocking_call_v] => lambda do |timeout, command, &block|
          validate_slot! command
          super timeout, command, &block
        end
      }.freeze

      def initialize(connection, locked_key_slot:, cluster_commands:)
        @locked_key_slot = locked_key_slot
        @cluster_commands = cluster_commands
        super(connection)

        # Set up the slot validation for the set of methods which this object
        # implements. Note that we might be wrapping a RedisClient, RedisClient::Multi,
        # or RedisClient::Pipeline object, and they do not all respond to the same set of
        # methods. Hence, we need to dynamically detect what we're wrapping and define
        # the correct methods.
        METHOD_WRAPPERS.each do |methods, impl|
          methods.each do |method|
            define_singleton_method(method, &impl) if respond_to?(method)
          end
        end
      end

      private

      def validate_slot!(command)
        keys = @cluster_commands.extract_all_keys(command)
        key_slots = keys.map { |k| ::RedisClient::Cluster::KeySlotConverter.convert(k) }
        locked_slot = ::RedisClient::Cluster::KeySlotConverter.convert(@locked_key_slot)

        return if key_slots.all? { |slot| slot ==  locked_slot }

        key_slot_pairs = keys.zip(key_slots).map { |key, slot| "#{key} => #{slot}" }.join(', ')
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, <<~MESSAGE
          Connection is pinned to slot #{locked_slot} (via key #{@locked_key_slot}). \
          However, command #{command.inspect} has keys hashing to slots #{key_slot_pairs}. \
          Transactions in redis cluster must only refer to keys hashing to the same slot.
        MESSAGE
      end

      # This method ensures that calls to #pipelined, #multi, etc wrap yielded redis-callable objects
      # back in the pinning delegator
      def wrap_yielded_object(orig_block)
        return nil if orig_block.nil?

        proc do |clientish|
          orig_block.call(
            self.class.new(clientish, locked_key_slot: @locked_key_slot, cluster_commands: @cluster_commands)
          )
        end
      end
    end
  end
end
