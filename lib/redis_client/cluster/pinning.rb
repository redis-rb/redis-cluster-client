# frozen_string_literal: true

class RedisClient
  class Cluster
    module Pinning
      module ClientMixin
        attr_reader :locked_key_slot

        # This gets called when handing out connections in Cluster#with to lock the returned
        # connections to a given slot.
        def locked_to_key_slot(key_slot)
          raise ArgumentError, 'recursive slot locking is not allowed' if @locked_key_slot

          begin
            @locked_key_slot = key_slot
            yield
          ensure
            @locked_key_slot = nil
          end
        end
      end

      # This middleware is what actually enforces the slot locking above.
      module ClientMiddleware
        def initialize(client)
          @client = client
          super
        end

        def assert_slot_valid!(command, config) # rubocop:disable Metrics/AbcSize
          return unless @client.locked_key_slot
          return unless config.cluster_commands.loaded?

          keys = config.cluster_commands.extract_all_keys(command)
          key_slots = keys.map { |k| ::RedisClient::Cluster::KeySlotConverter.convert(k) }
          locked_slot = ::RedisClient::Cluster::KeySlotConverter.convert(@client.locked_key_slot)
          return if key_slots.all? { |slot| slot ==  locked_slot }

          key_slot_pairs = keys.zip(key_slots).map { |key, slot| "#{key} => #{slot}" }.join(', ')
          raise ::RedisClient::Cluster::Transaction::ConsistencyError, <<~MESSAGE
            Connection is pinned to slot #{locked_slot} (via key #{@client.locked_key_slot}). \
            However, command #{command.inspect} has keys hashing to slots #{key_slot_pairs}. \
            Transactions in redis cluster must only refer to keys hashing to the same slot.
          MESSAGE
        end

        def call(command, config)
          assert_slot_valid!(command, config)
          super
        end

        def call_pipelined(command, config)
          assert_slot_valid!(command, config)
          super
        end
      end
    end
  end
end
