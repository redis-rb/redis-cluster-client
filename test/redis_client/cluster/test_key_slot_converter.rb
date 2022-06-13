# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/key_slot_converter'

class RedisClient
  class Cluster
    class TestKeySlotConverter < Minitest::Test
      include TestingHelper

      def test_convert
        (1..255).map { |i| "key#{i}" }.each_with_index do |key, idx|
          want = @raw_clients.first.call('CLUSTER', 'KEYSLOT', key)
          got = ::RedisClient::Cluster::KeySlotConverter.convert(key)
          assert_equal(want, got, "Case: #{idx}")
        end
      end
    end
  end
end
