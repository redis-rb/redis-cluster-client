# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class Cluster
    class TestKeySlotConverter < TestingWrapper
      def setup
        @raw_clients = TEST_NODE_URIS.map { |addr| ::RedisClient.config(url: addr, **TEST_GENERIC_OPTIONS).new_client }
      end

      def teardown
        @raw_clients&.each(&:close)
      end

      def test_convert
        (1..255).map { |i| "key#{i}" }.each_with_index do |key, idx|
          want = @raw_clients.first.call('CLUSTER', 'KEYSLOT', key)
          got = ::RedisClient::Cluster::KeySlotConverter.convert(key)
          assert_equal(want, got, "Case: #{idx}")
        end

        assert_nil(::RedisClient::Cluster::KeySlotConverter.convert(nil), 'Case: nil')

        multi_byte_key = 'あいうえお'
        want = @raw_clients.first.call('CLUSTER', 'KEYSLOT', multi_byte_key)
        got = ::RedisClient::Cluster::KeySlotConverter.convert(multi_byte_key)
        assert_equal(want, got, "Case: #{multi_byte_key}")
      end
    end
  end
end
