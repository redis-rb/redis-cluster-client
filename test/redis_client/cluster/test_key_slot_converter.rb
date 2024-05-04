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

      def test_extract_hash_tag
        [
          { key: 'foo', want: '' },
          { key: 'foo{bar}baz', want: 'bar' },
          { key: 'foo{bar}baz{qux}quuc', want: 'bar' },
          { key: 'foo}bar{baz', want: '' },
          { key: 'foo{bar', want: '' },
          { key: 'foo}bar', want: '' },
          { key: 'foo{}bar', want: '' },
          { key: '{}foo', want: '' },
          { key: 'foo{}', want: '' },
          { key: '{}', want: '' },
          { key: '', want: '' },
          { key: nil, want: '' }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = ::RedisClient::Cluster::KeySlotConverter.extract_hash_tag(c[:key])
          assert_equal(c[:want], got, msg)
        end
      end

      def test_hash_tag_included?
        [
          { key: 'foo', want: false },
          { key: 'foo{bar}baz', want: true },
          { key: 'foo{bar}baz{qux}quuc', want: true },
          { key: 'foo}bar{baz', want: false },
          { key: 'foo{bar', want: false },
          { key: 'foo}bar', want: false },
          { key: 'foo{}bar', want: false },
          { key: '{}foo', want: false },
          { key: 'foo{}', want: false },
          { key: '{}', want: false },
          { key: '', want: false },
          { key: nil, want: false }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = ::RedisClient::Cluster::KeySlotConverter.hash_tag_included?(c[:key])
          assert_equal(c[:want], got, msg)
        end
      end
    end
  end
end
