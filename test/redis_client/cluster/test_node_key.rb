# frozen_string_literal: true

require 'uri'
require 'testing_helper'
require 'redis_client/cluster/node_key'

class RedisClient
  class Cluster
    class TestNodeKey < Minitest::Test
      def test_hashify
        [
          { node_key: '127.0.0.1:6379', want: { host: '127.0.0.1', port: '6379' } },
          { node_key: '::1:6379', want: { host: '::1', port: '6379' } },
          { node_key: 'foobar', want: { host: 'foobar', port: nil } },
          { node_key: '', want: { host: '', port: nil } },
          { node_key: nil, want: { host: nil, port: nil } }
        ].each_with_index do |c, idx|
          got = ::RedisClient::Cluster::NodeKey.hashify(c[:node_key])
          assert_equal(c[:want], got, "Case: #{idx}")
        end
      end

      def test_split
        [
          { node_key: '127.0.0.1:6379', want: ['127.0.0.1', '6379'] },
          { node_key: '::1:6379', want: ['::1', '6379'] },
          { node_key: 'foobar', want: ['foobar', nil] },
          { node_key: '', want: ['', nil] },
          { node_key: nil, want: [nil, nil] }
        ].each_with_index do |c, idx|
          got = ::RedisClient::Cluster::NodeKey.split(c[:node_key])
          assert_equal(c[:want], got, "Case: #{idx}")
        end
      end

      def test_build_from_uri
        [
          { uri: URI('redis://127.0.0.1:6379'), want: '127.0.0.1:6379' },
          { uri: nil, want: '' }
        ].each_with_index do |c, idx|
          got = ::RedisClient::Cluster::NodeKey.build_from_uri(c[:uri])
          assert_equal(c[:want], got, "Case: #{idx}")
        end
      end

      def test_build_from_host_port
        [
          { params: { host: '127.0.0.1', port: 6379 }, want: '127.0.0.1:6379' },
          { params: { host: nil, port: nil }, want: ':' }
        ].each_with_index do |c, idx|
          got = ::RedisClient::Cluster::NodeKey.build_from_host_port(c[:params][:host], c[:params][:port])
          assert_equal(c[:want], got, "Case: #{idx}")
        end
      end
    end
  end
end
