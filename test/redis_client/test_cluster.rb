# frozen_string_literal: true

require 'minitest/autorun'
require 'testing_helper'
require 'redis_client/cluster'

class RedisClient
  class TestCluster
    module Mixin
      include TestingHelper

      def setup
        @client = new_test_client
        @client.call('FLUSHDB')
        testing_hook
      end

      def teardown
        @client.call('FLUSHDB')
        testing_hook
        @client&.close
      end

      def testing_hook; end

      def test_inspect
        assert_match(/^#<RedisClient::Cluster [0-9., :]*>$/, @client.inspect)
      end

      def test_call
        (0..9).each do |i|
          assert_equal('OK', @client.call('SET', "key#{i}", i), "Case: SET: key#{i}")
          assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: GET: key#{i}")
        end
      end

      def test_call_once
        (0..9).each do |i|
          assert_equal('OK', @client.call_once('SET', "key#{i}", i), "Case: SET: key#{i}")
          assert_equal(i.to_s, @client.call_once('GET', "key#{i}"), "Case: GET: key#{i}")
        end
      end

      def test_blocking_call
        @client.call(*%w[RPUSH foo hello])
        @client.call(*%w[RPUSH foo world])
        client_side_timeout = 0.2
        server_side_timeout = 0.1
        assert_equal(%w[foo world], @client.blocking_call(client_side_timeout, 'BRPOP', 'foo', server_side_timeout), 'Case: 1st')
        assert_equal(%w[foo hello], @client.blocking_call(client_side_timeout, 'BRPOP', 'foo', server_side_timeout), 'Case: 2nd')
        assert_nil(@client.blocking_call(client_side_timeout, 'BRPOP', 'foo', server_side_timeout), 'Case: 3rd')
        assert_raises(::RedisClient::ReadTimeoutError, 'Case: 4th') { @client.blocking_call(0.1, 'BRPOP', 'foo', 0) }
      end

      def test_scan
        assert_raises(ArgumentError) { @client.scan }

        (0..9).each { |i| @client.call('SET', "key#{i}", i) }
        want = (0..9).map { |i| "key#{i}" }
        got = []
        @client.scan('COUNT', '5') { |key| got << key }
        assert_equal(want, got.sort)
      end

      def test_sscan
        (0..9).each do |i|
          (0..9).each { |j| @client.call('SADD', "key#{i}", "member#{j}") }
          want = (0..9).map { |j| "member#{j}" }
          got = []
          @client.sscan("key#{i}", 'COUNT', '5') { |member| got << member }
          assert_equal(want, got.sort)
        end
      end

      def test_hscan
        (0..9).each do |i|
          (0..9).each { |j| @client.call('HSET', "key#{i}", "field#{j}", j) }
          want = (0..9).map { |j| "field#{j}" }
          got = []
          @client.hscan("key#{i}", 'COUNT', '5') { |field| got << field }
          assert_equal(want, got.sort)
        end
      end

      def test_zscan
        (0..9).each do |i|
          (0..9).each { |j| @client.call('ZADD', "key#{i}", j, "member#{j}") }
          want = (0..9).map { |j| "member#{j}" }
          got = []
          @client.zscan("key#{i}", 'COUNT', '5') { |member| got << member }
          assert_equal(want, got.sort)
        end
      end
    end

    class PrimaryOnly < Minitest::Test
      include Mixin

      def new_test_client
        config = ::RedisClient::ClusterConfig.new(nodes: TEST_NODE_URIS, **TEST_GENERIC_OPTIONS)
        ::RedisClient::Cluster.new(config)
      end
    end

    class ScaleRead < Minitest::Test
      include Mixin

      def new_test_client
        config = ::RedisClient::ClusterConfig.new(nodes: TEST_NODE_URIS, replica: true, **TEST_GENERIC_OPTIONS)
        ::RedisClient::Cluster.new(config)
      end

      def testing_hook
        @client.call('WAIT', '1', '300')
      end
    end

    class Pooled < Minitest::Test
      include Mixin

      def new_test_client
        config = ::RedisClient::ClusterConfig.new(nodes: TEST_NODE_URIS, **TEST_GENERIC_OPTIONS)
        ::RedisClient::Cluster.new(config, pool: { timeout: TEST_TIMEOUT_SEC, size: 2 })
      end
    end
  end
end
