# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class TestCluster
    module Mixin # rubocop:disable Metrics/ModuleLength
      def setup
        @client = new_test_client
        @client.call('FLUSHDB')
        wait_for_replication
      end

      def teardown
        @client&.call('FLUSHDB')
        wait_for_replication
        @client&.close
      end

      def test_config
        refute_nil @client.config
        refute_nil @client.config.read_timeout
      end

      def test_inspect
        assert_match(/^#<RedisClient::Cluster [0-9., :]*>$/, @client.inspect)
      end

      def test_call
        assert_raises(ArgumentError) { @client.call }

        10.times do |i|
          assert_equal('OK', @client.call('SET', "key#{i}", i), "Case: SET: key#{i}")
          wait_for_replication
          assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: GET: key#{i}")
        end

        assert(@client.call('PING') { |r| r == 'PONG' })

        assert_equal(2, @client.call('HSET', 'hash', { foo: 1, bar: 2 }))
        assert_equal(%w[1 2], @client.call('HMGET', 'hash', %w[foo bar]))
      end

      def test_call_once
        assert_raises(ArgumentError) { @client.call_once }

        10.times do |i|
          assert_equal('OK', @client.call_once('SET', "key#{i}", i), "Case: SET: key#{i}")
          wait_for_replication
          assert_equal(i.to_s, @client.call_once('GET', "key#{i}"), "Case: GET: key#{i}")
        end

        assert(@client.call_once('PING') { |r| r == 'PONG' })

        assert_equal(2, @client.call_once('HSET', 'hash', { foo: 1, bar: 2 }))
        assert_equal(%w[1 2], @client.call_once('HMGET', 'hash', %w[foo bar]))
      end

      def test_blocking_call
        assert_raises(ArgumentError) { @client.blocking_call(TEST_TIMEOUT_SEC) }
        @client.call(*%w[RPUSH foo hello])
        @client.call(*%w[RPUSH foo world])
        wait_for_replication
        client_side_timeout = 1.0
        server_side_timeout = 0.5
        assert_equal(%w[foo world], @client.blocking_call(client_side_timeout, 'BRPOP', 'foo', server_side_timeout), 'Case: 1st')
        assert_equal(%w[foo hello], @client.blocking_call(client_side_timeout, 'BRPOP', 'foo', server_side_timeout), 'Case: 2nd')
        assert_nil(@client.blocking_call(client_side_timeout, 'BRPOP', 'foo', server_side_timeout), 'Case: 3rd')
        assert_raises(::RedisClient::ReadTimeoutError, 'Case: 4th') { @client.blocking_call(0.1, 'BRPOP', 'foo', 0) }
      end

      def test_scan
        assert_raises(ArgumentError) { @client.scan }

        10.times { |i| @client.call('SET', "key#{i}", i) }
        wait_for_replication
        want = (0..9).map { |i| "key#{i}" }
        got = []
        @client.scan('COUNT', '5') { |key| got << key }
        assert_equal(want, got.sort)
      end

      def test_sscan
        10.times do |i|
          10.times { |j| @client.call('SADD', "key#{i}", "member#{j}") }
          wait_for_replication
          want = (0..9).map { |j| "member#{j}" }
          got = []
          @client.sscan("key#{i}", 'COUNT', '5') { |member| got << member }
          assert_equal(want, got.sort)
        end
      end

      def test_hscan
        10.times do |i|
          10.times { |j| @client.call('HSET', "key#{i}", "field#{j}", j) }
          wait_for_replication
          want = (0..9).map { |j| "field#{j}" }
          got = []
          @client.hscan("key#{i}", 'COUNT', '5') { |field| got << field }
          assert_equal(want, got.sort)
        end
      end

      def test_zscan
        10.times do |i|
          10.times { |j| @client.call('ZADD', "key#{i}", j, "member#{j}") }
          wait_for_replication
          want = (0..9).map { |j| "member#{j}" }
          got = []
          @client.zscan("key#{i}", 'COUNT', '5') { |member| got << member }
          assert_equal(want, got.sort)
        end
      end

      def test_pipelined
        assert_empty([], @client.pipelined { |_| 1 + 1 })

        want = (0..9).map { 'OK' } + (1..3).to_a + %w[PONG]
        got = @client.pipelined do |pipeline|
          10.times { |i| pipeline.call('SET', "string#{i}", i) }
          3.times { |i| pipeline.call('RPUSH', 'list', i) }
          pipeline.call_once('PING')
        end
        assert_equal(want, got)

        wait_for_replication

        want = %w[PONG] + (0..9).map(&:to_s) + [%w[list 2]]
        got = @client.pipelined do |pipeline|
          pipeline.call_once('PING')
          10.times { |i| pipeline.call('GET', "string#{i}") }
          pipeline.blocking_call(0.2, 'BRPOP', 'list', '0.1')
        end
        assert_equal(want, got)
      end

      def test_pubsub
        10.times do |i|
          pubsub = @client.pubsub
          pubsub.call('SUBSCRIBE', "channel#{i}")
          assert_equal(['subscribe', "channel#{i}", 1], pubsub.next_event(0.1))
        end

        sub = Fiber.new do |client|
          channel = 'my-channel'
          pubsub = client.pubsub
          pubsub.call('SUBSCRIBE', channel)
          assert_equal(['subscribe', channel, 1], pubsub.next_event(TEST_TIMEOUT_SEC))
          Fiber.yield(channel)
          Fiber.yield(pubsub.next_event(TEST_TIMEOUT_SEC))
        end

        channel = sub.resume(@client)
        @client.call('PUBLISH', channel, 'hello world')
        assert_equal(['message', channel, 'hello world'], sub.resume)
      end

      def test_close
        assert_nil(@client.close)
      end

      def test_dedicated_commands
        10.times { |i| @client.call('SET', "key#{i}", i) }
        wait_for_replication
        [
          { command: %w[ACL HELP], is_a: Array },
          { command: ['WAIT', TEST_REPLICA_SIZE, '1'], is_a: Integer },
          { command: %w[KEYS *], want: (0..9).map { |i| "key#{i}" } },
          { command: %w[DBSIZE], want: (0..9).size },
          { command: %w[SCAN], is_a: Array },
          { command: %w[LASTSAVE], is_a: Array },
          { command: %w[ROLE], is_a: Array },
          { command: %w[CONFIG RESETSTAT], want: 'OK' },
          { command: %w[CONFIG GET maxmemory], is_a: Hash },
          { command: %w[CLIENT LIST], is_a: Array },
          { command: %w[CLIENT PAUSE 100], want: 'OK' },
          { command: %w[CLIENT INFO], is_a: String },
          { command: %w[CLUSTER SET-CONFIG-EPOCH 0], error: ::RedisClient::Cluster::OrchestrationCommandNotSupported },
          { command: %w[CLUSTER SAVECONFIG], want: 'OK' },
          { command: %w[CLUSTER GETKEYSINSLOT 13252 1], want: %w[key0] },
          { command: %w[CLUSTER NODES], is_a: String },
          { command: %w[READONLY], error: ::RedisClient::Cluster::OrchestrationCommandNotSupported },
          { command: %w[MEMORY STATS], is_a: Array },
          { command: %w[MEMORY PURGE], want: 'OK' },
          { command: %w[MEMORY USAGE key0], is_a: Integer },
          { command: %w[SCRIPT DEBUG NO], want: 'OK' },
          { command: %w[SCRIPT FLUSH], want: 'OK' },
          { command: %w[SCRIPT EXISTS b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c], want: [0] },
          { command: %w[PUBSUB CHANNELS test-channel*], want: [] },
          { command: %w[PUBSUB NUMSUB test-channel], want: { 'test-channel' => 0 } },
          { command: %w[PUBSUB NUMPAT], want: 0 },
          { command: %w[PUBSUB HELP], is_a: Array },
          { command: %w[MULTI], error: ::RedisClient::Cluster::AmbiguousNodeError },
          { command: %w[FLUSHDB], want: 'OK' }
        ].each do |c|
          msg = "Case: #{c[:command].join(' ')}"
          got = -> { @client.call(*c[:command]) }
          if c.key?(:error)
            assert_raises(c[:error], msg, &got)
          elsif c.key?(:is_a)
            assert_instance_of(c[:is_a], got.call, msg)
          else
            assert_equal(c[:want], got.call, msg)
          end
        end
      end

      def test_compatibility_with_redis_gem
        assert_equal('OK', @client.set('foo', 100))
        wait_for_replication
        assert_equal('100', @client.get('foo'))
        assert_raises(NoMethodError) { @client.densaugeo('1m') }
      end

      private

      def wait_for_replication
        client_side_timeout = TEST_TIMEOUT_SEC + 1.0
        server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
        @client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
      end
    end

    class PrimaryOnly < TestingWrapper
      include Mixin

      def new_test_client
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          **TEST_GENERIC_OPTIONS
        )
        ::RedisClient::Cluster.new(config)
      end
    end

    class ScaleRead < TestingWrapper
      include Mixin

      def new_test_client
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          replica: true,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          **TEST_GENERIC_OPTIONS
        )
        ::RedisClient::Cluster.new(config)
      end
    end

    class Pooled < TestingWrapper
      include Mixin

      def new_test_client
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          **TEST_GENERIC_OPTIONS
        )
        ::RedisClient::Cluster.new(config, pool: { timeout: TEST_TIMEOUT_SEC, size: 2 })
      end
    end
  end
end
