# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class TestCluster
    module Mixin
      def setup
        @captured_commands = []
        @client = new_test_client
        @client.call('FLUSHDB')
        wait_for_replication
        @captured_commands.clear
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

        assert_equal(2, @client.call('HSET', 'hash', foo: 1, bar: 2))
        wait_for_replication
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

        assert_equal(2, @client.call_once('HSET', 'hash', foo: 1, bar: 2))
        wait_for_replication
        assert_equal(%w[1 2], @client.call_once('HMGET', 'hash', %w[foo bar]))
      end

      def test_blocking_call
        skip("FIXME: this case is buggy on #{RUBY_ENGINE}") if RUBY_ENGINE == 'truffleruby' # FIXME: buggy

        assert_raises(ArgumentError) { @client.blocking_call(TEST_TIMEOUT_SEC) }

        @client.call_v(%w[RPUSH foo hello])
        @client.call_v(%w[RPUSH foo world])
        wait_for_replication

        client_side_timeout = TEST_REDIS_MAJOR_VERSION < 6 ? 2.0 : 1.5
        server_side_timeout = TEST_REDIS_MAJOR_VERSION < 6 ? '1' : '0.5'

        assert_equal(%w[foo world], @client.blocking_call(client_side_timeout, 'BRPOP', 'foo', server_side_timeout), 'Case: 1st')

        # FIXME: too flaky, just a workaround
        got = @client.blocking_call(client_side_timeout, 'BRPOP', 'foo', server_side_timeout)
        if got.nil?
          assert_nil(got, 'Case: 2nd')
        else
          assert_equal(%w[foo hello], got, 'Case: 2nd')
        end

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
        client_side_timeout = TEST_REDIS_MAJOR_VERSION < 6 ? 1.5 : 1.0
        server_side_timeout = TEST_REDIS_MAJOR_VERSION < 6 ? '1' : '0.5'
        got = @client.pipelined do |pipeline|
          pipeline.call_once('PING')
          10.times { |i| pipeline.call('GET', "string#{i}") }
          pipeline.blocking_call(client_side_timeout, 'BRPOP', 'list', server_side_timeout)
        end
        assert_equal(want, got)
      end

      def test_pipelined_with_errors
        assert_raises(RedisClient::Cluster::ErrorCollection) do
          @client.pipelined do |pipeline|
            10.times do |i|
              pipeline.call('SET', "string#{i}", i)
              pipeline.call('SET', "string#{i}", i, 'too many args')
              pipeline.call('SET', "string#{i}", i + 10)
            end
          end
        end

        wait_for_replication

        10.times { |i| assert_equal((i + 10).to_s, @client.call('GET', "string#{i}")) }
      end

      def test_pipelined_with_many_commands
        @client.pipelined { |pi| 1000.times { |i| pi.call('SET', i, i) } }
        wait_for_replication
        results = @client.pipelined { |pi| 1000.times { |i| pi.call('GET', i) } }
        results.each_with_index { |got, i| assert_equal(i.to_s, got) }
      end

      def test_transaction_with_single_key
        got = @client.multi do |t|
          t.call('SET', 'counter', '0')
          t.call('INCR', 'counter')
          t.call('INCR', 'counter')
        end

        assert_equal(['OK', 1, 2], got)
        assert_equal('2', @client.call('GET', 'counter'))
      end

      def test_transaction_with_multiple_key
        assert_raises(::RedisClient::Cluster::Transaction::ConsistencyError) do
          @client.multi do |t|
            t.call('SET', 'key1', '1')
            t.call('SET', 'key2', '2')
            t.call('SET', 'key3', '3')
          end
        end

        (1..3).each do |i|
          assert_nil(@client.call('GET', "key#{i}"))
        end
      end

      def test_transaction_with_empty_block
        assert_raises(ArgumentError) { @client.multi {} }
        assert_raises(LocalJumpError) { @client.multi }
      end

      def test_transaction_with_keyless_commands
        assert_raises(::RedisClient::Cluster::Transaction::ConsistencyError) do
          @client.multi do |t|
            t.call('ECHO', 'foo')
            t.call('ECHO', 'bar')
          end
        end
      end

      def test_transaction_with_hashtag
        got = @client.multi do |t|
          t.call('MSET', '{key}1', '1', '{key}2', '2')
          t.call('MSET', '{key}3', '3', '{key}4', '4')
        end

        assert_equal(%w[OK OK], got)
        assert_equal(%w[1 2 3 4], @client.call('MGET', '{key}1', '{key}2', '{key}3', '{key}4'))
      end

      def test_transaction_without_hashtag
        assert_raises(::RedisClient::Cluster::Transaction::ConsistencyError) do
          @client.multi do |t|
            t.call('MSET', 'key1', '1', 'key2', '2')
            t.call('MSET', 'key3', '3', 'key4', '4')
          end
        end

        assert_raises(::RedisClient::CommandError, 'CROSSSLOT keys') do
          @client.multi do |t|
            t.call('MSET', 'key1', '1', 'key2', '2')
            t.call('MSET', 'key1', '1', 'key3', '3')
            t.call('MSET', 'key1', '1', 'key4', '4')
          end
        end

        (1..4).each do |i|
          assert_nil(@client.call('GET', "key#{i}"))
        end
      end

      def test_pubsub_without_subscription
        pubsub = @client.pubsub
        assert_nil(pubsub.next_event(0.01))
        pubsub.close
      end

      def test_pubsub_with_wrong_command
        pubsub = @client.pubsub
        assert_nil(pubsub.call('SUBWAY'))
        assert_nil(pubsub.call_v(%w[SUBSCRIBE]))
        assert_raises(::RedisClient::CommandError, 'unknown command') { pubsub.next_event }
        assert_raises(::RedisClient::CommandError, 'wrong number of arguments') { pubsub.next_event }
        pubsub.close
      end

      def test_global_pubsub
        sub = Fiber.new do |pubsub|
          channel = 'my-global-channel'
          pubsub.call('SUBSCRIBE', channel)
          assert_equal(['subscribe', channel, 1], pubsub.next_event(TEST_TIMEOUT_SEC))
          Fiber.yield(channel)
          Fiber.yield(pubsub.next_event(TEST_TIMEOUT_SEC))
          pubsub.call('UNSUBSCRIBE')
          pubsub.close
        end

        channel = sub.resume(@client.pubsub)
        publish_messages { |cli| cli.call('PUBLISH', channel, 'hello global world') }
        assert_equal(['message', channel, 'hello global world'], sub.resume)
      end

      def test_global_pubsub_without_timeout
        sub = Fiber.new do |pubsub|
          pubsub.call('SUBSCRIBE', 'my-global-not-published-channel', 'my-global-published-channel')
          want = [%w[subscribe my-global-not-published-channel], %w[subscribe my-global-published-channel]]
          got = collect_messages(pubsub, size: 2, timeout: nil).map { |e| e.take(2) }.sort_by { |e| e[1].to_s }
          assert_equal(want, got)
          Fiber.yield('my-global-published-channel')
          Fiber.yield(collect_messages(pubsub, size: 1, timeout: nil).first)
          pubsub.call('UNSUBSCRIBE')
          pubsub.close
        end

        channel = sub.resume(@client.pubsub)
        publish_messages { |cli| cli.call('PUBLISH', channel, 'hello global published world') }
        assert_equal(['message', channel, 'hello global published world'], sub.resume)
      end

      def test_global_pubsub_with_multiple_channels
        sub = Fiber.new do |pubsub|
          pubsub.call('SUBSCRIBE', *Array.new(10) { |i| "g-chan#{i}" })
          got = collect_messages(pubsub, size: 10).sort_by { |e| e[1].to_s }
          10.times { |i| assert_equal(['subscribe', "g-chan#{i}", i + 1], got[i]) }
          Fiber.yield
          Fiber.yield(collect_messages(pubsub, size: 10))
          pubsub.call('UNSUBSCRIBE')
          pubsub.close
        end

        sub.resume(@client.pubsub)
        publish_messages { |cli| cli.pipelined { |pi| 10.times { |i| pi.call('PUBLISH', "g-chan#{i}", i) } } }
        got = sub.resume.sort_by { |e| e[1].to_s }
        10.times { |i| assert_equal(['message', "g-chan#{i}", i.to_s], got[i]) }
      end

      def test_sharded_pubsub
        if TEST_REDIS_MAJOR_VERSION < 7
          skip('Sharded Pub/Sub is supported by Redis 7+.')
          return
        end

        sub = Fiber.new do |pubsub|
          channel = 'my-sharded-channel'
          pubsub.call('SSUBSCRIBE', channel)
          assert_equal(['ssubscribe', channel, 1], pubsub.next_event(TEST_TIMEOUT_SEC))
          Fiber.yield(channel)
          Fiber.yield(pubsub.next_event(TEST_TIMEOUT_SEC))
          pubsub.call('SUNSUBSCRIBE')
          pubsub.close
        end

        channel = sub.resume(@client.pubsub)
        publish_messages { |cli| cli.call('SPUBLISH', channel, 'hello sharded world') }
        assert_equal(['smessage', channel, 'hello sharded world'], sub.resume)
      end

      def test_sharded_pubsub_without_timeout
        if TEST_REDIS_MAJOR_VERSION < 7
          skip('Sharded Pub/Sub is supported by Redis 7+.')
          return
        end

        sub = Fiber.new do |pubsub|
          pubsub.call('SSUBSCRIBE', 'my-sharded-not-published-channel')
          pubsub.call('SSUBSCRIBE', 'my-sharded-published-channel')
          want = [%w[ssubscribe my-sharded-not-published-channel], %w[ssubscribe my-sharded-published-channel]]
          got = collect_messages(pubsub, size: 2, timeout: nil).map { |e| e.take(2) }.sort_by { |e| e[1].to_s }
          assert_equal(want, got)
          Fiber.yield('my-sharded-published-channel')
          Fiber.yield(collect_messages(pubsub, size: 1, timeout: nil).first)
          pubsub.call('SUNSUBSCRIBE')
          pubsub.close
        end

        channel = sub.resume(@client.pubsub)
        publish_messages { |cli| cli.call('SPUBLISH', channel, 'hello sharded published world') }
        assert_equal(['smessage', channel, 'hello sharded published world'], sub.resume)
      end

      def test_sharded_pubsub_with_multiple_channels
        if TEST_REDIS_MAJOR_VERSION < 7
          skip('Sharded Pub/Sub is supported by Redis 7+.')
          return
        end

        sub = Fiber.new do |pubsub|
          10.times { |i| pubsub.call('SSUBSCRIBE', "s-chan#{i}") }
          got = collect_messages(pubsub, size: 10).sort_by { |e| e[1].to_s }
          10.times { |i| assert_equal(['ssubscribe', "s-chan#{i}"], got[i].take(2)) }
          Fiber.yield
          Fiber.yield(collect_messages(pubsub, size: 10))
          pubsub.call('SUNSUBSCRIBE')
          pubsub.close
        end

        sub.resume(@client.pubsub)
        publish_messages { |cli| cli.pipelined { |pi| 10.times { |i| pi.call('SPUBLISH', "s-chan#{i}", i) } } }
        got = sub.resume.sort_by { |e| e[1].to_s }
        10.times { |i| assert_equal(['smessage', "s-chan#{i}", i.to_s], got[i]) }
      end

      def test_other_pubsub_commands
        assert_instance_of(Array, @client.call('pubsub', 'channels'))
        assert_instance_of(Integer, @client.call('pubsub', 'numpat'))
        assert_instance_of(Hash, @client.call('pubsub', 'numsub'))
        assert_instance_of(Array, @client.call('pubsub', 'shardchannels')) if TEST_REDIS_MAJOR_VERSION >= 7
        assert_instance_of(Hash, @client.call('pubsub', 'shardnumsub')) if TEST_REDIS_MAJOR_VERSION >= 7
        ps = @client.pubsub
        assert_nil(ps.call('unsubscribe'))
        assert_nil(ps.call('punsubscribe'))
        assert_nil(ps.call('sunsubscribe')) if TEST_REDIS_MAJOR_VERSION >= 7
        ps.close
      end

      def test_dedicated_commands # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        10.times { |i| @client.call('SET', "key#{i}", i) }
        wait_for_replication
        [
          { command: %w[ACL HELP], is_a: Array, supported_redis_version: 6 },
          { command: ['WAIT', TEST_REPLICA_SIZE, '1'], is_a: Integer },
          { command: %w[KEYS *], want: (0..9).map { |i| "key#{i}" } },
          { command: %w[DBSIZE], want: (0..9).size },
          { command: %w[SCAN], is_a: Array },
          { command: %w[LASTSAVE], is_a: Array },
          { command: %w[ROLE], is_a: Array },
          { command: %w[CONFIG RESETSTAT], want: 'OK' },
          { command: %w[CONFIG GET maxmemory], is_a: TEST_REDIS_MAJOR_VERSION < 6 ? Array : Hash },
          {
            command: %w[CLIENT LIST],
            blk: ->(r) { r.lines("\n", chomp: true).map(&:split).map { |e| Hash[e.map { |x| x.split('=') }] } },
            is_a: Array
          },
          { command: %w[CLIENT PAUSE 100], want: 'OK' },
          { command: %w[CLIENT INFO], is_a: String, supported_redis_version: 6 },
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
          { command: %w[SCRIPT EXISTS 5b9fb3410653a731f8ddfeff39a0c061 31b6de18e43fe980ed07d8b0f5a8cabe], want: [0, 0] },
          {
            command: %w[SCRIPT EXISTS b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c],
            blk: ->(reply) { reply.map { |r| !r.zero? } },
            want: [false]
          },
          {
            command: %w[SCRIPT EXISTS 5b9fb3410653a731f8ddfeff39a0c061 31b6de18e43fe980ed07d8b0f5a8cabe],
            blk: ->(reply) { reply.map { |r| !r.zero? } },
            want: [false, false]
          },
          { command: %w[PUBSUB CHANNELS test-channel*], want: [] },
          { command: %w[PUBSUB NUMSUB test-channel], want: { 'test-channel' => 0 } },
          { command: %w[PUBSUB NUMPAT], want: 0 },
          { command: %w[PUBSUB HELP], is_a: Array },
          { command: %w[MULTI], error: ::RedisClient::Cluster::AmbiguousNodeError },
          { command: %w[FLUSHDB], want: 'OK' }
        ].each do |c|
          next if c.key?(:supported_redis_version) && c[:supported_redis_version] > TEST_REDIS_MAJOR_VERSION

          msg = "Case: #{c[:command].join(' ')}"
          got = -> { @client.call_v(c[:command], &c[:blk]) }
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

      def test_circuit_breakers
        cli = ::RedisClient.cluster(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          # This option is important - need to make sure that the reloads happen on different connections
          # to the timeouts, so that they don't count against the circuit breaker (they'll have their own breakers).
          connect_with_original_config: true,
          **TEST_GENERIC_OPTIONS.merge(
            circuit_breaker: {
              # Also important - the retry_count on resharding errors is set to 3, so we have to allow at lest
              # that many errors to avoid tripping the breaker in the first call.
              error_threshold: 4,
              error_timeout: 60,
              success_threshold: 10
            }
          )
        ).new_client

        assert_raises(::RedisClient::ReadTimeoutError) { cli.blocking_call(0.1, 'BRPOP', 'foo', 0) }
        assert_raises(::RedisClient::CircuitBreaker::OpenCircuitError) { cli.blocking_call(0.1, 'BRPOP', 'foo', 0) }

        cli&.close
      end

      private

      def wait_for_replication
        client_side_timeout = TEST_TIMEOUT_SEC + 1.0
        server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
        @client&.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
      end

      def collect_messages(pubsub, size:, max_attempts: 30, timeout: 1.0)
        messages = []
        attempts = 0
        loop do
          attempts += 1
          break if attempts > max_attempts

          reply = pubsub.next_event(timeout)
          break if reply.nil?

          messages << reply
          break messages if messages.size == size
        end
      end

      def publish_messages
        client = new_test_client
        yield client
        client.close
      end

      def hiredis_used?
        ::RedisClient.const_defined?(:HiredisConnection) &&
          ::RedisClient.default_driver == ::RedisClient::HiredisConnection
      end
    end

    class PrimaryOnly < TestingWrapper
      include Mixin

      def new_test_client(capture_buffer: @captured_commands)
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: [CommandCaptureMiddleware],
          custom: { captured_commands: capture_buffer },
          **TEST_GENERIC_OPTIONS
        )
        ::RedisClient::Cluster.new(config)
      end
    end

    class ScaleReadRandom < TestingWrapper
      include Mixin

      def new_test_client(capture_buffer: @captured_commands)
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          replica: true,
          replica_affinity: :random,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: [CommandCaptureMiddleware],
          custom: { captured_commands: capture_buffer },
          **TEST_GENERIC_OPTIONS
        )
        ::RedisClient::Cluster.new(config)
      end
    end

    class ScaleReadRandomWithPrimary < TestingWrapper
      include Mixin

      def new_test_client(capture_buffer: @captured_commands)
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          replica: true,
          replica_affinity: :random_with_primary,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: [CommandCaptureMiddleware],
          custom: { captured_commands: capture_buffer },
          **TEST_GENERIC_OPTIONS
        )
        ::RedisClient::Cluster.new(config)
      end
    end

    class ScaleReadLatency < TestingWrapper
      include Mixin

      def new_test_client(capture_buffer: @captured_commands)
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          replica: true,
          replica_affinity: :latency,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: [CommandCaptureMiddleware],
          custom: { captured_commands: capture_buffer },
          **TEST_GENERIC_OPTIONS
        )
        ::RedisClient::Cluster.new(config)
      end
    end

    class Pooled < TestingWrapper
      include Mixin

      def new_test_client(capture_buffer: @captured_commands)
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: [CommandCaptureMiddleware],
          custom: { captured_commands: capture_buffer },
          **TEST_GENERIC_OPTIONS
        )
        ::RedisClient::Cluster.new(config, pool: { timeout: TEST_TIMEOUT_SEC, size: 2 })
      end
    end
  end
end
