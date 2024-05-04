# frozen_string_literal: true

require 'set'
require 'testing_helper'

class RedisClient
  class Cluster
    class TestCommand < TestingWrapper
      def setup
        @raw_clients = TEST_NODE_URIS.map { |addr| ::RedisClient.config(url: addr, **TEST_GENERIC_OPTIONS).new_client }
      end

      def teardown
        @raw_clients&.each(&:close)
      end

      def test_load
        [
          { nodes: @raw_clients, error: nil },
          { nodes: [], error: ::RedisClient::Cluster::InitialSetupError },
          { nodes: [''], error: NoMethodError },
          { nodes: nil, error: ::RedisClient::Cluster::InitialSetupError }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = -> { ::RedisClient::Cluster::Command.load(c[:nodes]) }
          if c[:error].nil?
            assert_instance_of(::RedisClient::Cluster::Command, got.call, msg)
          else
            assert_raises(c[:error], msg, &got)
          end
        end
      end

      def test_load_slow_timeout
        nodes = @raw_clients
        assert_equal(TEST_TIMEOUT_SEC, nodes.first.read_timeout)
        nodes.first.singleton_class.prepend(Module.new do
          def call(...)
            @slow_timeout = read_timeout
            super
          end
        end)
        ::RedisClient::Cluster::Command.load(nodes, slow_command_timeout: 9)
        assert_equal(9, nodes.first.instance_variable_get(:@slow_timeout))
        assert_equal(TEST_TIMEOUT_SEC, nodes.first.read_timeout)
      end

      def test_parse_command_reply
        [
          {
            rows: [
              ['get', 2, Set['readonly', 'fast'], 1, -1, 1, Set['@read', '@string', '@fast'], Set[], Set[], Set[]],
              ['set', -3, Set['write', 'denyoom', 'movablekeys'], 1, -1, 2, Set['@write', '@string', '@slow'], Set[], Set[], Set[]]
            ],
            want: {
              'get' => { first_key_position: 1, last_key_position: -1, key_step: 1, write?: false, readonly?: true },
              'set' => { first_key_position: 1, last_key_position: -1, key_step: 2, write?: true, readonly?: false }
            }
          },
          {
            rows: [
              ['GET', 2, Set['readonly', 'fast'], 1, -1, 1, Set['@read', '@string', '@fast'], Set[], Set[], Set[]]
            ],
            want: {
              'get' => { first_key_position: 1, last_key_position: -1, key_step: 1, write?: false, readonly?: true }
            }
          },
          { rows: [[]], want: {} },
          { rows: [], want: {} },
          { rows: nil, want: {} }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = ::RedisClient::Cluster::Command.send(:parse_command_reply, c[:rows])
          assert_equal(c[:want].size, got.size, msg)
          assert_equal(c[:want].keys.sort, got.keys.sort, msg)
          c[:want].each do |k, v|
            assert_equal(v, got[k].to_h, "#{msg}: #{k}")
          end
        end
      end

      def test_extract_first_key
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: %w[SET foo 1], want: 'foo' },
          { command: %w[GET foo], want: 'foo' },
          { command: %w[GET foo{bar}baz], want: 'foo{bar}baz' },
          { command: %w[MGET foo bar baz], want: 'foo' },
          { command: %w[UNKNOWN foo bar], want: '' },
          { command: [['GET'], 'foo'], want: 'foo' },
          { command: ['GET', ['foo']], want: 'foo' },
          { command: [], want: '' },
          { command: nil, want: '' }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.extract_first_key(c[:command])
          assert_equal(c[:want], got, msg)
        end
      end

      def test_should_send_to_primary?
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: %w[SET foo 1], want: true },
          { command: %w[GET foo], want: false },
          { command: %w[UNKNOWN foo bar], want: nil },
          { command: [], want: nil },
          { command: nil, want: nil }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.should_send_to_primary?(c[:command])
          c[:want].nil? ? assert_nil(got, msg) : assert_equal(c[:want], got, msg)
        end
      end

      def test_should_send_to_replica?
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: %w[SET foo 1], want: false },
          { command: %w[GET foo], want: true },
          { command: %w[UNKNOWN foo bar], want: nil },
          { command: [], want: nil },
          { command: nil, want: nil }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.should_send_to_replica?(c[:command])
          c[:want].nil? ? assert_nil(got, msg) : assert_equal(c[:want], got, msg)
        end
      end

      def test_exists?
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { name: 'ping', want: true },
          { name: :ping, want: true },
          { name: 'PING', want: true },
          { name: 'densaugeo', want: false },
          { name: :densaugeo, want: false },
          { name: 'DENSAUGEO', want: false },
          { name: '', want: false },
          { name: 0, want: false },
          { name: nil, want: false }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.exists?(c[:name])
          assert_equal(c[:want], got, msg)
        end
      end

      def test_determine_first_key_position
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: %w[EVAL "return ARGV[1]" 0 hello], want: 3 },
          { command: [['EVAL'], '"return ARGV[1]"', 0, 'hello'], want: 3 },
          { command: %w[EVALSHA sha1 2 foo bar baz zap], want: 3 },
          { command: %w[MIGRATE host port key 0 5 COPY], want: 3 },
          { command: ['MIGRATE', 'host', 'port', '', '0', '5', 'COPY', 'KEYS', 'key'], want: 8 },
          { command: %w[ZINTERSTORE out 2 zset1 zset2 WEIGHTS 2 3], want: 3 },
          { command: %w[ZUNIONSTORE out 2 zset1 zset2 WEIGHTS 2 3], want: 3 },
          { command: %w[OBJECT HELP], want: 2 },
          { command: %w[MEMORY HELP], want: 0 },
          { command: %w[MEMORY USAGE key], want: 2 },
          { command: %w[XREAD COUNT 2 STREAMS mystream writers 0-0 0-0], want: 4 },
          { command: %w[XREADGROUP GROUP group consumer STREAMS key id], want: 5 },
          { command: %w[SET foo 1], want: 1 },
          { command: %w[set foo 1], want: 1 },
          { command: [['SET'], 'foo', 1], want: 1 },
          { command: %w[GET foo], want: 1 }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.send(:determine_first_key_position, c[:command])
          assert_equal(c[:want], got, msg)
        end
      end

      def test_determine_optional_key_position
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { params: { command: %w[XREAD COUNT 2 STREAMS mystream writers 0-0 0-0], option_name: 'streams' }, want: 4 },
          { params: { command: %w[XREADGROUP GROUP group consumer STREAMS key id], option_name: 'streams' }, want: 5 },
          { params: { command: %w[GET foo], option_name: 'bar' }, want: 0 },
          { params: { command: ['FOO', ['BAR'], 'BAZ'], option_name: 'bar' }, want: 2 },
          { params: { command: %w[FOO BAR BAZ], option_name: 'BAR' }, want: 2 },
          { params: { command: [], option_name: nil }, want: 0 },
          { params: { command: [], option_name: '' }, want: 0 },
          { params: { command: nil, option_name: nil }, want: 0 }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.send(:determine_optional_key_position, c[:params][:command], c[:params][:option_name])
          assert_equal(c[:want], got, msg)
        end
      end

      def test_determine_key_step
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { name: 'MSET', want: 2 },
          { name: 'MGET', want: 1 },
          { name: 'DEL', want: 1 },
          { name: 'EVALSHA', want: 1 }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.send(:determine_key_step, c[:name])
          assert_equal(c[:want], got, msg)
        end
      end

      def test_extract_all_keys
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: ['EVAL', 'return ARGV[1]', '0', 'hello'], want: [] },
          { command: ['EVAL', 'return ARGV[1]', '3', 'key1', 'key2', 'key3', 'arg1', 'arg2'], want: %w[key1 key2 key3] },
          { command: [['EVAL'], '"return ARGV[1]"', 0, 'hello'], want: [] },
          { command: %w[EVALSHA sha1 2 foo bar baz zap], want: %w[foo bar] },
          { command: %w[MIGRATE host port key 0 5 COPY], want: %w[key] },
          { command: ['MIGRATE', 'host', 'port', '', '0', '5', 'COPY', 'KEYS', 'key1'], want: %w[key1] },
          { command: ['MIGRATE', 'host', 'port', '', '0', '5', 'COPY', 'KEYS', 'key1', 'key2'], want: %w[key1 key2] },
          { command: %w[ZINTERSTORE out 2 zset1 zset2 WEIGHTS 2 3], want: %w[zset1 zset2] },
          { command: %w[ZUNIONSTORE out 2 zset1 zset2 WEIGHTS 2 3], want: %w[zset1 zset2] },
          { command: %w[OBJECT HELP], want: [] },
          { command: %w[MEMORY HELP], want: [] },
          { command: %w[MEMORY USAGE key], want: %w[key] },
          { command: %w[XREAD COUNT 2 STREAMS mystream writers 0-0 0-0], want: %w[mystream writers] },
          { command: %w[XREADGROUP GROUP group consumer STREAMS key id], want: %w[key] },
          { command: %w[SET foo 1], want: %w[foo] },
          { command: %w[set foo 1], want: %w[foo] },
          { command: [['SET'], 'foo', 1], want: %w[foo] },
          { command: %w[GET foo], want: %w[foo] },
          { command: %w[MGET foo bar baz], want: %w[foo bar baz] },
          { command: %w[MSET foo val bar val baz val], want: %w[foo bar baz] },
          { command: %w[BLPOP foo bar 0], want: %w[foo bar] }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.send(:extract_all_keys, c[:command])
          assert_equal(c[:want], got, msg)
        end
      end
    end
  end
end
