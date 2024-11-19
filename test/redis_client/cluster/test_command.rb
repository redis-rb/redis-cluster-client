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
              'get' => { first_key_position: 1, key_step: 1, write?: false, readonly?: true },
              'set' => { first_key_position: 1, key_step: 2, write?: true, readonly?: false }
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
          { command: %w[set foo 1], want: 'foo' },
          { command: %w[SET foo 1], want: 'foo' },
          { command: %w[get foo], want: 'foo' },
          { command: %w[get foo{bar}baz], want: 'foo{bar}baz' },
          { command: %w[mget foo bar baz], want: 'foo' },
          { command: ['eval', 'return ARGV[1]', '0', 'hello'], want: 'hello' },
          { command: %w[evalsha sha1 2 foo bar baz zap], want: 'foo' },
          { command: %w[migrate host port key 0 5 copy], want: 'key' },
          { command: ['migrate', 'host', 'port', '', '0', '5', 'copy', 'keys', 'key'], want: 'key' },
          { command: %w[zinterstore out 2 zset1 zset2 weights 2 3], want: 'zset1' },
          { command: %w[zunionstore out 2 zset1 zset2 weights 2 3], want: 'zset1' },
          { command: %w[object encoding key], want: 'key' },
          { command: %w[memory help], want: '' },
          { command: %w[memory usage key], want: 'key' },
          { command: %w[xread count 2 streams mystream writers 0-0 0-0], want: 'mystream' },
          { command: %w[xreadgroup group group consumer streams key id], want: 'key' },
          { command: %w[unknown foo bar], want: '' }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.extract_first_key(c[:command])
          assert_equal(c[:want], got, msg)
        end
      end

      def test_should_send_to_primary?
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: %w[set foo 1], want: true },
          { command: %w[SET foo 1], want: true },
          { command: %w[get foo], want: false },
          { command: %w[GET foo], want: false },
          { command: %w[unknown foo bar], want: nil },
          { command: [], want: nil }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.should_send_to_primary?(c[:command])
          c[:want].nil? ? assert_nil(got, msg) : assert_equal(c[:want], got, msg)
        end
      end

      def test_should_send_to_replica?
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: %w[set foo 1], want: false },
          { command: %w[SET foo 1], want: false },
          { command: %w[get foo], want: true },
          { command: %w[GET foo], want: true },
          { command: %w[unknown foo bar], want: nil },
          { command: [], want: nil }
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
    end
  end
end
