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

      def test_load_restores_read_timeout_when_call_raises
        fake_node_class = Struct.new(:read_timeout, :timeout_writes) do
          def call(*)
            raise ::RedisClient::ConnectionError, 'boom'
          end
        end

        fake_node = fake_node_class.new(1.0, [])
        fake_node.define_singleton_method(:read_timeout=) do |value|
          timeout_writes << value
          super(value)
        end

        assert_raises(::RedisClient::Cluster::InitialSetupError) do
          ::RedisClient::Cluster::Command.load([fake_node], slow_command_timeout: 5.0)
        end

        assert_equal([5.0, 1.0], fake_node.timeout_writes)
        assert_in_delta(1.0, fake_node.read_timeout)
      end

      def test_parse_command_reply
        [
          {
            rows: [
              ['get', 2, Set['readonly', 'fast'], 1, -1, 1, Set['@read', '@string', '@fast'], Set[], Set[], Set[]],
              ['set', -3, Set['write', 'denyoom', 'movablekeys'], 1, -1, 2, Set['@write', '@string', '@slow'], Set[], Set[], Set[]]
            ],
            want: {
              'get' => {
                first_key_position: 1, key_step: 1, write?: false, readonly?: true,
                request_policy: nil, response_policy: nil, subcommands: nil
              },
              'set' => {
                first_key_position: 1, key_step: 2, write?: true, readonly?: false,
                request_policy: nil, response_policy: nil, subcommands: nil
              }
            }
          },
          {
            # a container command of the redis 7.0 or later
            rows: [
              [
                'xinfo', -2, Set[], 0, 0, 0, Set['@slow'], Set[], Set[],
                [
                  ['xinfo|stream', -3, Set['readonly'], 2, 2, 1, Set['@read', '@stream', '@slow'], Set[], Set[], Set[]],
                  ['xinfo|help', 2, Set['loading', 'stale'], 0, 0, 0, Set['@stream', '@slow'], Set[], Set[], Set[]]
                ]
              ]
            ],
            want: {
              'xinfo' => {
                first_key_position: 0, key_step: 0, write?: false, readonly?: false,
                request_policy: nil, response_policy: nil,
                subcommands: {
                  'stream' => {
                    first_key_position: 2, key_step: 1, write?: false, readonly?: true,
                    request_policy: nil, response_policy: nil, subcommands: nil
                  },
                  'help' => {
                    first_key_position: 0, key_step: 0, write?: false, readonly?: false,
                    request_policy: nil, response_policy: nil, subcommands: nil
                  }
                }
              }
            }
          },
          {
            # command tips of the redis 7.0 or later
            rows: [
              [
                'function', -2, Set[], 0, 0, 0, Set['@slow'], Set[], Set[],
                [
                  [
                    'function|load', -3, Set['write', 'denyoom', 'noscript'], 0, 0, 0, Set['@write', '@slow', '@scripting'],
                    Set['request_policy:all_shards', 'response_policy:all_succeeded'], Set[], Set[]
                  ]
                ]
              ],
              ['dbsize', 1, Set['readonly', 'fast'], 0, 0, 0, Set['@keyspace', '@read', '@fast'],
               Set['request_policy:all_shards', 'response_policy:agg_sum'], Set[], Set[]]
            ],
            want: {
              'function' => {
                first_key_position: 0, key_step: 0, write?: false, readonly?: false,
                request_policy: nil, response_policy: nil,
                subcommands: {
                  'load' => {
                    first_key_position: 0, key_step: 0, write?: true, readonly?: false,
                    request_policy: 'all_shards', response_policy: 'all_succeeded', subcommands: nil
                  }
                }
              },
              'dbsize' => {
                first_key_position: 0, key_step: 0, write?: false, readonly?: true,
                request_policy: 'all_shards', response_policy: 'agg_sum', subcommands: nil
              }
            }
          },
          {
            # the reply of the redis 6.x doesn't have the tips and the subcommands
            rows: [['object', -2, Set['readonly'], 0, 0, 0, Set['@keyspace', '@read', '@slow']]],
            want: {
              'object' => {
                first_key_position: 2, key_step: 0, write?: false, readonly?: true,
                request_policy: nil, response_policy: nil, subcommands: nil
              }
            }
          },
          {
            # the reply of the redis 5.x doesn't have the ACL categories either
            rows: [['xgroup', -2, %w[write denyoom], 0, 0, 0]],
            want: {
              'xgroup' => {
                first_key_position: 2, key_step: 0, write?: true, readonly?: false,
                request_policy: nil, response_policy: nil, subcommands: nil
              }
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
            assert_equal(v, to_nested_hash(got[k]), "#{msg}: #{k}")
          end
        end
      end

      def test_get_spec_with_subcommands
        rows = [
          ['get', 2, Set['readonly', 'fast'], 1, -1, 1, Set[], Set[], Set[], Set[]],
          [
            'xinfo', -2, Set[], 0, 0, 0, Set[], Set[], Set[],
            [['xinfo|stream', -3, Set['readonly'], 2, 2, 1, Set[], Set[], Set[], Set[]]]
          ]
        ]
        cmd = ::RedisClient::Cluster::Command.new(::RedisClient::Cluster::Command.send(:parse_command_reply, rows))
        [
          { command: %w[xinfo stream mystream], want: 2 },
          { command: %w[XINFO STREAM mystream], want: 2 },
          { command: %w[xinfo groups mystream], want: 0 }, # an unknown subcommand falls back to the container
          { command: %w[xinfo], want: 0 },
          { command: %w[get foo], want: 1 },
          { command: %w[unknown foo], want: nil },
          { command: [], want: nil }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.get_spec(c[:command])&.first_key_position
          c[:want].nil? ? assert_nil(got, msg) : assert_equal(c[:want], got, msg)
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
          { command: %w[OBJECT ENCODING key], want: 'key' },
          { command: %w[memory help], want: '' },
          { command: %w[memory usage key], want: 'key' },
          { command: %w[xgroup create key group $], want: 'key' },
          { command: %w[xread count 2 streams mystream writers 0-0 0-0], want: 'mystream' },
          { command: %w[xreadgroup group group consumer streams key id], want: 'key' },
          { command: %w[unknown foo bar], want: nil },
          # The key positions of the subcommands are available in the redis 7.0 or later.
          { command: %w[xinfo stream key], want: 'key', supported_redis_version: 7 },
          { command: %w[XINFO STREAM key], want: 'key', supported_redis_version: 7 },
          { command: %w[xinfo help], want: '', supported_redis_version: 7 },
          { command: %w[client no-evict on], want: '', supported_redis_version: 7 }
        ].each_with_index do |c, idx|
          next if c.key?(:supported_redis_version) && c[:supported_redis_version] > TEST_REDIS_MAJOR_VERSION

          msg = "Case: #{idx}"
          got = cmd.get_spec(c[:command])&.extract_first_key(c[:command])
          if c[:want].nil?
            assert_nil(got, msg)
          else
            assert_equal(c[:want], got, msg)
          end
        end
      end

      def test_should_send_to_primary?
        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: %w[set foo 1], want: true },
          { command: %w[SET foo 1], want: true },
          { command: %w[get foo], want: false },
          { command: %w[GET foo], want: false },
          { command: %w[xgroup create key group $], want: true },
          { command: %w[unknown foo bar], want: nil },
          { command: [], want: nil },
          { command: %w[xinfo stream key], want: false, supported_redis_version: 7 }
        ].each_with_index do |c, idx|
          next if c.key?(:supported_redis_version) && c[:supported_redis_version] > TEST_REDIS_MAJOR_VERSION

          msg = "Case: #{idx}"
          got = cmd.get_spec(c[:command])&.should_send_to_primary?
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
          { command: [], want: nil },
          { command: %w[xinfo stream key], want: true, supported_redis_version: 7 }
        ].each_with_index do |c, idx|
          next if c.key?(:supported_redis_version) && c[:supported_redis_version] > TEST_REDIS_MAJOR_VERSION

          msg = "Case: #{idx}"
          got = cmd.get_spec(c[:command])&.should_send_to_replica?
          c[:want].nil? ? assert_nil(got, msg) : assert_equal(c[:want], got, msg)
        end
      end

      def test_command_tips
        skip('The command tips are available in the redis 7.0 or later.') if TEST_REDIS_MAJOR_VERSION < 7

        cmd = ::RedisClient::Cluster::Command.load(@raw_clients)
        [
          { command: %w[get foo], request_policy: nil, response_policy: nil },
          { command: %w[dbsize], request_policy: 'all_shards', response_policy: 'agg_sum' },
          { command: %w[function load lib], request_policy: 'all_shards', response_policy: 'all_succeeded' },
          { command: %w[function kill], request_policy: 'all_shards', response_policy: 'one_succeeded' },
          { command: %w[info], request_policy: 'all_shards', response_policy: 'special' },
          { command: %w[mget foo bar], request_policy: 'multi_shard', response_policy: nil }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          spec = cmd.get_spec(c[:command])

          refute_nil(spec, msg)
          assert_equal({ request_policy: c[:request_policy], response_policy: c[:response_policy] },
                       { request_policy: spec.request_policy, response_policy: spec.response_policy },
                       msg)
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

      private

      def to_nested_hash(spec)
        spec.to_h.tap do |h|
          h[:subcommands] = h[:subcommands]&.transform_values { |sub| to_nested_hash(sub) }
        end
      end
    end
  end
end
