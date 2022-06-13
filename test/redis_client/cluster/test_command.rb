# frozen_string_literal: true

require 'set'
require 'testing_helper'
require 'redis_client/cluster'
require 'redis_client/cluster/command'

class RedisClient
  class Cluster
    class TestCommand < Minitest::Test
      include ::RedisClient::TestingHelper

      def test_parse_command_details
        keys = %i[arity flags first last step].freeze
        [
          {
            rows: [
              ['get', 2, Set['readonly', 'fast'], 1, 1, 1, Set['@read', '@string', '@fast'], Set[], Set[], Set[]],
              ['set', -3, Set['write', 'denyoom', 'movablekeys'], 1, 1, 1, Set['@write', '@string', '@slow'], Set[], Set[], Set[]]
            ],
            want: {
              'get' => { arity: 2, flags: Set['readonly', 'fast'], first: 1, last: 1, step: 1 },
              'set' => { arity: -3, flags: Set['write', 'denyoom', 'movablekeys'], first: 1, last: 1, step: 1 }
            }
          },
          {
            rows: [
              ['GET', 2, Set['readonly', 'fast'], 1, 1, 1, Set['@read', '@string', '@fast'], Set[], Set[], Set[]]
            ],
            want: {
              'get' => { arity: 2, flags: Set['readonly', 'fast'], first: 1, last: 1, step: 1 }
            }
          },
          { rows: [[]], want: {} },
          { rows: [], want: {} },
          { rows: nil, want: {} }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = ::RedisClient::Cluster::Command.send(:parse_command_details, c[:rows])
          assert_equal(c[:want].size, got.size, msg)
          assert_equal(c[:want].keys.sort, got.keys.sort, msg)
          c[:want].each do |k1, v|
            keys.each { |k2| assert_equal(v[k2], got[k1][k2], "#{msg}: #{k2}") }
          end
        end
      end

      def test_pick_details
        keys = %i[first_key_position write readonly].freeze
        [
          {
            details: {
              'get' => { arity: 2, flags: Set['readonly', 'fast'], first: 1, last: 1, step: 1 },
              'set' => { arity: -3, flags: Set['write', 'denyoom', 'movablekeys'], first: 1, last: 1, step: 1 }
            },
            want: {
              'get' => { first_key_position: 1, write: false, readonly: true },
              'set' => { first_key_position: 1, write: true, readonly: false }
            }
          },
          { details: {}, want: {} },
          { details: nil, want: {} }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          cmd = ::RedisClient::Cluster::Command.new(c[:details])
          got = cmd.send(:pick_details, c[:details])
          assert_equal(c[:want].size, got.size, msg)
          assert_equal(c[:want].keys.sort, got.keys.sort, msg)
          c[:want].each do |k1, v|
            keys.each { |k2| assert_equal(v[k2], got[k1][k2], "#{msg}: #{k2}") }
          end
        end
      end

      def test_dig_details
        cmd = ::RedisClient::Cluster::Command.new(
          {
            'get' => { arity: 2, flags: Set['readonly', 'fast'], first: 1, last: 1, step: 1 },
            'set' => { arity: -3, flags: Set['write', 'denyoom', 'movablekeys'], first: 1, last: 1, step: 1 }
          }
        )
        [
          { params: { command: %w[SET foo 1], key: :first_key_position }, want: 1 },
          { params: { command: %w[SET foo 1], key: :write }, want: true },
          { params: { command: %w[set foo 1], key: :write }, want: true },
          { params: { command: %w[SET foo 1], key: :readonly }, want: false },
          { params: { command: %w[GET foo], key: :first_key_position }, want: 1 },
          { params: { command: %w[GET foo], key: :write }, want: false },
          { params: { command: %w[GET foo], key: :readonly }, want: true },
          { params: { command: %w[get foo], key: :readonly }, want: true },
          { params: { command: %w[UNKNOWN foo], key: :readonly }, want: nil },
          { params: { command: [['SET'], 'foo', 1], key: :write }, want: true },
          { params: { command: [], key: :readonly }, want: nil },
          { params: { command: nil, key: :readonly }, want: nil }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = cmd.send(:dig_details, c[:params][:command], c[:params][:key])
          c[:want].nil? ? assert_nil(got, msg) : assert_equal(c[:want], got, msg)
        end
      end

      def test_determine_first_key_position
        cmd = ::RedisClient::Cluster::Command.load(@clients)
        [
          { command: %w[EVAL "return ARGV[1]" 0 hello], want: 3 },
          { command: [['EVAL'], '"return ARGV[1]"', 0, 'hello'], want: 3 },
          { command: %w[EVALSHA sha1 2 foo bar baz zap], want: 3 },
          { command: %w[MIGRATE host port key 0 5 COPY], want: 3 },
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
        cmd = ::RedisClient::Cluster::Command.load(@clients)
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

      def test_extract_hash_tag
        cmd = ::RedisClient::Cluster::Command.load(@clients)
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
          got = cmd.send(:extract_hash_tag, c[:key])
          assert_equal(c[:want], got, msg)
        end
      end
    end
  end
end
