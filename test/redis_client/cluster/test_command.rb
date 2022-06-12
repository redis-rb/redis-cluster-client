# frozen_string_literal: true

require 'set'
require 'testing_helper'
require 'redis_client/cluster/command'

class RedisClient
  class Cluster
    class TestCommand < Minitest::Test
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
        details = {
          'get' => { arity: 2, flags: Set['readonly', 'fast'], first: 1, last: 1, step: 1 },
          'set' => { arity: -3, flags: Set['write', 'denyoom', 'movablekeys'], first: 1, last: 1, step: 1 }
        }

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
          cmd = ::RedisClient::Cluster::Command.new(details)
          got = cmd.send(:dig_details, c[:params][:command], c[:params][:key])
          c[:want].nil? ? assert_nil(got, msg) : assert_equal(c[:want], got, msg)
        end
      end
    end
  end
end
