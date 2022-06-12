# frozen_string_literal: true

require 'set'
require 'testing_helper'
require 'redis_client/cluster/command'

class RedisClient
  class Cluster
    class TestCommand < Minitest::Test
      def test_parse_command_details
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
          { rows: [], want: {} },
          { rows: nil, want: {} }
        ].each do |c|
          got = ::RedisClient::Cluster::Command.send(:parse_command_details, c[:rows])
          assert_equal(c[:want].size, got.size)
          assert_equal(c[:want].keys.sort, got.keys.sort)
          c[:want].each do |k, v|
            a = got[k]
            assert_equal(v[:arity], a[:arity])
            assert_equal(v[:flags], a[:flags])
            assert_equal(v[:first], a[:first])
            assert_equal(v[:last], a[:last])
            assert_equal(v[:step], a[:step])
          end
        end
      end

      def test_pick_details
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
        ].each do |c|
          cmd = ::RedisClient::Cluster::Command.new(c[:details])
          got = cmd.send(:pick_details, c[:details])
          assert_equal(c[:want].size, got.size)
          assert_equal(c[:want].keys.sort, got.keys.sort)
          c[:want].each do |k, v|
            a = got[k]
            assert_equal(v[:first_key_position], a[:first_key_position])
            assert_equal(v[:write], a[:write])
            assert_equal(v[:readonly], a[:readonly])
          end
        end
      end
    end
  end
end
