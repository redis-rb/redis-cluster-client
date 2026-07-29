# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class Cluster
    class Router
      class TestRoutingTable < TestingWrapper
        def test_build
          [
            { value: { 'echo' => { request_policy: 'all_shards' } }, name: 'echo', want: :send_command_to_primaries },
            { value: { 'echo' => { request_policy: 'all_nodes' } }, name: 'echo', want: :send_command_to_all_nodes },
            { value: { 'echo' => { request_policy: :all_shards } }, name: 'echo', want: :send_command_to_primaries },
            { value: { echo: { 'request_policy' => 'all_shards' } }, name: 'echo', want: :send_command_to_primaries },
            { value: { 'ECHO' => { request_policy: 'all_shards' } }, name: 'echo', want: :send_command_to_primaries },
            { value: { 'echo' => { request_policy: 'all_shards', response_policy: 'all_succeeded' } }, name: 'echo', want: :send_command_to_primaries },
            { value: { 'echo' => { request_policy: 'all_shards', response_policy: 'one_succeeded' } }, name: 'echo', want: :send_command_to_primaries_leniently },
            { value: { 'echo' => { request_policy: 'all_nodes', response_policy: 'one_succeeded' } }, name: 'echo', want: :send_command_to_all_nodes_leniently },
            { value: { 'ping' => { request_policy: 'all_shards' } }, name: 'ping', want: :send_command_to_primaries },
            { value: { 'keys' => nil }, name: 'keys', removed: true },
            { value: { 'keys' => {} }, name: 'keys', removed: true },
            { value: { 'echo' => nil }, name: 'echo', removed: true },
            { value: { 'echo' => { request_policy: 'all_over' } }, error: true },
            { value: { 'echo' => { request_policy: 'multi_shard' } }, error: true },
            { value: { 'echo' => { request_policy: 'all_shards', response_policy: 'special' } }, error: true },
            { value: { 'echo' => { response_policy: 'agg_sum' } }, error: true },
            { value: { 'echo' => { request_policy: 'all_shards', foo: true } }, error: true },
            { value: { 'echo' => 'all_shards' }, error: true },
            { value: { 'multi' => { request_policy: 'all_shards' } }, error: true },
            { value: { 'watch' => nil }, error: true },
            { value: { 'SUBSCRIBE' => {} }, error: true },
            { value: { 'config get' => { request_policy: 'all_shards' } }, error: true },
            { value: { '' => {} }, error: true },
            { value: { 123 => {} }, error: true },
            { value: [], error: true },
            { value: 'all_shards', error: true }
          ].each_with_index do |c, idx|
            msg = "Case: #{idx}: #{c}"
            got = -> { ::RedisClient::Cluster::Router::RoutingTable.build(c[:value]) }
            if c.key?(:error)
              assert_raises(ArgumentError, msg, &got)
            elsif c.key?(:removed)
              table = got.call
              assert_predicate(table, :frozen?, msg)
              refute(table.key?(c[:name]), msg)
              refute(table.key?(c[:name].upcase), msg)
              assert_equal(:send_command_to_replicas, table['dbsize'].method_name, msg)
            else
              table = got.call
              assert_predicate(table, :frozen?, msg)
              assert_equal(c[:want], table[c[:name]].method_name, msg)
              assert_same(table[c[:name]], table[c[:name].upcase], msg)
              assert_equal(:send_command_to_replicas, table['dbsize'].method_name, msg)
            end
          end
        end

        def test_build_default_table
          # The table object is shared unless the overrides are given.
          assert_same(
            ::RedisClient::Cluster::Router::RoutingTable.build(nil),
            ::RedisClient::Cluster::Router::RoutingTable.build({})
          )
          assert_equal(:send_ping_command, ::RedisClient::Cluster::Router::RoutingTable.build(nil)['ping'].method_name)
        end

        def test_build_reply_transformer
          # The response policy decides the aggregation of the replies of the nodes.
          table = ::RedisClient::Cluster::Router::RoutingTable.build(
            'echo' => { request_policy: 'all_shards', response_policy: 'agg_sum' },
            'time' => { request_policy: 'all_nodes', response_policy: 'all_succeeded' },
            'lolwut' => { request_policy: 'all_shards' }
          )

          assert_equal(3, table['echo'].reply_transformer.call([1, 2, 'x']))
          assert_equal('a', table['time'].reply_transformer.call(%w[a b]))
          assert_nil(table['lolwut'].reply_transformer)
        end

        def test_validate!
          assert_raises(ArgumentError) { ::RedisClient::Cluster::Router::RoutingTable.validate!('all_shards') }
          assert_raises(ArgumentError) { ::RedisClient::Cluster::Router::RoutingTable.validate!('echo' => { request_policy: 'all_over' }) }

          # The valid inputs raise nothing.
          ::RedisClient::Cluster::Router::RoutingTable.validate!(nil)
          ::RedisClient::Cluster::Router::RoutingTable.validate!({})
          ::RedisClient::Cluster::Router::RoutingTable.validate!('echo' => { request_policy: 'all_shards' })
        end

        def test_find_policy_action
          assert_equal(
            :send_command_to_primaries,
            ::RedisClient::Cluster::Router::RoutingTable.find_policy_action('all_shards', nil).method_name
          )
          assert_equal(
            :send_command_to_all_nodes_leniently,
            ::RedisClient::Cluster::Router::RoutingTable.find_policy_action('all_nodes', 'one_succeeded').method_name
          )
          assert_nil(::RedisClient::Cluster::Router::RoutingTable.find_policy_action('multi_shard', nil))
          assert_nil(::RedisClient::Cluster::Router::RoutingTable.find_policy_action('all_shards', 'special'))
          assert_nil(::RedisClient::Cluster::Router::RoutingTable.find_policy_action(nil, nil))
        end
      end
    end
  end
end
