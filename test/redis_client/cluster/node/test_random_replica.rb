# frozen_string_literal: true

require 'uri'
require 'testing_helper'

class RedisClient
  class Cluster
    class Node
      class TestRandomReplica < TestingWrapper
        def setup
          test_config = ::RedisClient::ClusterConfig.new(
            nodes: TEST_NODE_URIS,
            fixed_hostname: TEST_FIXED_HOSTNAME,
            **TEST_GENERIC_OPTIONS
          )
          test_node_info = ::RedisClient::Cluster::Node.load_info(test_config.per_node_key)
          if TEST_FIXED_HOSTNAME
            test_node_info.each do |info|
              _, port = ::RedisClient::Cluster::NodeKey.split(info[:node_key])
              info[:node_key] = ::RedisClient::Cluster::NodeKey.build_from_host_port(TEST_FIXED_HOSTNAME, port)
            end
          end
          node_addrs = test_node_info.map { |info| ::RedisClient::Cluster::NodeKey.hashify(info[:node_key]) }
          test_config.update_node(node_addrs)
          @options = test_config.per_node_key
          test_node = ::RedisClient::Cluster::Node.new(@options, node_info: test_node_info)
          @replications = test_node.instance_variable_get(:@replications)
          test_node&.each(&:close)
          @test_topology = ::RedisClient::Cluster::Node::RandomReplica.new(
            @replications,
            @options,
            nil,
            **TEST_GENERIC_OPTIONS
          )
        end

        def teardown
          @test_topology&.clients&.each_value(&:close)
        end

        def test_clients_with_redis_client
          got = @test_topology.clients
          got.each_value { |client| assert_instance_of(::RedisClient, client) }
          assert_equal(%w[master slave], got.map { |_, v| v.call('ROLE').first }.uniq.sort)
        end

        def test_clients_with_pooled_redis_client
          test_topology = ::RedisClient::Cluster::Node::RandomReplica.new(
            @replications,
            @options,
            { timeout: 3, size: 2 },
            **TEST_GENERIC_OPTIONS
          )

          got = test_topology.clients
          got.each_value { |client| assert_instance_of(::RedisClient::Pooled, client) }
          assert_equal(%w[master slave], got.map { |_, v| v.call('ROLE').first }.uniq.sort)
        ensure
          test_topology&.clients&.each_value(&:close)
        end

        def test_primary_clients
          got = @test_topology.primary_clients
          got.each_value do |client|
            assert_instance_of(::RedisClient, client)
            assert_equal('master', client.call('ROLE').first)
          end
        end

        def test_replica_clients
          got = @test_topology.replica_clients
          got.each_value do |client|
            assert_instance_of(::RedisClient, client)
            assert_equal('slave', client.call('ROLE').first)
          end
        end

        def test_clients_for_scanning
          got = @test_topology.clients_for_scanning
          got.each_value { |client| assert_instance_of(::RedisClient, client) }
          assert_equal(TEST_SHARD_SIZE, got.size)
        end

        def test_find_node_key_of_replica
          want = 'dummy_key'
          got = @test_topology.find_node_key_of_replica('dummy_key')
          assert_equal(want, got)

          primary_key = @replications.keys.first
          replica_keys = @replications.fetch(primary_key)
          got = @test_topology.find_node_key_of_replica(primary_key)
          assert_includes(replica_keys, got)
        end

        def test_any_primary_node_key
          got = @test_topology.any_primary_node_key
          assert_includes(@replications.keys, got)
        end

        def test_any_replica_node_key
          got = @test_topology.any_replica_node_key
          assert_includes(@replications.values.flatten, got)
        end
      end
    end
  end
end
