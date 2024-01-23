# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/node/testing_topology_mixin'

class RedisClient
  class Cluster
    class Node
      class TestRandomReplicaWithPrimary < TestingWrapper
        TESTING_TOPOLOGY_OPTIONS = { replica: true, replica_affinity: :random_with_primary }.freeze
        include TestingTopologyMixin

        def test_clients_with_redis_client
          got = @test_node.clients
          got.each { |client| assert_kind_of(::RedisClient, client) }
          assert_equal(%w[master slave], got.map { |v| v.call('ROLE').first }.uniq.sort)
        end

        def test_clients_with_pooled_redis_client
          test_node = make_node(pool: { timeout: 3, size: 2 })
          got = test_node.clients
          got.each { |client| assert_kind_of(::RedisClient::Pooled, client) }
          assert_equal(%w[master slave], got.map { |v| v.call('ROLE').first }.uniq.sort)
        end

        def test_primary_clients
          got = @test_node.primary_clients
          got.each do |client|
            assert_kind_of(::RedisClient, client)
            assert_equal('master', client.call('ROLE').first)
          end
        end

        def test_replica_clients
          got = @test_node.replica_clients
          got.each do |client|
            assert_kind_of(::RedisClient, client)
            assert_equal('slave', client.call('ROLE').first)
          end
        end

        def test_clients_for_scanning
          got = @test_node.clients_for_scanning
          got.each { |client| assert_kind_of(::RedisClient, client) }
          assert_equal(TEST_SHARD_SIZE, got.size)
        end

        def test_find_node_key_of_replica
          want = 'dummy_key'
          got = @test_topology.find_node_key_of_replica('dummy_key')
          assert_equal(want, got)

          primary_key = @replications.keys.first
          replica_keys = @replications.fetch(primary_key)
          got = @test_topology.find_node_key_of_replica(primary_key)
          assert_includes(replica_keys + [primary_key], got)
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
