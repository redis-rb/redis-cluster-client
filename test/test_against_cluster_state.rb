# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterState < TestingWrapper
  SLOT_SIZE = 16_384

  module Mixin
    def setup
      @controller = ClusterController.new(
        TEST_NODE_URIS,
        replica_size: TEST_REPLICA_SIZE,
        **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
      )
      @controller.rebuild
      @client = new_test_client
    end

    def teardown
      @controller.rebuild
      @controller.close
      @client.close
    end

    def test_the_state_of_cluster_down
      @controller.down
      assert_raises(::RedisClient::CommandError) { @client.call('SET', 'key1', 1) }
      assert_equal('fail', fetch_cluster_info('cluster_state'))
    end

    def test_the_state_of_cluster_failover
      @controller.failover
      1000.times { |i| assert_equal('OK', @client.call('SET', "key#{i}", i)) }
      wait_for_replication
      1000.times { |i| assert_equal(i.to_s, @client.call('GET', "key#{i}")) }
      assert_equal('ok', fetch_cluster_info('cluster_state'))
    end

    def test_the_state_of_cluster_resharding
      @client.pipelined { |pipeline| 100_000.times { |i| pipeline.call('SET', "{key}#{i}", i) } }

      slot = SLOT_SIZE.times.max_by { |i| @client.call('CLUSTER', 'COUNTKEYSINSLOT', i) }
      src = @client.instance_variable_get(:@node).find_node_key_of_primary(slot)
      dest = @client.instance_variable_get(:@node).primary_node_keys.reject { |k| k == src }.sample

      @controller.start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      100_000.times { |i| assert_equal(i.to_s, @client.call('GET', "{key}#{i}"), "Case: GET: #{i}") }
      @controller.finish_resharding(slot: slot, dest_node_key: dest)
    end

    def test_the_state_of_cluster_resharding_with_pipelining
      skip('TODO: https://github.com/redis-rb/redis-cluster-client/issues/37')
      @client.pipelined { |pipeline| 100_000.times { |i| pipeline.call('SET', "{key}#{i}", i) } }

      slot = SLOT_SIZE.times.max_by { |i| @client.call('CLUSTER', 'COUNTKEYSINSLOT', i) }
      src = @client.instance_variable_get(:@node).find_node_key_of_primary(slot)
      dest = @client.instance_variable_get(:@node).primary_node_keys.reject { |k| k == src }.sample

      @controller.start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      values = @client.pipelined { |pipeline| 100_000.times { |i| pipeline.call('GET', "{key}#{i}") } }
      100_000.times { |i| assert_equal(i.to_s, values[i], "Case: GET: #{i}") }
      @controller.finish_resharding(slot: slot, dest_node_key: dest)
    end

    private

    def wait_for_replication
      @client.call('WAIT', TEST_REPLICA_SIZE, (TEST_TIMEOUT_SEC * 1000).to_i)
    end

    def fetch_cluster_info(key)
      @client.call('CLUSTER', 'INFO').split("\r\n").to_h { |v| v.split(':') }.fetch(key)
    end
  end

  class PrimaryOnly < TestingWrapper
    include Mixin

    def new_test_client
      config = ::RedisClient::ClusterConfig.new(
        nodes: TEST_NODE_URIS,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      )
      ::RedisClient::Cluster.new(config)
    end
  end

  class ScaleRead < TestingWrapper
    include Mixin

    def new_test_client
      config = ::RedisClient::ClusterConfig.new(
        nodes: TEST_NODE_URIS,
        replica: true,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      )
      ::RedisClient::Cluster.new(config)
    end
  end

  class Pooled < TestingWrapper
    include Mixin

    def new_test_client
      config = ::RedisClient::ClusterConfig.new(
        nodes: TEST_NODE_URIS,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        **TEST_GENERIC_OPTIONS
      )
      ::RedisClient::Cluster.new(config, pool: { timeout: TEST_TIMEOUT_SEC, size: 2 })
    end
  end
end
