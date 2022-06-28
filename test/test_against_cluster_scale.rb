# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterScale < TestingWrapper
  NUMBER_OF_KEYS = 20_000

  def self.test_order
    :alpha
  end

  def teardown
    @client&.close
    @controller&.close
  end

  def test_01_scale_out
    @client = build_testee_client(TEST_NODE_URIS)
    @controller = build_cluster_controller(TEST_NODE_URIS)

    @client.pipelined { |pi| NUMBER_OF_KEYS.times { |i| pi.call('SET', "key#{i}", i) } }
    wait_for_replication

    primary_url, replica_url = build_additional_node_urls
    @controller.scale_out(primary_url: primary_url, replica_url: replica_url)

    NUMBER_OF_KEYS.times { |i| assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: key#{i}") }
  end

  def test_02_scale_in
    @client = build_testee_client(TEST_NODE_URIS + build_additional_node_urls)
    @controller = build_cluster_controller(TEST_NODE_URIS + build_additional_node_urls)

    @controller.scale_in
    NUMBER_OF_KEYS.times { |i| assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: key#{i}") }
  end

  private

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    @client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
  end

  def build_testee_client(nodes)
    config = ::RedisClient::ClusterConfig.new(
      nodes: nodes,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    )
    ::RedisClient::Cluster.new(config)
  end

  def build_cluster_controller(nodes)
    ClusterController.new(
      nodes,
      replica_size: TEST_REPLICA_SIZE,
      **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
    )
  end

  def build_additional_node_urls
    primary_url = "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{TEST_REDIS_PORTS.max + 1}"
    replica_url = "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{TEST_REDIS_PORTS.max + 2}"
    [primary_url, replica_url]
  end
end