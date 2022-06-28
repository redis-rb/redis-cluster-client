# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterScale < TestingWrapper
  NUMBER_OF_KEYS = 20_000

  def self.test_order
    :alpha
  end

  def setup
    config = ::RedisClient::ClusterConfig.new(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    )
    @client = ::RedisClient::Cluster.new(config)
    @controller = ClusterController.new(
      TEST_NODE_URIS,
      replica_size: TEST_REPLICA_SIZE,
      **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
    )
  end

  def teardown
    @client.close
    @controller.close
  end

  def test_01_scale_out
    @client.pipelined { |pi| NUMBER_OF_KEYS.times { |i| pi.call('SET', "key#{i}", i) } }
    wait_for_replication

    primary_url = "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{TEST_REDIS_PORTS.max + 1}"
    replica_url = "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{TEST_REDIS_PORTS.max + 2}"
    @controller.scale_out(primary_url: primary_url, replica_url: replica_url)

    NUMBER_OF_KEYS.times { |i| assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: key#{i}") }
  end

  def test_02_scale_in
    @controller.scale_in
    NUMBER_OF_KEYS.times { |i| assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: key#{i}") }
  end

  private

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    @client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
  end
end
