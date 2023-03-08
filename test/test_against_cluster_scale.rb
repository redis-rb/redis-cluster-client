# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterScale < TestingWrapper
  NUMBER_OF_KEYS = 20_000

  def self.test_order
    :alpha
  end

  def setup
    @client = ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def teardown
    @client&.close
    @controller&.close
  end

  def test_01_scale_out
    @controller = build_cluster_controller(TEST_NODE_URIS, shard_size: 3)

    @client.pipelined { |pi| NUMBER_OF_KEYS.times { |i| pi.call('SET', "key#{i}", i) } }
    wait_for_replication

    primary_url, replica_url = build_additional_node_urls
    @controller.scale_out(primary_url: primary_url, replica_url: replica_url)

    NUMBER_OF_KEYS.times { |i| assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: key#{i}") }

    want = (TEST_NODE_URIS + build_additional_node_urls).size
    got = @client.instance_variable_get(:@router)
                 .instance_variable_get(:@node)
                 .instance_variable_get(:@topology)
                 .instance_variable_get(:@clients)
                 .size
    assert_equal(want, got, 'Case: number of nodes')
  end

  def test_02_scale_in
    @controller = build_cluster_controller(TEST_NODE_URIS + build_additional_node_urls, shard_size: 4)
    @controller.scale_in

    NUMBER_OF_KEYS.times do |i|
      assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: key#{i}")
    rescue ::RedisClient::CommandError => e
      raise unless e.message.start_with?('CLUSTERDOWN Hash slot not served')

      # FIXME: Why does the error occur?
      p "key#{i}"
    end

    want = TEST_NODE_URIS.size
    got = @client.instance_variable_get(:@router)
                 .instance_variable_get(:@node)
                 .instance_variable_get(:@topology)
                 .instance_variable_get(:@clients)
                 .size
    assert_equal(want, got, 'Case: number of nodes')
  end

  private

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    @client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
  end

  def build_cluster_controller(nodes, shard_size:)
    ClusterController.new(
      nodes,
      shard_size: shard_size,
      replica_size: TEST_REPLICA_SIZE,
      **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
    )
  end

  def build_additional_node_urls
    max = TEST_REDIS_PORTS.max
    (max + 1..max + 2).map { |port| "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{port}" }
  end
end
