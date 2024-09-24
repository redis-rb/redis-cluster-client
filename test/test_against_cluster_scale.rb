# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterScale < TestingWrapper
  WAIT_SEC = 1
  MAX_ATTEMPTS = 20
  NUMBER_OF_KEYS = 20_000

  def self.test_order
    :alpha
  end

  def setup
    @captured_commands = ::Middlewares::CommandCapture::CommandBuffer.new
    @redirect_count = ::Middlewares::RedirectCount::Counter.new
    @client = ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      custom: { captured_commands: @captured_commands, redirect_count: @redirect_count },
      middlewares: [::Middlewares::CommandCapture, ::Middlewares::RedirectCount],
      **TEST_GENERIC_OPTIONS
    ).new_client
    @client.call('echo', 'init')
    @captured_commands.clear
    @redirect_count.clear
    @cluster_down_error_count = 0
  end

  def teardown
    @client&.close
    @controller&.close
    print "#{@redirect_count.get}, "\
      "ClusterNodesCall: #{@captured_commands.count('cluster', 'nodes')}, "\
      "ClusterDownError: #{@cluster_down_error_count} = "
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
    refute(@captured_commands.count('cluster', 'nodes').zero?, @captured_commands.to_a.map(&:command))
  end

  def test_02_scale_in
    @controller = build_cluster_controller(TEST_NODE_URIS + build_additional_node_urls, shard_size: 4)
    @controller.scale_in

    NUMBER_OF_KEYS.times do |i|
      got = retry_call(attempts: MAX_ATTEMPTS) { @client.call('GET', "key#{i}") }
      assert_equal(i.to_s, got, "Case: key#{i}")
    end

    want = TEST_NODE_URIS.size
    got = @client.instance_variable_get(:@router)
                 .instance_variable_get(:@node)
                 .instance_variable_get(:@topology)
                 .instance_variable_get(:@clients)
                 .size
    assert_equal(want, got, 'Case: number of nodes')
    refute(@captured_commands.count('cluster', 'nodes').zero?, @captured_commands.to_a.map(&:command))
  end

  private

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    swap_timeout(@client, timeout: 0.1) do |client|
      client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
    end
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

  def retry_call(attempts:)
    loop do
      raise MaxRetryExceeded if attempts <= 0

      attempts -= 1
      break yield
    rescue ::RedisClient::CommandError => e
      raise unless e.message.start_with?('CLUSTERDOWN Hash slot not served')

      @cluster_down_error_count += 1
      sleep WAIT_SEC
    end
  end
end
