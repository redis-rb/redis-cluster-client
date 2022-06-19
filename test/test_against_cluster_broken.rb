# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterBroken < TestingWrapper
  def setup
    @client = new_test_client
    @controller = new_cluster_controller
  end

  def teardown
    @client.close
    @controller.close
  end

  def test_several_nodes_are_down # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    node = @client.instance_variable_get(:@node)
    primary_node_key = node.primary_node_keys.sample
    replica_node_key = node.replica_node_keys.reject { |k| node.replicated?(primary_node_key, k) }.sample

    msgs = []
    msgs += @client.call('CLUSTER', 'NODES').split("\n")
    msgs += ["Primary: #{primary_node_key}, Replica: #{replica_node_key}"]

    node.instance_variable_get(:@clients).each do |node_key, raw_cli|
      next if node_key != primary_node_key && node_key != replica_node_key

      begin
        # @see https://github.com/redis/redis/blob/475563e2e941ebbdb83f50474bf2daa5ae276fcf/src/debug.c#L387-L493
        raw_cli.call('SHUTDOWN')
      rescue ::RedisClient::Error
        # pass
      end
    end

    skip('TODO')
    @controller.wait_for_cluster_to_be_ready

    errors = []
    begin
      @client.call_once('PING')
    rescue ::RedisClient::Error => e
      errors << e
    end

    msgs += @client.call('CLUSTER', 'NODES').split("\n")
    msgs += errors.group_by(&:message).map do |k, v|
      "#{v.first.class.name}: #{k}\n#{v.first.backtrace.take(10).join("\n")}"
    end
    msg = msgs.join("\n")

    assert_equal([1], @client.instance_variable_get(:@node).node_keys, msg)
    assert_equal(0, errors.size, msg)
  end

  private

  def new_test_client
    config = ::RedisClient::ClusterConfig.new(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    )
    ::RedisClient::Cluster.new(config)
  end

  def new_cluster_controller
    @controller = ClusterController.new(
      TEST_NODE_URIS,
      replica_size: TEST_REPLICA_SIZE,
      **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
    )
  end
end
