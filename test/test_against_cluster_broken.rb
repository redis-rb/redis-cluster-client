# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterBroken < TestingWrapper
  def setup
    config = new_test_config
    @node_info = ::RedisClient::Cluster::Node.load_info(config.per_node_key)
    @client = ::RedisClient::Cluster.new(config)
  end

  def teardown
    @client.close
  end

  def test_a_replica_is_down
    do_test_a_node_is_down('slave', number_of_keys: 10)
  end

  def test_a_primary_is_down
    do_test_a_node_is_down('master', number_of_keys: 10)
  end

  private

  def new_test_config
    ::RedisClient::ClusterConfig.new(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    )
  end

  def wait_for_replication
    @client.call('WAIT', TEST_REPLICA_SIZE, (TEST_TIMEOUT_SEC * 1000).to_i)
  end

  def do_test_a_node_is_down(role, number_of_keys:)
    number_of_keys.times { |i| @client.call('SET', "pre-#{i}", i) }
    number_of_keys.times { |i| @client.pipelined { |pi| pi.call('SET', "pre-pipelined-#{i}", i) } }
    wait_for_replication

    node_key = @node_info.select { |e| e[:role] == role }.sample.fetch(:node_key)
    node = @client.send(:find_node, node_key)
    refute_nil(node, node_key)
    node.call('SHUTDOWN')

    assert_equal('PONG', @client.call('PING'), 'Case: PING')
    do_assertions_without_pipelining(number_of_keys: number_of_keys)
    do_assertions_with_pipelining(number_of_keys: number_of_keys)
  end

  def do_assertions_without_pipelining(number_of_keys:)
    number_of_keys.times { |i| assert_equal(i.to_s, @client.call('GET', "pre-#{i}"), "Case: pre-#{i}: GET") }
    number_of_keys.times { |i| assert_equal('OK', @client.call('SET', "post-#{i}", i), "Case: post-#{i}: SET") }
    assert_equal('OK', wait_for_replication, 'Case: post: WAIT')
    number_of_keys.times { |i| assert_equal(i.to_s, @client.call('GET', "post-#{i}"), "Case: post-#{i}: GET") }
  end

  def do_assertions_with_pipelining(number_of_keys:)
    want = number_of_keys.times(&:to_s)
    got = @client.pipelined { |pi| number_of_keys.times { |i| pi.call('GET', "pre-pipelined-#{i}") } }
    assert_equal(want, got, 'Case: pre-pipelined: GET')

    want = Array.new(number_of_keys, 'OK')
    got = @client.pipelined { |pi| number_of_keys.times { |i| pi.call('SET', "post-pipelined-#{i}", i) } }
    assert_equal(want, got, 'Case: post-pipelined: SET')

    assert_equal('OK', wait_for_replication, 'Case: post-pipelined: WAIT')
    want = number_of_keys.times(&:to_s)
    got = @client.pipelined { |pi| number_of_keys.times { |i| pi.call('GET', "post-pipelined-#{i}") } }
    assert_equal(want, got, 'Case: post-pipelined: GET')
  end
end
