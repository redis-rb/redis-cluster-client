# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterBroken < TestingWrapper
  WAIT_SEC = 3

  def setup
    @client = ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
    @controller = ClusterController.new(
      TEST_NODE_URIS,
      replica_size: TEST_REPLICA_SIZE,
      **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
    )
  end

  def teardown
    @client&.close
    @controller&.close
  end

  def test_a_replica_is_down
    sacrifice = @controller.select_sacrifice_of_replica
    do_test_a_node_is_down(sacrifice, number_of_keys: 10)
  end

  def test_a_primary_is_down
    sacrifice = @controller.select_sacrifice_of_primary
    do_test_a_node_is_down(sacrifice, number_of_keys: 10)
  end

  def test_transaction_aborts_when_node_down
    # Need a new client for this test which isn't used in the helpers
    test_client = ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
    key = 'key1'
    slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
    sacrifice = @controller.select_sacrifice_by_slot(slot)

    test_client.call_v(['WATCH', key])

    kill_a_node(sacrifice, kill_attempts: 10)
    wait_for_cluster_to_be_ready(wait_attempts: 10)

    # We don't know that it was closed yet
    assert client_in_transaction?(test_client)
    # It was closed
    assert_raises(RedisClient::ConnectionError) { test_client.call_v(['GET', key]) }
    # We're not in a transaction anymore
    refute client_in_transaction?(test_client)
    # So doing some other access will be fine now.
    assert_nil(test_client.call_v(%w[GET other_key]))
  end

  private

  def client_in_transaction?(client)
    !!client.instance_variable_get(:@transaction)
  end

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    @client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
  end

  def do_test_a_node_is_down(sacrifice, number_of_keys:)
    prepare_test_data(number_of_keys: number_of_keys)

    kill_a_node(sacrifice, kill_attempts: 10)
    wait_for_cluster_to_be_ready(wait_attempts: 10)

    assert_equal('PONG', @client.call('PING'), 'Case: PING')
    do_assertions_without_pipelining(number_of_keys: number_of_keys)
    do_assertions_with_pipelining(number_of_keys: number_of_keys)
  end

  def prepare_test_data(number_of_keys:)
    number_of_keys.times { |i| @client.call('SET', "pre-#{i}", i) }
    number_of_keys.times { |i| @client.pipelined { |pi| pi.call('SET', "pre-pipelined-#{i}", i) } }
    wait_for_replication
  end

  def kill_a_node(sacrifice, kill_attempts:)
    refute_nil(sacrifice, "#{sacrifice.config.host}:#{sacrifice.config.port}")

    loop do
      assert_operator(kill_attempts, :>=, 0)

      sacrifice.call('SHUTDOWN', 'NOSAVE')
    rescue ::RedisClient::CommandError => e
      raise unless e.message.include?('Errors trying to SHUTDOWN')
    rescue ::RedisClient::ConnectionError
      break
    ensure
      kill_attempts -= 1
      sleep WAIT_SEC
    end
  end

  def wait_for_cluster_to_be_ready(wait_attempts:)
    loop do
      break if wait_attempts <= 0 || @client.call('PING') == 'PONG'
    rescue ::RedisClient::Cluster::NodeMightBeDown
      # ignore
    ensure
      wait_attempts -= 1
      sleep WAIT_SEC
    end
  end

  def do_assertions_without_pipelining(number_of_keys:)
    number_of_keys.times { |i| assert_equal(i.to_s, @client.call('GET', "pre-#{i}"), "Case: pre-#{i}: GET") }
    number_of_keys.times { |i| assert_equal('OK', @client.call('SET', "post-#{i}", i), "Case: post-#{i}: SET") }
  end

  def do_assertions_with_pipelining(number_of_keys:)
    want = Array.new(number_of_keys, &:to_s)
    got = @client.pipelined { |pi| number_of_keys.times { |i| pi.call('GET', "pre-pipelined-#{i}") } }
    assert_equal(want, got, 'Case: pre-pipelined: GET')

    want = Array.new(number_of_keys, 'OK')
    got = @client.pipelined { |pi| number_of_keys.times { |i| pi.call('SET', "post-pipelined-#{i}", i) } }
    assert_equal(want, got, 'Case: post-pipelined: SET')
  end
end
