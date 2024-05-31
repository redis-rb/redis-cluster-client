# frozen_string_literal: true

require 'testing_helper'
require 'securerandom'

class TestAgainstClusterBroken < TestingWrapper
  WAIT_SEC = 3

  def setup
    @client = ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
    @client.call('echo', 'init')
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

  def test_reloading_on_connection_error
    sacrifice = @controller.select_sacrifice_of_primary
    # Find a key which lives on the sacrifice node
    test_key = generate_key_for_node(sacrifice)
    @client.call('SET', test_key, 'foobar1')

    # Shut the node down.
    kill_a_node_and_wait_for_failover(sacrifice)

    # When we try and fetch the key, it'll attempt to connect to the broken node, and
    # thus trigger a reload of the cluster topology.
    assert_equal 'OK', @client.call('SET', test_key, 'foobar2')
  end

  def test_transaction_retry_on_connection_error
    sacrifice = @controller.select_sacrifice_of_primary
    # Find a key which lives on the sacrifice node
    test_key = generate_key_for_node(sacrifice)
    @client.call('SET', test_key, 'foobar1')

    call_count = 0
    # Begin a transaction, but shut the node down after the WATCH is issued
    res = @client.multi(watch: [test_key]) do |tx|
      kill_a_node_and_wait_for_failover(sacrifice) if call_count == 0
      call_count += 1
      tx.call('SET', test_key, 'foobar2')
    end

    # The transaction should have retried once and successfully completed
    # the second time.
    assert_equal ['OK'], res
    assert_equal 'foobar2', @client.call('GET', test_key)
    assert_equal 2, call_count
  end

  private

  def generate_key_for_node(conn)
    # Figure out a slot on the the sacrifice node, and a key in that slot.
    conn_id = conn.call('CLUSTER', 'MYID')
    conn_slots = conn.call('CLUSTER', 'SLOTS')
                     .select { |res| res[2][2] == conn_id }
                     .flat_map { |res| (res[0]..res[1]).to_a }
    loop do
      test_key = SecureRandom.hex
      return test_key if conn_slots.include?(conn.call('CLUSTER', 'KEYSLOT', test_key))
    end
  end

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    swap_timeout(@client, timeout: 0.1) do |client|
      client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
    end
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
      break if kill_attempts <= 0

      sacrifice.call('SHUTDOWN', 'NOSAVE')
    rescue ::RedisClient::CommandError => e
      raise unless e.message.include?('Errors trying to SHUTDOWN')
    rescue ::RedisClient::ConnectionError
      break
    ensure
      kill_attempts -= 1
      sleep WAIT_SEC
    end

    assert_raises(::RedisClient::ConnectionError) { sacrifice.call('PING') }
  end

  def kill_a_node_and_wait_for_failover(sacrifice)
    other_client = @controller.clients.reject { _1 == sacrifice }.first
    sacrifice_id = sacrifice.call('CLUSTER', 'MYID')
    kill_a_node(sacrifice, kill_attempts: 10)
    failover_checks = 0
    loop do
      raise 'Timed out waiting for failover in kill_a_node_and_wait_for_failover' if failover_checks > 30

      # Wait for the sacrifice node to not be a primary according to CLUSTER SLOTS.
      cluster_slots = other_client.call('CLUSTER', 'SLOTS')
      break unless cluster_slots.any? { _1[2][2] == sacrifice_id }

      sleep 1
      failover_checks += 1
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
