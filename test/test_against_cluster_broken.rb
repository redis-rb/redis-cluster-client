# frozen_string_literal: true

require 'logger'
require 'json'
require 'testing_helper'
require 'securerandom'

class TestAgainstClusterBroken < TestingWrapper
  WAIT_SEC = 0.1
  MAX_ATTEMPTS = 100
  NUMBER_OF_KEYS = 16_000
  MAX_PIPELINE_SIZE = 40
  HASH_TAG_GRAIN = 5
  SLICED_NUMBERS = (0...NUMBER_OF_KEYS).each_slice(MAX_PIPELINE_SIZE).freeze

  def setup
    @controller = build_controller
    @captured_commands = ::Middlewares::CommandCapture::CommandBuffer.new
    @redirect_count = ::Middlewares::RedirectCount::Counter.new
    @cluster_down_error_count = 0
    @logger = Logger.new($stdout)
    print "\n"
    @logger.info('setup: test')
    prepare_test_data
    @clients = Array.new(3) { build_client.tap { |c| c.call('echo', 'init') } }
  end

  def teardown
    @logger.info('teardown: test')
    revive_dead_nodes
    @clients.each(&:close)
    @controller&.close
  end

  def test_client_patience
    do_manual_failover
    wait_for_cluster_to_be_ready
    do_assertions(offset: 0)

    # a replica
    sacrifice_replica = @controller.select_sacrifice_of_replica
    kill_a_node(sacrifice_replica)
    wait_for_cluster_to_be_ready(ignore: [sacrifice_replica])
    do_assertions(offset: 1)

    # a primary
    sacrifice_primary = @controller.select_sacrifice_of_primary
    kill_a_node(sacrifice_primary)
    wait_for_cluster_to_be_ready(ignore: [sacrifice_replica, sacrifice_primary])
    do_assertions(offset: 2)

    # recovery
    revive_dead_nodes
    wait_for_cluster_to_be_ready
    do_assertions(offset: 3)
  end

  def test_reloading_on_connection_error
    sacrifice = @controller.select_sacrifice_of_primary
    # Find a key which lives on the sacrifice node
    test_key = generate_key_for_node(sacrifice)
    @clients[0].call('SET', test_key, 'foobar1')

    # Shut the node down.
    kill_a_node_and_wait_for_failover(sacrifice)

    # When we try and fetch the key, it'll attempt to connect to the broken node, and
    # thus trigger a reload of the cluster topology.
    assert_equal 'OK', @clients[0].call('SET', test_key, 'foobar2')
  end

  def test_transaction_retry_on_connection_error
    sacrifice = @controller.select_sacrifice_of_primary
    # Find a key which lives on the sacrifice node
    test_key = generate_key_for_node(sacrifice)
    @clients[0].call('SET', test_key, 'foobar1')

    call_count = 0
    # Begin a transaction, but shut the node down after the WATCH is issued
    res = @clients[0].multi(watch: [test_key]) do |tx|
      kill_a_node_and_wait_for_failover(sacrifice) if call_count == 0
      call_count += 1
      tx.call('SET', test_key, 'foobar2')
    end

    # The transaction should have retried once and successfully completed
    # the second time.
    assert_equal ['OK'], res
    assert_equal 'foobar2', @clients[0].call('GET', test_key)
    assert_equal 2, call_count
  end

  private

  def prepare_test_data
    client = build_client(custom: nil, middlewares: nil)
    client.call('FLUSHDB')

    SLICED_NUMBERS.each do |numbers|
      client.pipelined do |pi|
        numbers.each do |i|
          pi.call('SET', "single:#{i}", i)
          pi.call('SET', "pipeline:#{i}", i)
          pi.call('SET', "{group#{i / HASH_TAG_GRAIN}}:transaction:#{i}", i)
        end
      end
    end

    wait_for_replication(client)
    client.close
  end

  def do_assertions(offset:)
    @captured_commands.clear
    @redirect_count.clear
    @cluster_down_error_count = 0

    log_info('assertions') do
      log_info('assertions: single') do
        NUMBER_OF_KEYS.times do |i|
          want = (i + offset).to_s
          got = retryable { @clients[0].call_once('GET', "single:#{i}") }
          assert_equal(want, got, 'Case: Single GET')

          want = 'OK'
          got = retryable { @clients[0].call_once('SET', "single:#{i}", i + offset + 1) }
          assert_equal(want, got, 'Case: Single SET')
        end
      end

      log_info('assertions: pipeline') do
        SLICED_NUMBERS.each do |numbers|
          want = numbers.map { |i| (i + offset).to_s }
          got = retryable do
            @clients[1].pipelined do |pi|
              numbers.each { |i| pi.call('GET', "pipeline:#{i}") }
            end
          end
          assert_equal(want, got, 'Case: Pipeline GET')

          want = numbers.map { 'OK' }
          got = retryable do
            @clients[1].pipelined do |pi|
              numbers.each { |i| pi.call('SET', "pipeline:#{i}", i + offset + 1) }
            end
          end
          assert_equal(want, got, 'Case: Pipeline SET')
        end
      end

      log_info('assertions: transaction') do
        NUMBER_OF_KEYS.times.group_by { |i| i / HASH_TAG_GRAIN }.each do |group, numbers|
          want = numbers.map { 'OK' }
          keys = numbers.map { |i| "{group#{group}}:transaction:#{i}" }
          got = retryable do
            @clients[2].multi(watch: group.odd? ? nil : keys) do |tx|
              keys.each_with_index { |key, i| tx.call('SET', key, numbers[i] + offset + 1) }
            end
          end
          assert_equal(want, got, 'Case: Transaction: SET')
        end
      end

      log_metrics
    end
  end

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

  def wait_for_replication(client)
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    swap_timeout(client, timeout: 0.1) do |c|
      c.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
    end
  end

  def wait_for_cluster_to_be_ready(ignore: [])
    log_info('wait for the cluster to be stable') do
      @controller.wait_for_cluster_to_be_ready(skip_clients: ignore)
    end
  end

  def do_manual_failover
    log_info('failover') do
      @controller.failover
    end
  end

  def kill_a_node(sacrifice)
    log_info("kill #{sacrifice.config.host}:#{sacrifice.config.port}") do
      refute_nil(sacrifice, "#{sacrifice.config.host}:#{sacrifice.config.port}")

      `docker compose ps --format json`.lines.map { |line| JSON.parse(line) }.each do |service|
        published_ports = service.fetch('Publishers').map { |e| e.fetch('PublishedPort') }.uniq
        next unless published_ports.include?(sacrifice.config.port)

        service_name = service.fetch('Service')
        system("docker compose --progress quiet pause #{service_name}", exception: true)
        break
      end

      assert_raises(::RedisClient::ConnectionError) { sacrifice.call_once('PING') }
    end
  end

  def revive_dead_nodes
    log_info('revive dead nodes') do
      `docker compose ps --format json --status paused`.lines.map { |line| JSON.parse(line) }.each do |service|
        service_name = service.fetch('Service')
        system("docker compose --progress quiet unpause #{service_name}", exception: true)
      end
    end
  end

  def log_info(message)
    @logger.info("start: #{message}")
    yield
    @logger.info(" done: #{message}")
  end

  def log_metrics
    print "#{@redirect_count.get}, "\
      "ClusterNodesCall: #{@captured_commands.count('cluster', 'nodes')}, "\
      "ClusterDownError: #{@cluster_down_error_count}\n"
  end

  def retryable(attempts: MAX_ATTEMPTS, wait_sec: WAIT_SEC)
    loop do
      raise MaxRetryExceeded if attempts <= 0

      attempts -= 1
      break yield
    rescue ::RedisClient::ConnectionError, ::RedisClient::Cluster::NodeMightBeDown
      @cluster_down_error_count += 1
      sleep wait_sec
    rescue ::RedisClient::CommandError => e
      raise unless e.message.start_with?('CLUSTERDOWN')

      @cluster_down_error_count += 1
      sleep wait_sec
    rescue ::RedisClient::Cluster::ErrorCollection => e
      raise unless e.errors.values.all? do |err|
        err.message.start_with?('CLUSTERDOWN') || err.is_a?(::RedisClient::ConnectionError)
      end

      @cluster_down_error_count += 1
      sleep wait_sec
    end
  end

  def kill_a_node_and_wait_for_failover(sacrifice)
    other_client = @controller.clients.reject { _1 == sacrifice }.first
    sacrifice_id = sacrifice.call('CLUSTER', 'MYID')
    kill_a_node(sacrifice)
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

  def build_client(
    custom: { captured_commands: @captured_commands, redirect_count: @redirect_count },
    middlewares: [::Middlewares::CommandCapture, ::Middlewares::RedirectCount],
    **opts
  )
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      custom: custom,
      middlewares: middlewares,
      **TEST_GENERIC_OPTIONS.merge(timeout: 0.1),
      **opts
    ).new_client
  end

  def build_controller
    ClusterController.new(
      TEST_NODE_URIS,
      replica_size: TEST_REPLICA_SIZE,
      **TEST_GENERIC_OPTIONS.merge(timeout: 0.1)
    )
  end
end
