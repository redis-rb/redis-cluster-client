# frozen_string_literal: true

require 'logger'
require 'json'
require 'testing_helper'

class TestAgainstClusterBroken < TestingWrapper
  WAIT_SEC = 0.1
  MAX_ATTEMPTS = 100
  NUMBER_OF_KEYS = 1600
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
    @clients = Array.new(3) do
      build_client.tap { |c| c.call('echo', 'init') }
    end
  end

  def teardown
    @logger.info('teardown: test')
    revive_dead_nodes
    @clients.each(&:close)
    @controller&.close
    refute(@captured_commands.count('cluster', 'nodes').zero?, @captured_commands.to_a.map(&:command))
    print "#{@redirect_count.get}, "\
      "ClusterNodesCall: #{@captured_commands.count('cluster', 'nodes')}, "\
      "ClusterDownError: #{@cluster_down_error_count} = "
  end

  def test_client_patience
    # a replica
    sacrifice_replica = @controller.select_sacrifice_of_replica
    kill_a_node(sacrifice_replica)
    wait_for_cluster_to_be_ready(ignore: [sacrifice_replica])
    do_assertions(offset: 0)

    # a primary
    sacrifice_primary = @controller.select_sacrifice_of_primary
    kill_a_node(sacrifice_primary)
    wait_for_cluster_to_be_ready(ignore: [sacrifice_replica, sacrifice_primary])
    do_assertions(offset: 1)

    # recovery
    revive_dead_nodes
    wait_for_cluster_to_be_ready
    do_assertions(offset: 2)
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
    log_info('assertions') do
      log_info('assertions: single') do
        (NUMBER_OF_KEYS / MAX_PIPELINE_SIZE).times do |i|
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
        (NUMBER_OF_KEYS / (MAX_PIPELINE_SIZE / HASH_TAG_GRAIN)).times.group_by { |i| i / HASH_TAG_GRAIN }.each do |group, numbers|
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

  def retryable(attempts: MAX_ATTEMPTS, wait_sec: WAIT_SEC)
    loop do
      raise MaxRetryExceeded if attempts <= 0

      attempts -= 1
      break yield
    rescue ::RedisClient::ConnectionError, ::RedisClient::Cluster::NodeMightBeDown
      @cluster_down_error_count += 1
    rescue ::RedisClient::CommandError => e
      raise unless e.message.start_with?('CLUSTERDOWN')

      @cluster_down_error_count += 1
    rescue ::RedisClient::Cluster::ErrorCollection => e
      raise unless e.errors.values.all? do |err|
        err.message.start_with?('CLUSTERDOWN') || err.is_a?(::RedisClient::ConnectionError)
      end

      @cluster_down_error_count += 1
    ensure
      sleep wait_sec
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
