# frozen_string_literal: true

require 'json'
require 'testing_helper'

class TestAgainstClusterBroken < TestingWrapper
  WAIT_SEC = 1
  MAX_ATTEMPTS = 60
  NUMBER_OF_KEYS = 10

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
    @controller = ClusterController.new(
      TEST_NODE_URIS,
      replica_size: TEST_REPLICA_SIZE,
      **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
    )
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

  def test_client_patience
    prepare_test_data(number_of_keys: NUMBER_OF_KEYS)

    # a replica
    kill_a_node(@controller.select_sacrifice_of_replica)
    wait_for_cluster_to_be_ready(wait_attempts: MAX_ATTEMPTS)
    do_assertions(number_of_keys: NUMBER_OF_KEYS)
    refute(@captured_commands.count('cluster', 'nodes').zero?, @captured_commands.to_a.map(&:command))

    # a primary
    kill_a_node(@controller.select_sacrifice_of_primary)
    wait_for_cluster_to_be_ready(wait_attempts: MAX_ATTEMPTS)
    do_assertions(number_of_keys: NUMBER_OF_KEYS)
    refute(@captured_commands.count('cluster', 'nodes').zero?, @captured_commands.to_a.map(&:command))

    # recovery
    revive_dead_nodes
    wait_for_cluster_to_be_ready(wait_attempts: MAX_ATTEMPTS)
    do_assertions(number_of_keys: NUMBER_OF_KEYS)
    refute(@captured_commands.count('cluster', 'nodes').zero?, @captured_commands.to_a.map(&:command))
  end

  private

  def prepare_test_data(number_of_keys:)
    number_of_keys.times { |i| @client.call('SET', "pre-#{i}", i) }
    number_of_keys.times { |i| @client.pipelined { |pi| pi.call('SET', "pre-pipelined-#{i}", i) } }
    wait_for_replication
  end

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    swap_timeout(@client, timeout: 0.1) do |client|
      client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
    end
  end

  def wait_for_cluster_to_be_ready(wait_attempts:)
    loop do
      raise MaxRetryExceeded if wait_attempts <= 0

      wait_attempts -= 1
      break if @client.call('PING') == 'PONG'
    rescue ::RedisClient::Cluster::NodeMightBeDown
      @cluster_down_error_count += 1
    ensure
      sleep WAIT_SEC
    end
  end

  def kill_a_node(sacrifice)
    refute_nil(sacrifice, "#{sacrifice.config.host}:#{sacrifice.config.port}")

    `docker compose ps --format json`.lines.map { |line| JSON.parse(line) }.each do |service|
      published_ports = service.fetch('Publishers').map { |e| e.fetch('PublishedPort') }.uniq
      next unless published_ports.include?(sacrifice.config.port)

      service_name = service.fetch('Service')
      system("docker compose --progress quiet pause #{service_name}", exception: true)
      break
    end

    assert_raises(::RedisClient::ConnectionError) { sacrifice.call('PING') }
  end

  def revive_dead_nodes
    `docker compose ps --format json --status paused`.lines.map { |line| JSON.parse(line) }.each do |service|
      service_name = service.fetch('Service')
      system("docker compose --progress quiet unpause #{service_name}", exception: true)
    end
  end

  def do_assertions(number_of_keys:)
    number_of_keys.times { |i| assert_equal(i.to_s, @client.call('GET', "pre-#{i}"), "Case: pre-#{i}: GET") }
    number_of_keys.times { |i| assert_equal('OK', @client.call('SET', "post-#{i}", i), "Case: post-#{i}: SET") }

    want = Array.new(number_of_keys, &:to_s)
    got = @client.pipelined { |pi| number_of_keys.times { |i| pi.call('GET', "pre-pipelined-#{i}") } }
    assert_equal(want, got, 'Case: pre-pipelined: GET')

    want = Array.new(number_of_keys, 'OK')
    got = @client.pipelined { |pi| number_of_keys.times { |i| pi.call('SET', "post-pipelined-#{i}", i) } }
    assert_equal(want, got, 'Case: post-pipelined: SET')
  end
end
