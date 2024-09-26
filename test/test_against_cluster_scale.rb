# frozen_string_literal: true

require 'testing_helper'

module TestAgainstClusterScale
  PATTERN = ENV.fetch('TEST_CLASS_PATTERN', '')

  module Mixin
    WAIT_SEC = 1
    MAX_ATTEMPTS = 20
    NUMBER_OF_KEYS = 20_000
    MAX_PIPELINE_SIZE = 40
    HASH_TAG_GRAIN = 5
    SLICED_NUMBERS = (0...NUMBER_OF_KEYS).each_slice(MAX_PIPELINE_SIZE).freeze

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
      SLICED_NUMBERS.each do |numbers|
        @client.pipelined do |pi|
          numbers.each do |i|
            pi.call('SET', "key#{i}", i)
            pi.call('SET', "{group#{i / HASH_TAG_GRAIN}}:key#{i}", i)
          end
        end
      end

      wait_for_replication

      primary_url, replica_url = build_additional_node_urls
      @controller = build_cluster_controller(TEST_NODE_URIS, shard_size: 3)
      @controller.scale_out(primary_url: primary_url, replica_url: replica_url)

      do_test_after_scaled_out

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

      do_test_after_scaled_in

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

    def retryable(attempts:)
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

  if PATTERN == 'Single' || PATTERN.empty?
    class Single < TestingWrapper
      include Mixin

      def self.test_order
        :alpha
      end

      def do_test_after_scaled_out
        NUMBER_OF_KEYS.times do |i|
          assert_equal(i.to_s, @client.call('GET', "key#{i}"), "Case: key#{i}")
        end
      end

      def do_test_after_scaled_in
        NUMBER_OF_KEYS.times do |i|
          got = retryable(attempts: MAX_ATTEMPTS) { @client.call('GET', "key#{i}") }
          assert_equal(i.to_s, got, "Case: key#{i}")
        end
      end
    end
  end

  if PATTERN == 'Pipeline' || PATTERN.empty?
    class Pipeline < TestingWrapper
      include Mixin

      def self.test_order
        :alpha
      end

      def do_test_after_scaled_out
        SLICED_NUMBERS.each do |numbers|
          got = @client.pipelined do |pi|
            numbers.each { |i| pi.call('GET', "key#{i}") }
          end

          assert_equal(numbers.map(&:to_s), got, 'Case: GET')
        end
      end

      def do_test_after_scaled_in
        SLICED_NUMBERS.each do |numbers|
          got = retryable(attempts: MAX_ATTEMPTS) do
            @client.pipelined do |pi|
              numbers.each { |i| pi.call('GET', "key#{i}") }
            end
          end

          assert_equal(numbers.map(&:to_s), got, 'Case: GET')
        end
      end
    end
  end

  if PATTERN == 'Transaction' || PATTERN.empty?
    class Transaction < TestingWrapper
      include Mixin

      def self.test_order
        :alpha
      end

      def do_test_after_scaled_out
        NUMBER_OF_KEYS.times.group_by { |i| i / HASH_TAG_GRAIN }.each do |group, numbers|
          keys = numbers.map { |i| "{group#{group}}:key#{i}" }
          got = @client.multi(watch: group.odd? ? nil : keys) do |tx|
            keys.each { |key| tx.call('INCR', key) }
          end

          want = numbers.map { |i| (i + 1) }
          assert_equal(want, got, 'Case: INCR')
        end
      end

      def do_test_after_scaled_in
        NUMBER_OF_KEYS.times.group_by { |i| i / HASH_TAG_GRAIN }.each do |group, numbers|
          keys = numbers.map { |i| "{group#{group}}:key#{i}" }
          got = retryable(attempts: MAX_ATTEMPTS) do
            @client.multi(watch: group.odd? ? nil : keys) do |tx|
              keys.each { |key| tx.call('INCR', key) }
            end
          end

          want = numbers.map { |i| (i + 2) }
          assert_equal(want, got, 'Case: INCR')
        end
      end
    end
  end

  if PATTERN == 'PubSub' || PATTERN.empty?
    class PubSub < TestingWrapper
      include Mixin

      def self.test_order
        :alpha
      end

      def do_test_after_scaled_out
        1000.times do |i|
          pubsub = @client.pubsub
          pubsub.call('SSUBSCRIBE', "chan#{i}")
          event = pubsub.next_event(0.01)
          event = pubsub.next_event(0.01) if event.nil? # state changed
          assert_equal(['ssubscribe', "chan#{i}", 1], event)
          assert_nil(pubsub.next_event(0.01))
        ensure
          pubsub&.close
        end
      end

      alias do_test_after_scaled_in do_test_after_scaled_out
    end
  end
end
