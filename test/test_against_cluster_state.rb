# frozen_string_literal: true

require 'testing_helper'

module TestAgainstClusterState
  SLOT_SIZE = 16_384
  PATTERN = ENV.fetch('TEST_CLASS_PATTERN', '')

  module Mixin
    def setup
      @controller = ClusterController.new(
        TEST_NODE_URIS,
        replica_size: TEST_REPLICA_SIZE,
        **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
      )
      @controller.rebuild
      @captured_commands = ::Middlewares::CommandCapture::CommandBuffer.new
      @redirect_count = ::Middlewares::RedirectCount::Counter.new
      @client = new_test_client
      @client.call('echo', 'init')
      @captured_commands.clear
      @redirect_count.clear
    end

    def teardown
      @controller&.close
      @client&.close
      print "#{@redirect_count.get}, "\
        "ClusterNodesCall: #{@captured_commands.count('cluster', 'nodes')} = "
    end

    def test_the_state_of_cluster_down
      @controller.down
      assert_raises(::RedisClient::CommandError) { @client.call('SET', 'key1', 1) }
      assert_equal('fail', fetch_cluster_info('cluster_state'))
    end

    def test_the_state_of_cluster_failover
      @controller.failover
      1000.times { |i| assert_equal('OK', @client.call('SET', "key#{i}", i)) }
      wait_for_replication
      1000.times { |i| assert_equal(i.to_s, @client.call('GET', "key#{i}")) }
      assert_equal('ok', fetch_cluster_info('cluster_state'))
      refute(@redirect_count.zero?, @redirect_count.get)
    end

    def test_the_state_of_cluster_resharding
      resharded_keys = nil
      do_resharding_test do |keys|
        resharded_keys = keys
        keys.each do |key|
          want = key
          got = @client.call('GET', key)
          assert_equal(want, got, "Case: GET: #{key}")
        end
      end

      refute(@redirect_count.zero?, @redirect_count.get)
      resharded_keys.each do |key|
        want = key
        got = @client.call('GET', key)
        assert_equal(want, got, "Case: GET: #{key}")
      end
    end

    def test_the_state_of_cluster_resharding_with_pipelining
      resharded_keys = nil
      do_resharding_test do |keys|
        resharded_keys = keys
        values = @client.pipelined do |pipeline|
          keys.each { |key| pipeline.call('GET', key) }
        end

        keys.each_with_index do |key, i|
          want = key
          got = values[i]
          assert_equal(want, got, "Case: GET: #{key}")
        end
      end

      values = @client.pipelined do |pipeline|
        resharded_keys.each { |key| pipeline.call('GET', key) }
      end

      resharded_keys.each_with_index do |key, i|
        want = key
        got = values[i]
        assert_equal(want, got, "Case: GET: #{key}")
      end

      # Since redirections are handled by #call_pipelined_aware_of_redirection,
      # we can't trace them in pipelining processes.
      #
      # refute(@redirect_count.zero?, @redirect_count.get)
    end

    def test_the_state_of_cluster_resharding_with_transaction
      call_cnt = 0
      resharded_keys = nil

      do_resharding_test do |keys|
        resharded_keys = keys
        @client.multi do |tx|
          call_cnt += 1
          keys.each do |key|
            tx.call('SET', key, '0')
            tx.call('INCR', key)
          end
        end

        keys.each do |key|
          want = '1'
          got = @client.call('GET', key)
          assert_equal(want, got, "Case: GET: #{key}")
        end
      end

      refute(@redirect_count.zero?, @redirect_count.get)

      @client.multi do |tx|
        call_cnt += 1
        resharded_keys.each do |key|
          tx.call('SET', key, '2')
          tx.call('INCR', key)
        end
      end

      resharded_keys.each do |key|
        want = '3'
        got = @client.call('GET', key)
        assert_equal(want, got, "Case: GET: #{key}")
      end

      assert_equal(2, call_cnt)
    end

    def test_the_state_of_cluster_resharding_with_transaction_and_watch
      call_cnt = 0
      resharded_keys = nil

      do_resharding_test do |keys|
        resharded_keys = keys
        @client.multi(watch: keys) do |tx|
          call_cnt += 1
          keys.each do |key|
            tx.call('SET', key, '0')
            tx.call('INCR', key)
          end
        end

        keys.each do |key|
          want = '1'
          got = @client.call('GET', key)
          assert_equal(want, got, "Case: GET: #{key}")
        end
      end

      refute(@redirect_count.zero?, @redirect_count.get)

      @client.multi(watch: resharded_keys) do |tx|
        call_cnt += 1
        resharded_keys.each do |key|
          tx.call('SET', key, '2')
          tx.call('INCR', key)
        end
      end

      resharded_keys.each do |key|
        want = '3'
        got = @client.call('GET', key)
        assert_equal(want, got, "Case: GET: #{key}")
      end

      assert_equal(2, call_cnt)
    end

    def test_the_state_of_cluster_resharding_with_reexecuted_watch
      client2 = new_test_client(middlewares: nil)
      call_cnt = 0

      @client.call('SET', 'watch_key', 'original_value')
      @client.multi(watch: %w[watch_key]) do |tx|
        # Use client2 to change the value of watch_key, which would cause this transaction to fail
        if call_cnt == 0
          client2.call('SET', 'watch_key', 'client2_value')

          # Now perform (and _finish_) a reshard, which should make this transaction receive a MOVED
          # redirection when it goes to commit. That should result in the entire block being retried
          slot = ::RedisClient::Cluster::KeySlotConverter.convert('watch_key')
          src, dest = @controller.select_resharding_target(slot)
          @controller.start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
          @controller.finish_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
        end
        call_cnt += 1

        tx.call('SET', 'watch_key', "@client_value_#{call_cnt}")
      end
      # It should have retried the entire transaction block.
      assert_equal(2, call_cnt)
      # The second call succeeded
      assert_equal('@client_value_2', @client.call('GET', 'watch_key'))
      refute(@redirect_count.zero?, @redirect_count.get)
    ensure
      client2&.close
    end

    def test_the_state_of_cluster_resharding_with_pipelining_on_new_connection
      # This test is excercising a very delicate race condition; i think the use of @client to set
      # the keys in do_resharding_test is actually causing the race condition not to happen, so this
      # test is actually performing the resharding on its own.
      key_count = 10
      key_count.times do |i|
        key = "key#{i}"
        slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
        src, dest = @controller.select_resharding_target(slot)
        @controller.start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
        @controller.finish_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      end

      res = @client.pipelined do |p|
        key_count.times do |i|
          p.call_v(['SET', "key#{i}", "value#{i}"])
        end
      end

      key_count.times do |i|
        assert_equal('OK', res[i])
        assert_equal("value#{i}", @client.call_v(['GET', "key#{i}"]))
      end
    end

    private

    def wait_for_replication
      client_side_timeout = TEST_TIMEOUT_SEC + 1.0
      server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
      swap_timeout(@client, timeout: 0.1) do |client|
        client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
      rescue RedisClient::Cluster::ErrorCollection => e
        raise unless e.errors.values.all? { |err| err.message.start_with?('ERR WAIT cannot be used with replica instances') }
      end
    end

    def fetch_cluster_info(key)
      @client.call('CLUSTER', 'INFO').split("\r\n").to_h { |v| v.split(':') }.fetch(key)
    end

    def do_resharding_test(number_of_keys: 1000)
      @client.pipelined { |pipeline| number_of_keys.times { |i| pipeline.call('SET', "key#{i}", "key#{i}") } }
      wait_for_replication
      count, slot = @client.pipelined { |pi| SLOT_SIZE.times { |i| pi.call('CLUSTER', 'COUNTKEYSINSLOT', i) } }
                           .each_with_index.max_by { |c, _| c }
      refute_equal(0, count)
      keys = @client.call('CLUSTER', 'GETKEYSINSLOT', slot, count)
      refute_empty(keys)
      src, dest = @controller.select_resharding_target(slot)
      @controller.start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      yield(keys)
      @controller.finish_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
    end
  end

  if PATTERN == 'PrimaryOnly' || PATTERN.empty?
    class PrimaryOnly < TestingWrapper
      include Mixin

      private

      def new_test_client(
        custom: { captured_commands: @captured_commands, redirect_count: @redirect_count },
        middlewares: [::Middlewares::CommandCapture, ::Middlewares::RedirectCount],
        **opts
      )
        ::RedisClient.cluster(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: middlewares,
          custom: custom,
          **TEST_GENERIC_OPTIONS,
          **opts
        ).new_client
      end
    end
  end

  if PATTERN == 'Pooled' || PATTERN.empty?
    class Pooled < TestingWrapper
      include Mixin

      private

      def new_test_client(
        custom: { captured_commands: @captured_commands, redirect_count: @redirect_count },
        middlewares: [::Middlewares::CommandCapture, ::Middlewares::RedirectCount],
        **opts
      )
        ::RedisClient.cluster(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: middlewares,
          custom: custom,
          **TEST_GENERIC_OPTIONS,
          **opts
        ).new_pool(timeout: TEST_TIMEOUT_SEC, size: 2)
      end
    end
  end

  if PATTERN == 'ScaleReadRandom' || PATTERN.empty?
    class ScaleReadRandom < TestingWrapper
      include Mixin

      private

      def new_test_client(
        custom: { captured_commands: @captured_commands, redirect_count: @redirect_count },
        middlewares: [::Middlewares::CommandCapture, ::Middlewares::RedirectCount],
        **opts
      )
        ::RedisClient.cluster(
          nodes: TEST_NODE_URIS,
          replica: true,
          replica_affinity: :random,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: middlewares,
          custom: custom,
          **TEST_GENERIC_OPTIONS,
          **opts
        ).new_client
      end
    end
  end

  if PATTERN == 'ScaleReadRandomWithPrimary' || PATTERN.empty?
    class ScaleReadRandomWithPrimary < TestingWrapper
      include Mixin

      private

      def new_test_client(
        custom: { captured_commands: @captured_commands, redirect_count: @redirect_count },
        middlewares: [::Middlewares::CommandCapture, ::Middlewares::RedirectCount],
        **opts
      )
        ::RedisClient.cluster(
          nodes: TEST_NODE_URIS,
          replica: true,
          replica_affinity: :random_with_primary,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: middlewares,
          custom: custom,
          **TEST_GENERIC_OPTIONS,
          **opts
        ).new_client
      end
    end
  end

  if PATTERN == 'ScaleReadLatency' || PATTERN.empty?
    class ScaleReadLatency < TestingWrapper
      include Mixin

      private

      def new_test_client(
        custom: { captured_commands: @captured_commands, redirect_count: @redirect_count },
        middlewares: [::Middlewares::CommandCapture, ::Middlewares::RedirectCount],
        **opts
      )
        ::RedisClient.cluster(
          nodes: TEST_NODE_URIS,
          replica: true,
          replica_affinity: :latency,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: middlewares,
          custom: custom,
          **TEST_GENERIC_OPTIONS,
          **opts
        ).new_client
      end
    end
  end
end
