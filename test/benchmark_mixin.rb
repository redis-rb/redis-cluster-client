# frozen_string_literal: true

module BenchmarkMixin
  MIN_THRESHOLD = 0.95
  MAX_PIPELINE_SIZE = 100

  def setup
    @client = new_test_client
    @client.call('flushdb')
    wait_for_replication
  end

  def teardown
    @client&.call('flushdb')
    wait_for_replication
    @client&.close
  end

  def bench_single_echo
    assert_performance_linear(MIN_THRESHOLD) do |n|
      n.times do
        @client.call('echo', 'Hello world')
      end
    end
  end

  def bench_single_set
    assert_performance_linear(MIN_THRESHOLD) do |n|
      n.times do |i|
        @client.call('set', "key#{i}", i)
      end
    end
  end

  def bench_single_get
    assert_performance_linear(MIN_THRESHOLD) do |n|
      n.times do |i|
        @client.call('get', "key#{i}")
      end
    end
  end

  def bench_pipeline_echo
    assert_performance_linear(MIN_THRESHOLD) do |n|
      (1..n).each_slice(MAX_PIPELINE_SIZE) do |list|
        @client.pipelined do |pi|
          list.each do
            pi.call('echo', 'Hello world')
          end
        end
      end
    end
  end

  def bench_pipeline_set
    assert_performance_linear(MIN_THRESHOLD) do |n|
      (1..n).each_slice(MAX_PIPELINE_SIZE) do |list|
        @client.pipelined do |pi|
          list.each do |i|
            pi.call('set', "key#{i}", i)
          end
        end
      end
    end
  end

  def bench_pipeline_get
    assert_performance_linear(MIN_THRESHOLD) do |n|
      (1..n).each_slice(MAX_PIPELINE_SIZE) do |list|
        @client.pipelined do |pi|
          list.each do |i|
            pi.call('get', "key#{i}")
          end
        end
      end
    end
  end

  private

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    swap_timeout(@client, timeout: 0.1) do |client|
      client&.blocking_call(client_side_timeout, 'wait', TEST_REPLICA_SIZE, server_side_timeout)
    end
  end
end

module BenchmarkMixinForProxy
  def setup
    @client = new_test_client
    @cluster_client = new_cluster_client
    @cluster_client.call('flushdb')
    wait_for_replication
  end

  def teardown
    @cluster_client&.call('flushdb')
    wait_for_replication
    @cluster_client&.close
    @client&.close
  end

  private

  def new_cluster_client
    ::RedisClient.cluster(nodes: TEST_NODE_URIS, fixed_hostname: TEST_FIXED_HOSTNAME, **TEST_GENERIC_OPTIONS).new_client
  end

  def wait_for_replication
    client_side_timeout = TEST_TIMEOUT_SEC + 1.0
    server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
    swap_timeout(@cluster_client, timeout: 0.1) do |cluster_client|
      cluster_client&.blocking_call(client_side_timeout, 'wait', TEST_REPLICA_SIZE, server_side_timeout)
    end
  end
end
