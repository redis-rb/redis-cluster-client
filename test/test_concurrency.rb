# frozen_string_literal: true

require 'testing_helper'

class TestConcurrency < TestingWrapper
  MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))
  ATTEMPTS = 200
  WANT = '1'

  def setup
    @client = new_test_client
    MAX_THREADS.times { |i| @client.call('SET', "key#{i}", WANT) }
  end

  def teardown
    @client&.close
  end

  def test_forking
    pids = Array.new(MAX_THREADS) do
      Process.fork do
        ATTEMPTS.times { MAX_THREADS.times { |i| @client.call('INCR', "key#{i}") } }
        sleep 0.1
        ATTEMPTS.times { MAX_THREADS.times { |i| @client.call('DECR', "key#{i}") } }
      end
    end

    pids.each do |pid|
      _, status = Process.waitpid2(pid)
      assert_predicate(status, :success?)
    end

    MAX_THREADS.times { |i| assert_equal(WANT, @client.call('GET', "key#{i}")) }
  end

  def test_forking_with_pipelining
    pids = Array.new(MAX_THREADS) do
      Process.fork do
        @client.pipelined { |pi| ATTEMPTS.times { MAX_THREADS.times { |i| pi.call('INCR', "key#{i}") } } }
        sleep 0.1
        @client.pipelined { |pi| ATTEMPTS.times { MAX_THREADS.times { |i| pi.call('DECR', "key#{i}") } } }
      end
    end

    pids.each do |pid|
      _, status = Process.waitpid2(pid)
      assert_predicate(status, :success?)
    end

    MAX_THREADS.times { |i| assert_equal(WANT, @client.call('GET', "key#{i}")) }
  end

  def test_threading
    threads = Array.new(MAX_THREADS) do
      Thread.new do
        ATTEMPTS.times { MAX_THREADS.times { |i| @client.call('INCR', "key#{i}") } }
        ATTEMPTS.times { MAX_THREADS.times { |i| @client.call('DECR', "key#{i}") } }
      rescue StandardError => e
        Thread.current.thread_variable_set(:error, e)
      end
    end

    threads.each(&:join)
    threads.each { |t| assert_nil(t.thread_variable_get(:error)) }
    MAX_THREADS.times { |i| assert_equal(WANT, @client.call('GET', "key#{i}")) }
  end

  def test_threading_with_pipelining
    threads = Array.new(MAX_THREADS) do
      Thread.new do
        @client.pipelined { |pi| ATTEMPTS.times { MAX_THREADS.times { |i| pi.call('INCR', "key#{i}") } } }
        @client.pipelined { |pi| ATTEMPTS.times { MAX_THREADS.times { |i| pi.call('DECR', "key#{i}") } } }
      rescue StandardError => e
        Thread.current.thread_variable_set(:error, e)
      end
    end

    threads.each(&:join)
    threads.each { |t| assert_nil(t.thread_variable_get(:error)) }
    MAX_THREADS.times { |i| assert_equal(WANT, @client.call('GET', "key#{i}")) }
  end

  private

  def new_test_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_pool(
      timeout: TEST_TIMEOUT_SEC,
      size: MAX_THREADS
    )
  end
end
