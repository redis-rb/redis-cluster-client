# frozen_string_literal: true

require 'testing_helper'

class TestConcurrency < TestingWrapper
  MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))
  ATTEMPTS = 1000
  WANT = '1'

  def setup
    @client = new_test_client
    MAX_THREADS.times { |i| @client.call('SET', "key#{i}", WANT) }
  end

  def teardown
    @client&.close
  end

  def test_forking
    skip("fork is not available on #{RUBY_ENGINE}") if %w[jruby truffleruby].include?(RUBY_ENGINE)

    pids = Array.new(MAX_THREADS) do
      Process.fork do
        ATTEMPTS.times { |i| @client.call('INCR', "key#{i}") }
      end
    end

    pids += Array.new(MAX_THREADS) do
      Process.fork do
        ATTEMPTS.times { |i| @client.call('DECR', "key#{i}") }
      end
    end

    pids.each do |pid|
      _, status = Process.waitpid2(pid)
      assert_predicate(status, :success?)
    end

    MAX_THREADS.times { |i| assert_equal(WANT, @client.call('GET', "key#{i}")) }
  end

  def test_forking_with_pipelining
    skip("fork is not available on #{RUBY_ENGINE}") if %w[jruby truffleruby].include?(RUBY_ENGINE)

    pids = Array.new(MAX_THREADS) do
      Process.fork do
        @client.pipelined { |pi| ATTEMPTS.times { |i| pi.call('INCR', "key#{i}") } }
      end
    end

    pids += Array.new(MAX_THREADS) do
      Process.fork do
        @client.pipelined { |pi| ATTEMPTS.times { |i| pi.call('DECR', "key#{i}") } }
      end
    end

    pids.each do |pid|
      _, status = Process.waitpid2(pid)
      assert_predicate(status, :success?)
    end

    MAX_THREADS.times { |i| assert_equal(WANT, @client.call('GET', "key#{i}")) }
  end

  def test_forking_with_transaction
    skip("fork is not available on #{RUBY_ENGINE}") if %w[jruby truffleruby].include?(RUBY_ENGINE)

    @client.call('SET', '{key}1', WANT)

    pids = Array.new(MAX_THREADS) do
      Process.fork do
        @client.multi(watch: %w[{key}1]) do |tx|
          ATTEMPTS.times do
            tx.call('INCR', '{key}1')
            tx.call('DECR', '{key}1')
          end
        end
      end
    end

    pids.each do |pid|
      _, status = Process.waitpid2(pid)
      assert_predicate(status, :success?)
    end

    assert_equal(WANT, @client.call('GET', '{key}1'))
  end

  def test_threading
    threads = Array.new(MAX_THREADS) do
      Thread.new do
        ATTEMPTS.times { |i| @client.call('INCR', "key#{i}") }
        nil
      rescue StandardError => e
        e
      end
    end

    threads += Array.new(MAX_THREADS) do
      Thread.new do
        ATTEMPTS.times { |i| @client.call('DECR', "key#{i}") }
        nil
      rescue StandardError => e
        e
      end
    end

    threads.each { |t| assert_nil(t.value) }
    MAX_THREADS.times { |i| assert_equal(WANT, @client.call('GET', "key#{i}")) }
  end

  def test_threading_with_pipelining
    threads = Array.new(MAX_THREADS) do
      Thread.new do
        @client.pipelined { |pi| ATTEMPTS.times { |i| pi.call('INCR', "key#{i}") } }
        nil
      rescue StandardError => e
        e
      end
    end

    threads += Array.new(MAX_THREADS) do
      Thread.new do
        @client.pipelined { |pi| ATTEMPTS.times { |i| pi.call('DECR', "key#{i}") } }
        nil
      rescue StandardError => e
        e
      end
    end

    threads.each { |t| assert_nil(t.value) }
    MAX_THREADS.times { |i| assert_equal(WANT, @client.call('GET', "key#{i}")) }
  end

  def test_threading_with_transaction
    @client.call('SET', '{key}1', WANT)

    threads = Array.new(MAX_THREADS) do
      Thread.new do
        @client.multi(watch: %w[{key}1]) do |tx|
          ATTEMPTS.times do
            tx.call('INCR', '{key}1')
            tx.call('DECR', '{key}1')
          end
        end
      rescue StandardError => e
        e
      end
    end

    threads.each { |t| refute_instance_of(StandardError, t.value) }
    assert_equal(WANT, @client.call('GET', '{key}1'))
  end

  def test_ractor
    skip('Ractor is not available') unless Object.const_defined?(:Ractor, false)
    skip("#{RedisClient.default_driver} is not safe for Ractor") if RedisClient.default_driver != RedisClient::RubyConnection
    skip('OpenSSL gem has non-shareable objects') if TEST_REDIS_SSL
    skip('unstable ractor') if RUBY_ENGINE == 'ruby' && RUBY_ENGINE_VERSION.split('.').take(2).join('.').to_f < 4.0

    ractors = Array.new(MAX_THREADS) do |i|
      Ractor.new(i) do |i|
        c = ::RedisClient.cluster(nodes: TEST_NODE_URIS, fixed_hostname: TEST_FIXED_HOSTNAME, **TEST_GENERIC_OPTIONS).new_client
        c.call('get', "key#{i}")
      rescue StandardError => e
        e
      ensure
        c&.close
      end
    end

    ractors.each { |r| assert_equal(WANT, r.value) }
  end

  def test_ractor_with_pipelining
    skip('Ractor is not available') unless Object.const_defined?(:Ractor, false)
    skip("#{RedisClient.default_driver} is not safe for Ractor") if RedisClient.default_driver != RedisClient::RubyConnection
    skip('OpenSSL gem has non-shareable objects') if TEST_REDIS_SSL
    skip('unstable ractor') if RUBY_ENGINE == 'ruby' && RUBY_ENGINE_VERSION.split('.').take(2).join('.').to_f < 4.0

    ractors = Array.new(MAX_THREADS) do |i|
      Ractor.new(i) do |i|
        c = ::RedisClient.cluster(nodes: TEST_NODE_URIS, fixed_hostname: TEST_FIXED_HOSTNAME, **TEST_GENERIC_OPTIONS).new_client
        c.pipelined do |pi|
          pi.call('get', "key#{i}")
          pi.call('echo', 'hi')
        end
      rescue StandardError => e
        e
      ensure
        c&.close
      end
    end

    ractors.each { |r| assert_equal([WANT, 'hi'], r.value) }
  end

  def test_ractor_with_transaction
    skip('Ractor is not available') unless Object.const_defined?(:Ractor, false)
    skip("#{RedisClient.default_driver} is not safe for Ractor") if RedisClient.default_driver != RedisClient::RubyConnection
    skip('OpenSSL gem has non-shareable objects') if TEST_REDIS_SSL
    skip('unstable ractor') if RUBY_ENGINE == 'ruby' && RUBY_ENGINE_VERSION.split('.').take(2).join('.').to_f < 4.0

    ractors = Array.new(MAX_THREADS) do |i|
      Ractor.new(i) do |i|
        c = ::RedisClient.cluster(nodes: TEST_NODE_URIS, fixed_hostname: TEST_FIXED_HOSTNAME, **TEST_GENERIC_OPTIONS).new_client
        c.multi(watch: ["key#{i}"]) do |tx|
          tx.call('incr', "key#{i}")
          tx.call('incr', "key#{i}")
        end
      rescue StandardError => e
        e
      ensure
        c&.close
      end
    end

    ractors.each { |r| assert_equal([2, 3], r.value) }
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
