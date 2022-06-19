# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class TestClusterConfig < TestingWrapper
    def test_inspect
      want = '#<RedisClient::ClusterConfig [{:host=>"127.0.0.1", :port=>6379}]>'
      got = ::RedisClient::ClusterConfig.new.inspect
      assert_equal(want, got)
    end

    def test_new_pool
      assert_instance_of(
        ::RedisClient::Cluster,
        ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          **TEST_GENERIC_OPTIONS
        ).new_pool
      )
    end

    def test_new_client
      assert_instance_of(
        ::RedisClient::Cluster,
        ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          **TEST_GENERIC_OPTIONS
        ).new_client
      )
    end

    def test_per_node_key
      [
        {
          config: ::RedisClient::ClusterConfig.new,
          want: {
            '127.0.0.1:6379' => { host: '127.0.0.1', port: 6379 }
          }
        },
        {
          config: ::RedisClient::ClusterConfig.new(replica: true),
          want: {
            '127.0.0.1:6379' => { host: '127.0.0.1', port: 6379 }
          }
        },
        {
          config: ::RedisClient::ClusterConfig.new(fixed_hostname: 'endpoint.example.com'),
          want: {
            '127.0.0.1:6379' => { host: 'endpoint.example.com', port: 6379 }
          }
        },
        {
          config: ::RedisClient::ClusterConfig.new(timeout: 1),
          want: {
            '127.0.0.1:6379' => { host: '127.0.0.1', port: 6379, timeout: 1 }
          }
        }
      ].each_with_index do |c, idx|
        assert_equal(c[:want], c[:config].per_node_key, "Case: #{idx}")
      end
    end

    def test_use_replica?
      assert_predicate(::RedisClient::ClusterConfig.new(replica: true), :use_replica?)
      refute_predicate(::RedisClient::ClusterConfig.new(replica: false), :use_replica?)
      refute_predicate(::RedisClient::ClusterConfig.new, :use_replica?)
    end

    def test_update_node
      config = ::RedisClient::ClusterConfig.new(nodes: %w[redis://127.0.0.1:6379])
      assert_equal([{ host: '127.0.0.1', port: 6379 }], config.instance_variable_get(:@node_configs))
      config.update_node(%w[redis://127.0.0.2:6380])
      assert_equal([{ host: '127.0.0.2', port: 6380 }], config.instance_variable_get(:@node_configs))
    end

    def test_add_node
      config = ::RedisClient::ClusterConfig.new(nodes: %w[redis://127.0.0.1:6379])
      assert_equal([{ host: '127.0.0.1', port: 6379 }], config.instance_variable_get(:@node_configs))
      config.add_node('127.0.0.2', 6380)
      assert_equal([{ host: '127.0.0.1', port: 6379 }, { host: '127.0.0.2', port: 6380 }], config.instance_variable_get(:@node_configs))
    end

    def test_dup
      orig = ::RedisClient::ClusterConfig.new
      copy = orig.dup
      refute_equal(orig.object_id, copy.object_id)
    end

    def test_build_node_configs
      config = ::RedisClient::ClusterConfig.new
      [
        { addrs: %w[redis://127.0.0.1], want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: %w[redis://127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: %w[redis://127.0.0.1:6379/1], want: [{ host: '127.0.0.1', port: 6379, db: 1 }] },
        { addrs: %w[redis://127.0.0.1:6379 redis://127.0.0.2:6380], want: [{ host: '127.0.0.1', port: 6379 }, { host: '127.0.0.2', port: 6380 }] },
        { addrs: %w[rediss://foo:bar@127.0.0.1:6379], want: [{ ssl: true, username: 'foo', password: 'bar', host: '127.0.0.1', port: 6379 }] },
        { addrs: %w[redis://foo@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, username: 'foo' }] },
        { addrs: %w[redis://:bar@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, password: 'bar' }] },
        { addrs: [{ host: '127.0.0.1', port: 6379 }], want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: [{ host: '127.0.0.1', port: 6379 }, { host: '127.0.0.2', port: '6380' }], want: [{ host: '127.0.0.1', port: 6379 }, { host: '127.0.0.2', port: 6380 }] },
        { addrs: [{ host: '127.0.0.1', port: 6379, username: 'foo', password: 'bar', ssl: true }], want: [{ ssl: true, username: 'foo', password: 'bar', host: '127.0.0.1', port: 6379 }] },
        { addrs: [{ host: '127.0.0.1', port: 6379, db: 1 }], want: [{ host: '127.0.0.1', port: 6379, db: 1 }] },
        { addrs: 'redis://127.0.0.1:6379', want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: { host: '127.0.0.1', port: 6379 }, want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: [{ host: '127.0.0.1' }], want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: %w[http://127.0.0.1:80], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [{ host: '127.0.0.1', port: 'foo' }], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: %w[redis://127.0.0.1:foo], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [6379], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: ['foo'], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [''], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [{}], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: {}, error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: '', error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: nil, error: ::RedisClient::ClusterConfig::InvalidClientConfigError }
      ].each_with_index do |c, idx|
        msg = "Case: #{idx}: #{c}"
        got = -> { config.send(:build_node_configs, c[:addrs]) }
        if c.key?(:error)
          assert_raises(c[:error], msg, &got)
        else
          assert_equal(c.fetch(:want), got.call, msg)
        end
      end
    end

    def test_merge_generic_config
      config = ::RedisClient::ClusterConfig.new
      [
        {
          params: {
            client_config: { ssl: false, username: 'foo', password: 'bar', timeout: 1 },
            node_configs: [{ ssl: true, username: 'baz', password: 'zap', host: '127.0.0.1' }]
          },
          want: { ssl: true, username: 'baz', password: 'zap', timeout: 1 }
        },
        {
          params: {
            client_config: { ssl: false, timeout: 1 },
            node_configs: [{ ssl: true, host: '127.0.0.1' }]
          },
          want: { ssl: true, timeout: 1 }
        },
        {
          params: {
            client_config: { timeout: 1 },
            node_configs: [{ ssl: true }]
          },
          want: { ssl: true, timeout: 1 }
        },
        { params: { client_config: {}, node_configs: [], keys: [] }, want: {} }
      ].each_with_index do |c, idx|
        msg = "Case: #{idx}"
        got = config.send(:merge_generic_config, c[:params][:client_config], c[:params][:node_configs])
        assert_equal(c[:want], got, msg)
      end
    end
  end
end
