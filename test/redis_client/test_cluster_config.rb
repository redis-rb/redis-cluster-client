# frozen_string_literal: true

require 'uri'
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

    def test_startup_nodes
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
        },
        {
          config: ::RedisClient::ClusterConfig.new(nodes: ['redis://1.2.3.4:1234', 'rediss://5.6.7.8:5678']),
          want: {
            '1.2.3.4:1234' => { host: '1.2.3.4', port: 1234 },
            '5.6.7.8:5678' => { host: '5.6.7.8', port: 5678, ssl: true }
          }
        },
        {
          config: ::RedisClient::ClusterConfig.new(custom: { foo: 'bar' }),
          want: {
            '127.0.0.1:6379' => { host: '127.0.0.1', port: 6379, custom: { foo: 'bar' } }
          }
        }
      ].each_with_index do |c, idx|
        assert_equal(c[:want], c[:config].startup_nodes, "Case: #{idx}")
      end
    end

    def test_use_replica?
      assert_predicate(::RedisClient::ClusterConfig.new(replica: true), :use_replica?)
      refute_predicate(::RedisClient::ClusterConfig.new(replica: false), :use_replica?)
      refute_predicate(::RedisClient::ClusterConfig.new, :use_replica?)
    end

    def test_replica_affinity
      [
        { value: :random, want: :random },
        { value: 'random', want: :random },
        { value: :latency, want: :latency },
        { value: 'latency', want: :latency },
        { value: :unknown, want: :unknown },
        { value: 'unknown', want: :unknown },
        { value: 0, want: :'0' },
        { value: nil, want: :'' }
      ].each do |c|
        cfg = ::RedisClient::ClusterConfig.new(replica_affinity: c[:value])
        assert_equal(c[:want], cfg.replica_affinity)
      end
    end

    def test_command_builder
      assert_equal(::RedisClient::CommandBuilder, ::RedisClient::ClusterConfig.new.command_builder)
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
        { addrs: %w[redis://foo:@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, username: 'foo' }] },
        { addrs: %w[redis://:bar@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, password: 'bar' }] },
        { addrs: %W[redis://#{URI.encode_www_form_component('!&<123-abc>')}:@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, username: '!&<123-abc>' }] },
        { addrs: %W[redis://:#{URI.encode_www_form_component('!&<123-abc>')}@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, password: '!&<123-abc>' }] },
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

    def test_client_config_for_node
      config = ::RedisClient::ClusterConfig.new(
        nodes: ['redis://username:password@1.2.3.4:1234', 'rediss://5.6.7.8:5678'],
        custom: { foo: 'bar' }
      )
      assert_equal({
                     host: '9.9.9.9',
                     port: 9999,
                     username: 'username',
                     password: 'password',
                     custom: { foo: 'bar' }
                   }, config.client_config_for_node('9.9.9.9:9999'))
    end

    def test_client_config_id
      assert_equal('foo-cluster', ::RedisClient::ClusterConfig.new(id: 'foo-cluster').id)
      assert_nil(::RedisClient::ClusterConfig.new.id)
    end
  end
end
