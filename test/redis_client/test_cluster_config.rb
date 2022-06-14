# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster_config'

class RedisClient
  class TestClusterConfig < Minitest::Test
    def test_build_node_configs
      config = ::RedisClient::ClusterConfig.new(nodes: %w[redis://127.0.0.1:6379])
      [
        { addrs: %w[redis://127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: %w[redis://127.0.0.1:6379/1], want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: %w[redis://127.0.0.1:6379 redis://127.0.0.2:6380], want: [{ host: '127.0.0.1', port: 6379 }, { host: '127.0.0.2', port: 6380 }] },
        { addrs: %w[rediss://foo:bar@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, username: 'foo', password: 'bar', ssl: true }] },
        { addrs: %w[redis://foo@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, username: 'foo' }] },
        { addrs: %w[redis://:bar@127.0.0.1:6379], want: [{ host: '127.0.0.1', port: 6379, password: 'bar' }] },
        { addrs: [{ host: '127.0.0.1', port: 6379 }], want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: [{ host: '127.0.0.1', port: 6379 }, { host: '127.0.0.2', port: '6380' }], want: [{ host: '127.0.0.1', port: 6379 }, { host: '127.0.0.2', port: 6380 }] },
        { addrs: [{ host: '127.0.0.1', port: 6379, username: 'foo', password: 'bar', ssl: true }], want: [{ host: '127.0.0.1', port: 6379, username: 'foo', password: 'bar', ssl: true }] },
        { addrs: [{ host: '127.0.0.1', port: 6379, db: 1 }], want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: 'redis://127.0.0.1:6379', want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: { host: '127.0.0.1', port: 6379 }, want: [{ host: '127.0.0.1', port: 6379 }] },
        { addrs: %w[http://127.0.0.1:80], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [{ host: '127.0.0.1' }], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [{ host: '127.0.0.1', port: 'foo' }], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: %w[redis://127.0.0.1:foo], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [''], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: ['foo'], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [6379], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: [], error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: {}, error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: '', error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
        { addrs: nil, error: ::RedisClient::ClusterConfig::InvalidClientConfigError }
      ].each_with_index do |c, idx|
        msg = "Case: #{idx}"
        got = -> { config.send(:build_node_configs, c[:addrs]) }
        if c.key?(:error)
          assert_raises(c[:error], msg, &got)
        else
          assert_equal(c.fetch(:want), got.call, msg)
        end
      end
    end

    def test_add_common_node_config_if_needed
      config = ::RedisClient::ClusterConfig.new(nodes: %w[redis://127.0.0.1:6379])
      [
        { params: { client_config: {}, node_configs: [], key: '' }, want: {} }
      ].each_with_index do |c, idx|
        msg = "Case: #{idx}"
        got = config.send(:add_common_node_config_if_needed, c[:params][:client_config], c[:params][:node_configs], c[:params][:key])
        assert_equal(c[:want], got, msg)
      end
    end
  end
end
