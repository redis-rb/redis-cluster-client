# frozen_string_literal: true

require 'testing_helper'
require 'redis_cluster_client'
require 'redis_client/cluster_config'

class TestRedisClient < Minitest::Test
  def test_cluster
    [
      { kwargs: {}, error: nil },
      { kwargs: { nodes: { host: '127.0.0.1', port: 6379 } }, error: nil },
      { kwargs: { nodes: [{ host: '127.0.0.1', port: 6379 }] }, error: nil },
      { kwargs: { nodes: 'redis://127.0.0.1:6379' }, error: nil },
      { kwargs: { nodes: %w[redis://127.0.0.1:6379] }, error: nil },
      { kwargs: { nodes: %w[redis://127.0.0.1:6379], replica: true }, error: nil },
      { kwargs: { nodes: %w[redis://127.0.0.1:6379], replica: true, fixed_hostname: 'endpoint.example.com' }, error: nil },
      { kwargs: { nodes: %w[redis://127.0.0.1:6379], replica: 1, fixed_hostname: 1 }, error: nil },
      { kwargs: { nodes: %w[redis://127.0.0.1:6379], foo: 'bar' }, error: nil },
      { kwargs: { nodes: [''] }, error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
      { kwargs: { nodes: ['foo'] }, error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
      { kwargs: { nodes: [6379] }, error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
      { kwargs: { nodes: [] }, error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
      { kwargs: { nodes: '' }, error: ::RedisClient::ClusterConfig::InvalidClientConfigError },
      { kwargs: { nodes: nil }, error: ::RedisClient::ClusterConfig::InvalidClientConfigError }
    ].each_with_index do |c, idx|
      msg = "Case: #{idx}"
      got = -> { ::RedisClient.cluster(**c[:kwargs]) }
      if c[:error].nil?
        assert_instance_of(::RedisClient::ClusterConfig, got.call, msg)
      else
        assert_raises(c[:error], msg, &got)
      end
    end
  end
end
