# frozen_string_literal: true

# rubocop:disable Lint/UnderscorePrefixedVariableName

require 'redis_client'

TEST_REDIS_HOST = ENV.fetch('REDIS_HOST', '127.0.0.1')
TEST_REDIS_PORT = 6379
TEST_TIMEOUT_SEC = 5.0
TEST_RECONNECT_ATTEMPTS = 3

_new_raw_cli = ->(**opts) { ::RedisClient.config(host: TEST_REDIS_HOST, port: TEST_REDIS_PORT, **opts).new_client }
_test_cert_path = ->(f) { File.expand_path(File.join('ssl_certs', f), __dir__) }

TEST_SSL_PARAMS = {
  ca_file: _test_cert_path.call('redis-rb-ca.crt'),
  cert: _test_cert_path.call('redis-rb-cert.crt'),
  key: _test_cert_path.call('redis-rb-cert.key')
}.freeze

_base_opts = {
  timeout: TEST_TIMEOUT_SEC,
  reconnect_attempts: TEST_RECONNECT_ATTEMPTS
}

_ssl_opts = {
  ssl: true,
  ssl_params: TEST_SSL_PARAMS
}.freeze

_redis_scheme = 'redis'

begin
  _tmp_cli = _new_raw_cli.call(**_base_opts)
  _tmp_cli.call('PING')
rescue ::RedisClient::UnsupportedServer
  _base_opts.merge!(protocol: 2)
rescue ::RedisClient::ConnectionError => e
  raise unless e.message.include?('Connection reset by peer') || e.message.include?('EOFError')

  _redis_scheme = 'rediss'
rescue ::RedisClient::CommandError => e
  raise unless e.message.include?('NOAUTH')

  _base_opts.merge!(password: '!&<123-abc>')
ensure
  _tmp_cli&.close
end

TEST_REDIS_SCHEME = _redis_scheme
TEST_REDIS_SSL = TEST_REDIS_SCHEME == 'rediss'
TEST_FIXED_HOSTNAME = TEST_REDIS_SSL ? TEST_REDIS_HOST : nil

TEST_SHARD_SIZE = ENV.fetch('REDIS_SHARD_SIZE', '3').to_i
TEST_REPLICA_SIZE = ENV.fetch('REDIS_REPLICA_SIZE', '1').to_i
TEST_NUMBER_OF_REPLICAS = TEST_REPLICA_SIZE * TEST_SHARD_SIZE
TEST_NUMBER_OF_NODES = TEST_SHARD_SIZE + TEST_NUMBER_OF_REPLICAS

case TEST_REDIS_HOST
when '127.0.0.1', 'localhost'
  TEST_REDIS_PORTS = TEST_REDIS_PORT.upto(TEST_REDIS_PORT + TEST_NUMBER_OF_NODES - 1).to_a.freeze
  TEST_NODE_URIS = TEST_REDIS_PORTS.map { |v| "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{v}" }.freeze
  TEST_NODE_OPTIONS = TEST_REDIS_PORTS.to_h { |v| ["#{TEST_REDIS_HOST}:#{v}", { host: TEST_REDIS_HOST, port: v }] }.freeze
when 'node1'
  TEST_REDIS_PORTS = Array.new(TEST_NUMBER_OF_NODES) { TEST_REDIS_PORT }.freeze
  TEST_NODE_URIS = Array.new(TEST_NUMBER_OF_NODES) do |i|
    host = "node#{i + 1}"
    "#{TEST_REDIS_SCHEME}://#{host}:#{TEST_REDIS_PORT}"
  end.freeze

  TEST_NODE_OPTIONS = Array.new(TEST_NUMBER_OF_NODES) do |i|
    host = "node#{format("%#{TEST_NUMBER_OF_NODES}d", i + 1)}"
    ["#{host}:#{TEST_REDIS_PORT}", { host: host, port: TEST_REDIS_PORT }]
  end.to_h.freeze
else
  raise NotImplementedError, TEST_REDIS_HOST
end

TEST_GENERIC_OPTIONS = (TEST_REDIS_SSL ? _base_opts.merge(_ssl_opts) : _base_opts).freeze

_tmp_cli = _new_raw_cli.call(**TEST_GENERIC_OPTIONS)
TEST_REDIS_VERSION = _tmp_cli.call('INFO', 'SERVER').split("\r\n").grep(/redis_version.+/).first.split(':')[1]
TEST_REDIS_MAJOR_VERSION = Integer(TEST_REDIS_VERSION.split('.').first)
_tmp_cli.close

BENCH_ENVOY_OPTIONS = { port: 7000, protocol: 2 }.freeze
BENCH_REDIS_CLUSTER_PROXY_OPTIONS = { port: 7001, protocol: 2 }.freeze

if Object.const_defined?(:Ractor, false) && Ractor.respond_to?(:make_shareable)
  Ractor.make_shareable(TEST_NODE_URIS)
  Ractor.make_shareable(TEST_GENERIC_OPTIONS)
end

# rubocop:enable Lint/UnderscorePrefixedVariableName
