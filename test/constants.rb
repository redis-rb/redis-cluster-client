# frozen_string_literal: true

# rubocop:disable Lint/UnderscorePrefixedVariableName

require 'redis_client'

TEST_REDIS_HOST = '127.0.0.1'
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
}.freeze

_ssl_opts = {
  ssl: true,
  ssl_params: TEST_SSL_PARAMS
}.freeze

_test_raw_cli = _new_raw_cli.call(**_base_opts)

begin
  _test_raw_cli.call('PING')
  TEST_REDIS_SCHEME = 'redis'
rescue ::RedisClient::ConnectionError => e
  raise e if e.message != 'Connection reset by peer'

  TEST_REDIS_SCHEME = 'rediss'
  _test_raw_cli = _new_raw_cli.call(**_base_opts, **_ssl_opts)
end

TEST_REDIS_SSL = TEST_REDIS_SCHEME == 'rediss'
TEST_FIXED_HOSTNAME = TEST_REDIS_SSL ? TEST_REDIS_HOST : nil

_rows = _test_raw_cli.call('cluster', 'nodes').split("\n")

TEST_NUMBER_OF_NODES = _rows.size
TEST_SHARD_SIZE = _rows.grep(/master/).size
TEST_NUMBER_OF_REPLICAS = _rows.grep(/slave/).size
TEST_REPLICA_SIZE = TEST_NUMBER_OF_REPLICAS / TEST_SHARD_SIZE

TEST_REDIS_PORTS = TEST_REDIS_PORT.upto(TEST_REDIS_PORT + TEST_NUMBER_OF_NODES - 1).to_a.freeze
TEST_NODE_URIS = TEST_REDIS_PORTS.map { |v| "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{v}" }.freeze
TEST_NODE_OPTIONS = TEST_REDIS_PORTS.to_h { |v| ["#{TEST_REDIS_HOST}:#{v}", { host: TEST_REDIS_HOST, port: v }] }.freeze

TEST_GENERIC_OPTIONS = TEST_REDIS_SSL ? _base_opts.merge(_ssl_opts).freeze : _base_opts

_test_raw_cli.close
_test_raw_cli = nil

# rubocop:enable Lint/UnderscorePrefixedVariableName
