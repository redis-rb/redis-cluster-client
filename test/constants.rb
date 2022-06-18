# frozen_string_literal: true

require 'redis_client'

TEST_REDIS_HOST = '127.0.0.1'
TEST_REDIS_PORTS = (6379..6384).freeze
TEST_TIMEOUT_SEC = 5.0
TEST_RECONNECT_ATTEMPTS = 3

begin
  ::RedisClient.config(
    host: TEST_REDIS_HOST,
    port: TEST_REDIS_PORTS.first,
    timeout: TEST_TIMEOUT_SEC
  ).new_client.call('PING')
  TEST_REDIS_SCHEME = 'redis'
rescue ::RedisClient::ConnectionError => e
  raise e if e.message != 'Connection reset by peer'

  TEST_REDIS_SCHEME = 'rediss'
end

TEST_REDIS_SSL = TEST_REDIS_SCHEME == 'rediss'
TEST_REPLICA_SIZE = 1
TEST_NUMBER_OF_REPLICAS = 3
TEST_FIXED_HOSTNAME = TEST_REDIS_SSL ? TEST_REDIS_HOST : nil

TEST_NODE_URIS = TEST_REDIS_PORTS.map { |v| "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{v}" }.freeze
TEST_NODE_OPTIONS = TEST_REDIS_PORTS.to_h { |v| ["#{TEST_REDIS_HOST}:#{v}", { host: TEST_REDIS_HOST, port: v }] }.freeze

GET_TEST_CERT_PATH = ->(f) { File.expand_path(File.join('ssl_certs', f), __dir__) }
TEST_GENERIC_OPTIONS = if TEST_REDIS_SSL
                         {
                           timeout: TEST_TIMEOUT_SEC,
                           reconnect_attempts: TEST_RECONNECT_ATTEMPTS,
                           ssl: true,
                           ssl_params: {
                             ca_file: GET_TEST_CERT_PATH.call('redis-rb-ca.crt'),
                             cert: GET_TEST_CERT_PATH.call('redis-rb-cert.crt'),
                             key: GET_TEST_CERT_PATH.call('redis-rb-cert.key')
                           }
                         }.freeze
                       else
                         {
                           timeout: TEST_TIMEOUT_SEC,
                           reconnect_attempts: TEST_RECONNECT_ATTEMPTS
                         }.freeze
                       end
