# frozen_string_literal: true

# @see https://docs.ruby-lang.org/en/2.1.0/MiniTest/Assertions.html

require 'redis_client'
require 'openssl'

module TestingHelper
  TEST_REDIS_SCHEME = ENV.fetch('REDIS_SCHEME', 'redis')
  TEST_REDIS_SSL = TEST_REDIS_SCHEME == 'rediss'
  TEST_REDIS_HOST = '127.0.0.1'
  TEST_REDIS_PORTS = (6379..6384).freeze
  TEST_TIMEOUT_SEC = 5.0
  TEST_RECONNECT_ATTEMPTS = 3
  TEST_REPLICA_SIZE = 1
  TEST_NUMBER_OF_REPLICAS = 3
  TEST_FIXED_HOSTNAME = TEST_REDIS_SSL ? TEST_REDIS_HOST : nil

  TEST_NODE_URIS = TEST_REDIS_PORTS.map { |v| "#{TEST_REDIS_SCHEME}://#{TEST_REDIS_HOST}:#{v}" }.freeze
  TEST_NODE_OPTIONS = TEST_REDIS_PORTS.to_h { |v| ["#{TEST_REDIS_HOST}:#{v}", { host: TEST_REDIS_HOST, port: v }] }.freeze

  GET_CERT_PATH = ->(f) { File.expand_path(File.join('ssl_certs', f), __dir__) }
  TEST_GENERIC_OPTIONS = if TEST_REDIS_SSL
                           {
                             timeout: TEST_TIMEOUT_SEC,
                             reconnect_attempts: TEST_RECONNECT_ATTEMPTS,
                             ssl: true,
                             ssl_params: {
                               ca_file: GET_CERT_PATH.call('redis-rb-ca.crt'),
                               cert: GET_CERT_PATH.call('redis-rb-cert.crt'),
                               key: GET_CERT_PATH.call('redis-rb-cert.key')
                             }
                           }.freeze
                         else
                           {
                             timeout: TEST_TIMEOUT_SEC,
                             reconnect_attempts: TEST_RECONNECT_ATTEMPTS
                           }.freeze
                         end

  def setup
    # TODO: for feature tests
  end

  def teardown
    # TODO: for feature tests
  end
end

module TestingHelper
  class ClusterController
    SLOT_SIZE = 16_384

    def initialize(node_addrs, **kwargs)
      raise 'Redis Cluster requires at least 3 master nodes.' if node_addrs.size < 3

      @timeout = kwargs[:timeout] || TEST_TIMEOUT_SEC
      @clients = node_addrs.map do |addr|
        ::RedisClient.new(url: addr, **TEST_GENERIC_OPTIONS.merge(kwargs))
      end
    end

    def wait_for_cluster_to_be_ready
      wait_meeting(@clients)
      wait_cluster_building(@clients)
      wait_replication(@clients)
      wait_cluster_recovering(@clients)
    end

    def rebuild
      flush_all_data(@clients)
      reset_cluster(@clients)
      assign_slots(@clients)
      save_config_epoch(@clients)
      meet_each_other(@clients)
      wait_meeting(@clients)
      replicate(@clients)
      save_config(@clients)
      wait_cluster_building(@clients)
      wait_replication(@clients)
      wait_cluster_recovering(@clients)
    end

    def down
      flush_all_data(@clients)
      reset_cluster(@clients)
    end

    def fail_serving_master
      master, slave = take_replication_pairs(@clients)
      master.shutdown
      attempt_count = 1
      max_attempts = 500
      attempt_count.step(max_attempts) do |i|
        break if slave.role == 'master' || i >= max_attempts

        attempt_count += 1
        sleep 0.1
      end
    end

    def failover
      master, slave = take_replication_pairs(@clients)
      wait_replication_delay(@clients, @timeout)
      slave.cluster(:failover, :takeover)
      wait_failover(to_node_key(master), to_node_key(slave), @clients)
      wait_replication_delay(@clients, @timeout)
      wait_cluster_recovering(@clients)
    end

    def start_resharding(slot, src_node_key, dest_node_key, slice_size: 10)
      node_map = hashify_node_map(@clients.first)
      src_node_id = node_map.fetch(src_node_key)
      src_client = find_client(@clients, src_node_key)
      dest_node_id = node_map.fetch(dest_node_key)
      dest_client = find_client(@clients, dest_node_key)
      dest_host, dest_port = dest_node_key.split(':')

      dest_client.call('CLUSTER', 'SETSLOT', slot, 'IMPORTING', src_node_id)
      src_client.call('CLUSTER', 'SETSLOT', slot, 'MIGRATING', dest_node_id)

      keys_count = src_client.call('CLUSTER', 'COUNTKEYSINSLOT', slot)
      loop do
        break if keys_count <= 0

        keys = src_client.call('CLUSTER', 'GETKEYSINSLOT', slot, slice_size)
        break if keys.empty?

        keys.each do |k|
          src_client.call('MIGRATE', dest_host, dest_port, k)
        rescue ::RedisClient::CommandError => e
          raise unless e.message.start_with?('IOERR')

          src_client.call('MIGRATE', dest_host, dest_port, k, 'REPLACE') # retry once
        ensure
          keys_count -= 1
        end
      end
    end

    def finish_resharding(slot, dest_node_key)
      node_map = hashify_node_map(@clients.first)
      @clients.first.call('CLUSTER', 'SETSLOT', slot, 'NODE', node_map.fetch(dest_node_key))
    end

    def close
      @clients.each(&:close)
    end

    private

    def flush_all_data(clients)
      clients.each do |c|
        c.flushall
      rescue ::RedisClient::CommandError
        # READONLY You can't write against a read only slave.
        nil
      end
    end

    def reset_cluster(clients)
      clients.each { |c| c.cluster(:reset) }
    end

    def assign_slots(clients)
      masters = take_masters(clients)
      slot_slice = SLOT_SIZE / masters.size
      mod = SLOT_SIZE % masters.size
      slot_sizes = Array.new(masters.size, slot_slice)
      mod.downto(1) { |i| slot_sizes[i] += 1 }

      slot_idx = 0
      masters.zip(slot_sizes).each do |c, s|
        slot_range = slot_idx..slot_idx + s - 1
        c.cluster(:addslots, *slot_range.to_a)
        slot_idx += s
      end
    end

    def save_config_epoch(clients)
      clients.each_with_index do |c, i|
        c.cluster('set-config-epoch', i + 1)
      rescue ::RedisClient::CommandError
        # ERR Node config epoch is already non-zero
        nil
      end
    end

    def meet_each_other(clients)
      clients.each do |client|
        next if client.id == clients.first.id

        client.cluster(:meet, target_host, target_port)
      end
    end

    def wait_meeting(clients, max_attempts: 600)
      size = clients.size.to_s

      wait_for_state(clients, max_attempts) do |client|
        info = hashify_cluster_info(client)
        info['cluster_known_nodes'] == size
      end
    end

    def replicate(clients)
      node_map = hashify_node_map(clients.first)
      masters = take_masters(clients)

      take_slaves(clients).each_with_index do |slave, i|
        master_info = masters[i].connection
        master_host = master_info.fetch(:host)
        master_port = master_info.fetch(:port)

        loop do
          begin
            master_node_id = node_map.fetch("#{master_host}:#{master_port}")
            slave.cluster(:replicate, master_node_id)
          rescue ::RedisClient::CommandError
            # ERR Unknown node [key]
            sleep 0.1
            node_map = hashify_node_map(clients.first)
            next
          end

          break
        end
      end
    end

    def save_config(clients)
      clients.each { |c| c.cluster(:saveconfig) }
    end

    def wait_cluster_building(clients, max_attempts: 600)
      wait_for_state(clients, max_attempts) do |client|
        info = hashify_cluster_info(client)
        info['cluster_state'] == 'ok'
      end
    end

    def wait_replication(clients, max_attempts: 600)
      wait_for_state(clients, max_attempts) do |client|
        flags = hashify_cluster_node_flags(client)
        flags.values.count { |f| f == 'slave' } == 3
      end
    end

    def wait_failover(master_key, slave_key, clients, max_attempts: 600)
      wait_for_state(clients, max_attempts) do |client|
        flags = hashify_cluster_node_flags(client)
        flags[master_key] == 'slave' && flags[slave_key] == 'master'
      end
    end

    def wait_replication_delay(clients, timeout_sec)
      timeout_msec = timeout_sec.to_i * 1000
      wait_for_state(clients, clients.size + 1) do |client|
        client.blocking_call('WAIT', 1, timeout_msec) if client.call('ROLE').first == 'master'
        true
      end
    end

    def wait_cluster_recovering(clients, max_attempts: 600)
      key = 0
      wait_for_state(clients, max_attempts) do |client|
        client.call('GET', key) if client.call('ROLE').first == 'master'
        true
      rescue ::RedisClient::CommandError => e
        if e.message.start_with?('CLUSTERDOWN')
          false
        elsif e.message.start_with?('MOVED')
          key += 1
          false
        else
          true
        end
      end
    end

    def wait_for_state(clients, max_attempts)
      attempt_count = 1
      clients.each do |client|
        attempt_count.step(max_attempts) do |i|
          break if i >= max_attempts

          attempt_count += 1
          break if yield(client)

          sleep 0.1
        end
      end
    end

    def hashify_cluster_info(client)
      client.call('CLUSTER', 'INFO').split("\r\n").to_h { |str| str.split(':') }
    end

    def hashify_cluster_node_flags(client)
      client.call('CLUSTER', 'NODES').split("\n").map(&:split)
            .to_h { |arr| [arr[1].split('@').first, (arr[2].split(',') & %w[master slave]).first] }
    end

    def hashify_node_map(client)
      client.call('CLUSTER', 'NODES').split("\n").map(&:split).to_h { |arr| [arr[1].split('@').first, arr[0]] }
    end

    def take_masters(clients)
      size = clients.size / 2
      return clients if size < 3

      clients.take(size)
    end

    def take_slaves(clients)
      size = clients.size / 2
      return [] if size < 3

      clients[size..size * 2]
    end

    def take_replication_pairs(clients)
      [take_masters(clients).last, take_slaves(clients).last]
    end

    def find_client(clients, node_key)
      clients.find { |cli| node_key == to_node_key(cli) }
    end

    def to_node_key(client)
      "#{client.config.host}:#{client.config.port}"
    end
  end
end
