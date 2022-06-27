# frozen_string_literal: true

require 'redis_client'

class ClusterController
  SLOT_SIZE = 16_384
  DEFAULT_SHARD_SIZE = 3
  DEFAULT_REPLICA_SIZE = 1
  DEFAULT_MAX_ATTEMPTS = 600
  DEFAULT_TIMEOUT_SEC = 5.0

  def initialize(node_addrs,
                 shard_size: DEFAULT_SHARD_SIZE,
                 replica_size: DEFAULT_REPLICA_SIZE,
                 state_check_attempts: DEFAULT_MAX_ATTEMPTS,
                 **kwargs)

    raise "Redis Cluster requires at least #{shard_size} master nodes." if node_addrs.size < shard_size

    @shard_size = shard_size
    @replica_size = replica_size
    @number_of_replicas = @replica_size * @shard_size
    @max_attempts = state_check_attempts
    @timeout = kwargs.fetch(:timeout, DEFAULT_TIMEOUT_SEC)
    @kwargs = kwargs.merge(timeout: @timeout)
    @clients = node_addrs.map { |addr| ::RedisClient.new(url: addr, **@kwargs) }
  end

  def wait_for_cluster_to_be_ready
    wait_meeting(@clients, max_attempts: @max_attempts)
    wait_cluster_building(@clients, max_attempts: @max_attempts)
    wait_replication(@clients, number_of_replicas: @number_of_replicas, max_attempts: @max_attempts)
    wait_cluster_recovering(@clients, max_attempts: @max_attempts)
  end

  def rebuild
    flush_all_data(@clients)
    reset_cluster(@clients)
    assign_slots(@clients, shard_size: @shard_size)
    save_config_epoch(@clients)
    meet_each_other(@clients)
    wait_meeting(@clients, max_attempts: @max_attempts)
    replicate(@clients, shard_size: @shard_size, replica_size: @replica_size)
    save_config(@clients)
    wait_cluster_building(@clients, max_attempts: @max_attempts)
    wait_replication(@clients, number_of_replicas: @number_of_replicas, max_attempts: @max_attempts)
    wait_cluster_recovering(@clients, max_attempts: @max_attempts)
  end

  def down
    flush_all_data(@clients)
    reset_cluster(@clients)
  end

  def failover
    master, slave = take_a_replication_pair(@clients)
    wait_replication_delay(@clients, replica_size: @replica_size, timeout: @timeout)
    slave.call('CLUSTER', 'FAILOVER', 'TAKEOVER')
    wait_failover(@clients, master_key: to_node_key(master), slave_key: to_node_key(slave), max_attempts: @max_attempts)
    wait_replication_delay(@clients, replica_size: @replica_size, timeout: @timeout)
    wait_cluster_recovering(@clients, max_attempts: @max_attempts)
  end

  def start_resharding(slot:, src_node_key:, dest_node_key:)
    src_node_id = fetch_internal_id_by_natted_node_key(@clients.first, src_node_key)
    src_client = find_client_by_natted_node_key(@clients, src_node_key)
    dest_node_id = fetch_internal_id_by_natted_node_key(@clients.first, dest_node_key)
    dest_client = find_client_by_natted_node_key(@clients, dest_node_key)
    dest_host, dest_port = dest_node_key.split(':')

    src_client.call('CLUSTER', 'SETSLOT', slot, 'MIGRATING', dest_node_id)
    dest_client.call('CLUSTER', 'SETSLOT', slot, 'IMPORTING', src_node_id)

    db_idx = '0'
    timeout_msec = @timeout.to_i * 1000

    number_of_keys = src_client.call('CLUSTER', 'COUNTKEYSINSLOT', slot)
    keys = src_client.call('CLUSTER', 'GETKEYSINSLOT', slot, number_of_keys)
    return if keys.empty?

    begin
      src_client.call('MIGRATE', dest_host, dest_port, '', db_idx, timeout_msec, 'KEYS', *keys)
    rescue ::RedisClient::CommandError => e
      raise unless e.message.start_with?('IOERR')

      # retry once
      src_client.call('MIGRATE', dest_host, dest_port, '', db_idx, timeout_msec, 'REPLACE', 'KEYS', *keys)
    end

    wait_replication_delay(@clients, replica_size: @replica_size, timeout: @timeout)
  end

  def finish_resharding(slot:, dest_node_key:)
    id = fetch_internal_id_by_natted_node_key(@clients.first, dest_node_key)
    client = find_client_by_natted_node_key(@clients, dest_node_key)
    client.call('CLUSTER', 'SETSLOT', slot, 'NODE', id)
  end

  def scale_out(primary_url:, replica_url:) # rubocop:disable Metrics/CyclomaticComplexity
    # @see https://redis.io/docs/manual/scaling/
    rows = fetch_and_parse_cluster_nodes(@clients)
    target_host, target_port = rows.find { |row| row[:role] == 'master' }.fetch(:node_key).split(':')

    primary = ::RedisClient.new(url: primary_url, **@kwargs)
    replica = ::RedisClient.new(url: replica_url, **@kwargs)

    @clients << primary
    @clients << replica
    @shard_size += 1
    @number_of_replicas = @replica_size * @shard_size

    primary.call('CLUSTER', 'MEET', target_host, target_port)
    replica.call('CLUSTER', 'MEET', target_host, target_port)
    wait_meeting(@clients, max_attempts: @max_attempts)

    primary_id = primary.call('CLUSTER', 'MYID')
    replica.call('CLUSTER', 'REPLICATE', primary_id)
    save_config(@clients)
    wait_replication(@clients, number_of_replicas: @number_of_replicas, max_attempts: @max_attempts)

    rows = fetch_and_parse_cluster_nodes(@clients)

    SLOT_SIZE.times.to_a.sample(SLOT_SIZE / @shard_size).each do |slot|
      src = rows.find do |row|
        next if row[:slots].empty?

        row[:slots].any? { |first, last| first <= slot && slot <= last }
      end.fetch(:node_key)
      dest = rows.find { |row| row[:id] == primary_id }.fetch(:node_key)
      start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      finish_resharding(slot: slot, dest_node_key: dest)
    end
  end

  def close
    @clients.each(&:close)
  end

  private

  def flush_all_data(clients)
    clients.each do |c|
      c.call('FLUSHALL')
    rescue ::RedisClient::CommandError
      # READONLY You can't write against a read only slave.
      nil
    end
  end

  def reset_cluster(clients)
    clients.each { |c| c.call('CLUSTER', 'RESET', 'HARD') }
  end

  def assign_slots(clients, shard_size:)
    masters = take_masters(clients, shard_size: shard_size)
    slot_slice = SLOT_SIZE / masters.size
    mod = SLOT_SIZE % masters.size
    slot_sizes = Array.new(masters.size, slot_slice)
    mod.downto(1) { |i| slot_sizes[i] += 1 }

    slot_idx = 0
    masters.zip(slot_sizes).each do |c, s|
      slot_range = slot_idx..slot_idx + s - 1
      c.call('CLUSTER', 'ADDSLOTS', *slot_range.to_a)
      slot_idx += s
    end
  end

  def save_config_epoch(clients)
    clients.each_with_index do |c, i|
      c.call('CLUSTER', 'SET-CONFIG-EPOCH', i + 1)
    rescue ::RedisClient::CommandError
      # ERR Node config epoch is already non-zero
      nil
    end
  end

  def meet_each_other(clients)
    target_host, target_port = fetch_cluster_nodes(clients.first).first[1].split('@').first.split(':')
    clients.drop(1).each { |c| c.call('CLUSTER', 'MEET', target_host, target_port) }
  end

  def wait_meeting(clients, max_attempts:)
    wait_for_state(clients, max_attempts: max_attempts) do |client|
      info = hashify_cluster_info(client)
      info['cluster_known_nodes'].to_s == clients.size.to_s
    rescue ::RedisClient::ConnectionError
      true
    end
  end

  def replicate(clients, shard_size:, replica_size:)
    node_map = hashify_node_map(clients)
    masters = take_masters(clients, shard_size: shard_size)

    take_slaves(clients, shard_size: shard_size).each_slice(replica_size).each_with_index do |slaves, i|
      master_host = masters[i].config.host
      master_port = masters[i].config.port

      loop do
        begin
          master_node_id = node_map.fetch(to_node_key_by_host_port(master_host, master_port))
          slaves.each { |slave| slave.call('CLUSTER', 'REPLICATE', master_node_id) }
        rescue ::RedisClient::CommandError
          # ERR Unknown node [key]
          sleep 0.1
          node_map = hashify_node_map(clients)
          next
        end

        break
      end
    end
  end

  def save_config(clients)
    clients.each { |c| c.call('CLUSTER', 'SAVECONFIG') }
  end

  def wait_cluster_building(clients, max_attempts:)
    wait_for_state(clients, max_attempts: max_attempts) do |client|
      info = hashify_cluster_info(client)
      info['cluster_state'] == 'ok'
    rescue ::RedisClient::ConnectionError
      true
    end
  end

  def wait_replication(clients, number_of_replicas:, max_attempts:)
    wait_for_state(clients, max_attempts: max_attempts) do |client|
      flags = hashify_cluster_node_flags(clients, client: client)
      flags.values.count { |f| f == 'slave' } == number_of_replicas
    rescue ::RedisClient::ConnectionError
      true
    end
  end

  def wait_failover(clients, master_key:, slave_key:, max_attempts:)
    wait_for_state(clients, max_attempts: max_attempts) do |client|
      flags = hashify_cluster_node_flags(clients, client: client)
      flags[master_key] == 'slave' && flags[slave_key] == 'master'
    rescue ::RedisClient::ConnectionError
      true
    end
  end

  def wait_replication_delay(clients, replica_size:, timeout:)
    timeout_msec = timeout.to_i * 1000
    wait_for_state(clients, max_attempts: clients.size + 1) do |client|
      client.blocking_call(timeout, 'WAIT', replica_size, timeout_msec - 100) if client.call('ROLE').first == 'master'
      true
    rescue ::RedisClient::ConnectionError
      true
    end
  end

  def wait_cluster_recovering(clients, max_attempts:)
    key = 0
    wait_for_state(clients, max_attempts: max_attempts) do |client|
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
    rescue ::RedisClient::ConnectionError
      true
    end
  end

  def wait_for_state(clients, max_attempts:)
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
    client.call('CLUSTER', 'INFO').split("\r\n").to_h { |v| v.split(':') }
  end

  def hashify_cluster_node_flags(clients, client: nil)
    id2key = fetch_internal_id_to_node_key_mappings(clients)
    fetch_cluster_nodes(client || clients.first)
      .to_h { |arr| [id2key[arr[0]], (arr[2].split(',') & %w[master slave]).first] }
  end

  def hashify_node_map(clients)
    id2key = fetch_internal_id_to_node_key_mappings(clients)
    clients.each do |client|
      return fetch_cluster_nodes(client).to_h { |arr| [id2key[arr[0]], arr[0]] }
    rescue ::RedisClient::ConnectionError
      next
    end
  end

  def fetch_internal_id_by_natted_node_key(client, node_key)
    fetch_cluster_nodes(client).find { |info| info[1].split('@').first == node_key }.first
  end

  def find_client_by_natted_node_key(clients, node_key)
    id = fetch_internal_id_by_natted_node_key(clients.first, node_key)
    id2key = fetch_internal_id_to_node_key_mappings(clients)
    key = id2key[id]
    clients.find { |cli| key == to_node_key(cli) }
  end

  def fetch_cluster_nodes(client)
    client.call('CLUSTER', 'NODES').split("\n").map(&:split)
  end

  def fetch_internal_id_to_node_key_mappings(clients)
    fetch_internal_id_to_client_mappings(clients).transform_values { |c| to_node_key(c) }
  end

  def fetch_internal_id_to_client_mappings(clients)
    clients.to_h { |c| [c.call('CLUSTER', 'MYID'), c] }
  end

  def fetch_and_parse_cluster_nodes(clients) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    rows = fetch_cluster_nodes(clients.first)
    rows.each { |arr| arr[2] = arr[2].split(',') }
    rows.select! { |arr| arr[7] == 'connected' && (arr[2] & %w[fail? fail handshake noaddr noflags]).empty? }
    rows.each do |arr|
      arr[1] = arr[1].split('@').first
      arr[2] = (arr[2] & %w[master slave]).first
      if arr[8].nil?
        arr[8] = []
        next
      end

      arr[8] = arr[8..].map { |r| r.split('-').map { |s| Integer(s) } }
                       .map { |a| a.size == 1 ? a << a.first : a }
                       .map(&:sort)
    end

    rows.map do |arr|
      { id: arr[0], node_key: arr[1], role: arr[2], primary_id: arr[3], ping_sent: arr[4],
        pong_recv: arr[5], config_epoch: arr[6], link_state: arr[7], slots: arr[8] }
    end
  end

  def take_masters(clients, shard_size:)
    clients.select { |cli| cli.call('ROLE').first == 'master' }.take(shard_size)
  end

  def take_slaves(clients, shard_size:)
    replicas = clients.select { |cli| cli.call('ROLE').first == 'slave' }
    replicas.size.zero? ? clients[shard_size..] : replicas
  end

  def take_a_replication_pair(clients)
    rows = fetch_and_parse_cluster_nodes(clients)
    primary = rows.find { |row| row[:role] == 'master' }
    replica = rows.find { |row| row[:primary_id] == primary[:id] }
    id2cli = fetch_internal_id_to_client_mappings(clients)
    [id2cli[primary[:id]], id2cli[replica[:id]]]
  end

  def to_node_key(client)
    to_node_key_by_host_port(client.config.host, client.config.port)
  end

  def to_node_key_by_host_port(host, port)
    "#{host}:#{port}"
  end
end
