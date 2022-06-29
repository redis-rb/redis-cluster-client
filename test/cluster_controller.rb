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
    rows = associate_with_clients_and_nodes(@clients)
    primary_info = rows.find { |row| row[:role] == 'master' }
    replica_info = rows.find { |row| row[:primary_id] == primary_info[:id] }

    wait_replication_delay(@clients, replica_size: @replica_size, timeout: @timeout)
    replica_info.fetch(:client).call('CLUSTER', 'FAILOVER', 'TAKEOVER')
    wait_failover(
      @clients,
      primary_node_key: primary_info.fetch(:node_key),
      replica_node_key: replica_info.fetch(:node_key),
      max_attempts: @max_attempts
    )
    wait_replication_delay(@clients, replica_size: @replica_size, timeout: @timeout)
    wait_cluster_recovering(@clients, max_attempts: @max_attempts)
  end

  def start_resharding(slot:, src_node_key:, dest_node_key:) # rubocop:disable Metrics/CyclomaticComplexity
    rows = associate_with_clients_and_nodes(@clients)
    src_info = rows.find { |r| r[:node_key] == src_node_key || r[:client_node_key] == src_node_key }
    dest_info = rows.find { |r| r[:node_key] == dest_node_key || r[:client_node_key] == dest_node_key }

    src_node_id = src_info.fetch(:id)
    src_client = src_info.fetch(:client)
    dest_node_id = dest_info.fetch(:id)
    dest_client = dest_info.fetch(:client)
    dest_host, dest_port = dest_info.fetch(:node_key).split(':')

    # @see https://redis.io/commands/cluster-setslot/#redis-cluster-live-resharding-explained
    dest_client.call('CLUSTER', 'SETSLOT', slot, 'IMPORTING', src_node_id)
    src_client.call('CLUSTER', 'SETSLOT', slot, 'MIGRATING', dest_node_id)

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

  def finish_resharding(slot:, src_node_key:, dest_node_key:) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    rows = associate_with_clients_and_nodes(@clients)
    src_info = rows.find { |r| r[:node_key] == src_node_key || r[:client_node_key] == src_node_key }
    dest_info = rows.find { |r| r[:node_key] == dest_node_key || r[:client_node_key] == dest_node_key }

    src = src_info.fetch(:client)
    dest = dest_info.fetch(:client)
    id = dest_info.fetch(:id)
    rest = rows.reject { |r| r[:role] == 'slave' || r[:client].equal?(src) || r[:client].equal?(dest) }.map { |r| r[:client] }

    ([dest, src] + rest).each do |cli|
      cli.call('CLUSTER', 'SETSLOT', slot, 'NODE', id)
    rescue ::RedisClient::CommandError => e
      raise if e.message != 'ERR Please use SETSLOT only with masters.'
      # how weird, ignore
    end
  end

  def scale_out(primary_url:, replica_url:)
    # @see https://redis.io/docs/manual/scaling/
    rows = associate_with_clients_and_nodes(@clients)
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
    wait_for_cluster_to_be_ready

    rows = associate_with_clients_and_nodes(@clients)

    SLOT_SIZE.times.to_a.sample(100).sort.each do |slot|
      src = rows.find { |row| row[:slots].include?(slot) }.fetch(:node_key)
      dest = rows.find { |row| row[:id] == primary_id }.fetch(:node_key)
      start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      finish_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
    end
  end

  def scale_in # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    rows = associate_with_clients_and_nodes(@clients)

    primary_info = rows.reject { |r| r[:slots].empty? }.min_by { |r| r[:slots].size }
    replica_info = rows.find { |r| r[:primary_id] == primary_info[:id] }
    rest_primary_node_keys = rows.reject { |r| r[:id] == primary_info[:id] || r[:role] == 'slave' }.map { |r| r[:node_key] }

    primary_info[:slots].each do |slot|
      src = primary_info.fetch(:node_key)
      dest = rest_primary_node_keys.sample
      start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      finish_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
    end

    replica = replica_info.fetch(:client)
    primary = primary_info.fetch(:client)
    threads = @clients.map do |cli|
      Thread.new(cli) do |c|
        Thread.pass
        c.pipelined do |pi|
          pi.call('CLUSTER', 'FORGET', replica_info[:id])
          pi.call('CLUSTER', 'FORGET', primary_info[:id])
        end
      rescue ::RedisClient::Error
        # ignore
      end
    end
    threads.each(&:join)
    replica.call('CLUSTER', 'RESET', 'SOFT')
    primary.call('CLUSTER', 'RESET', 'SOFT')
    @clients.reject! { |c| c.equal?(primary) || c.equal?(replica) }
    @shard_size -= 1
    @number_of_replicas = @replica_size * @shard_size

    wait_for_cluster_to_be_ready
    wait_for_state(@clients, max_attempts: @max_attempts) do |client|
      fetch_cluster_nodes(client).size == @shard_size + @number_of_replicas
    rescue ::RedisClient::ConnectionError
      true
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
      # READONLY You can't write against a read only replica.
      nil
    end
  end

  def reset_cluster(clients)
    clients.each { |c| c.call('CLUSTER', 'RESET', 'HARD') }
  end

  def assign_slots(clients, shard_size:)
    primaries = take_primaries(clients, shard_size: shard_size)
    slot_slice = SLOT_SIZE / primaries.size
    mod = SLOT_SIZE % primaries.size
    slot_sizes = Array.new(primaries.size, slot_slice)
    mod.downto(1) { |i| slot_sizes[i] += 1 }

    slot_idx = 0
    primaries.zip(slot_sizes).each do |c, s|
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
    rows = fetch_cluster_nodes(clients.first)
    rows = parse_cluster_nodes(rows)
    target_host, target_port = rows.first.fetch(:node_key).split(':')
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
    primaries = take_primaries(clients, shard_size: shard_size)
    replicas = take_replicas(clients, shard_size: shard_size)

    replicas.each_slice(replica_size).each_with_index do |subset, i|
      primary_id = primaries[i].call('CLUSTER', 'MYID')

      loop do
        begin
          subset.each { |replica| replica.call('CLUSTER', 'REPLICATE', primary_id) }
        rescue ::RedisClient::CommandError
          # ERR Unknown node [key]
          sleep 0.1
          primary_id = primaries[i].call('CLUSTER', 'MYID')
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
      rows = fetch_cluster_nodes(client)
      rows = parse_cluster_nodes(rows)
      rows.count { |r| r[:role] == 'slave' } == number_of_replicas
    rescue ::RedisClient::ConnectionError
      true
    end
  end

  def wait_failover(clients, primary_node_key:, replica_node_key:, max_attempts:)
    wait_for_state(clients, max_attempts: max_attempts) do |client|
      rows = fetch_cluster_nodes(client)
      rows = parse_cluster_nodes(rows)
      primary_info = rows.find { |r| r[:node_key] == primary_node_key || r[:client_node_key] == primary_node_key }
      replica_info = rows.find { |r| r[:node_key] == replica_node_key || r[:client_node_key] == replica_node_key }
      primary_info[:role] == 'slave' && replica_info[:role] == 'master'
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

  def fetch_cluster_nodes(client)
    client.call('CLUSTER', 'NODES').split("\n").map(&:split)
  end

  def associate_with_clients_and_nodes(clients)
    clients.flat_map do |client|
      rows = fetch_cluster_nodes(client)
      rows = parse_cluster_nodes(rows)
      row = rows.find { |r| r[:flags].include?('myself') }
      row.merge(client: client, client_node_key: "#{client.config.host}:#{client.config.port}")
    rescue ::RedisClient::ConnectionError
      next
    end
  end

  def parse_cluster_nodes(rows) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    rows.map do |row|
      flags = row[2].split(',')
      slots = if row[8].nil?
                []
              else
                row[8..].filter_map { |str| str.start_with?('[') ? nil : str.split('-').map { |s| Integer(s) } }
                        .map { |a| a.size == 1 ? a << a.first : a }.map(&:sort)
                        .flat_map { |first, last| (first..last).to_a }.sort
              end

      {
        id: row[0],
        node_key: row[1].split('@').first,
        flags: flags,
        role: (flags & %w[master slave]).first,
        primary_id: row[3],
        ping_sent: row[4],
        pong_recv: row[5],
        config_epoch: row[6],
        link_state: row[7],
        slots: slots
      }
    end
  end

  def take_primaries(clients, shard_size:)
    clients.select { |cli| cli.call('ROLE').first == 'master' }.take(shard_size)
  end

  def take_replicas(clients, shard_size:)
    replicas = clients.select { |cli| cli.call('ROLE').first == 'slave' }
    replicas.size.zero? ? clients[shard_size..] : replicas
  end
end
