# frozen_string_literal: true

require 'uri'
require 'testing_helper'

class RedisClient
  class Cluster
    class Node
      class TestConfig < TestingWrapper
        def test_connection_prelude
          [
            { params: { scale_read: true }, want: [%w[HELLO 3], %w[READONLY]] },
            { params: { scale_read: false }, want: [%w[HELLO 3]] },
            { params: {}, want: [%w[HELLO 3]] }
          ].each_with_index do |c, idx|
            got = ::RedisClient::Cluster::Node::Config.new(**c[:params]).connection_prelude
            assert_equal(c[:want], got, "Case: #{idx}")
          end
        end
      end
    end

    class TestNode < TestingWrapper
      def setup
        @test_config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          **TEST_GENERIC_OPTIONS
        )
        @test_node_info = ::RedisClient::Cluster::Node.load_info(@test_config.per_node_key)
        if TEST_FIXED_HOSTNAME
          @test_node_info.each do |info|
            _, port = ::RedisClient::Cluster::NodeKey.split(info[:node_key])
            info[:node_key] = ::RedisClient::Cluster::NodeKey.build_from_host_port(TEST_FIXED_HOSTNAME, port)
          end
        end
        node_addrs = @test_node_info.map { |info| ::RedisClient::Cluster::NodeKey.hashify(info[:node_key]) }
        @test_config.update_node(node_addrs)
        @test_node = ::RedisClient::Cluster::Node.new(
          @test_config.per_node_key,
          node_info: @test_node_info
        )
        @test_node_with_scale_read = ::RedisClient::Cluster::Node.new(
          @test_config.per_node_key,
          node_info: @test_node_info,
          with_replica: true
        )
      end

      def teardown
        @test_node&.each(&:close)
        @test_node_with_scale_read&.each(&:close)
      end

      def test_load_info
        [
          {
            params: { options: TEST_NODE_OPTIONS, kwargs: TEST_GENERIC_OPTIONS },
            want: { size: TEST_NODE_OPTIONS.size }
          },
          {
            params: { options: { '127.0.0.1:11211' => { host: '127.0.0.1', port: 11_211 } }, kwargs: TEST_GENERIC_OPTIONS },
            want: { error: ::RedisClient::Cluster::InitialSetupError }
          },
          {
            params: { options: {}, kwargs: TEST_GENERIC_OPTIONS },
            want: { error: ::RedisClient::Cluster::InitialSetupError }
          }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          got = -> { ::RedisClient::Cluster::Node.load_info(c[:params][:options], **c[:params][:kwargs]) }
          if c[:want].key?(:error)
            assert_raises(c[:want][:error], msg, &got)
          else
            assert_equal(c[:want][:size], got.call.size, msg)
          end
        end
      end

      def test_parse_node_info
        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 connected 5461-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 connected 10923-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-5460
        INFO

        want = [
          { id: '07c37dfeb235213a872192d90877d0cd55635b91', node_key: '127.0.0.1:30004', role: 'slave',
            primary_id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', ping_sent: '0', pong_recv: '1426238317239',
            config_epoch: '4', link_state: 'connected', slots: [] },
          { id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', node_key: '127.0.0.1:30002', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '2', link_state: 'connected', slots: [[5461, 10_922]] },
          { id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', node_key: '127.0.0.1:30003', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238318243',
            config_epoch: '3', link_state: 'connected', slots: [[10_923, 16_383]] },
          { id: '6ec23923021cf3ffec47632106199cb7f496ce01', node_key: '127.0.0.1:30005', role: 'slave',
            primary_id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '5', link_state: 'connected', slots: [] },
          { id: '824fe116063bc5fcf9f4ffd895bc17aee7731ac3', node_key: '127.0.0.1:30006', role: 'slave',
            primary_id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', ping_sent: '0', pong_recv: '1426238317741',
            config_epoch: '6', link_state: 'connected', slots: [] },
          { id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', node_key: '127.0.0.1:30001', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '0', config_epoch: '1', link_state: 'connected', slots: [[0, 5460]] }
        ]

        assert_equal(want, ::RedisClient::Cluster::Node.send(:parse_node_info, info), 'Case: success: continuous slots')

        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 connected 3001 5461-7000 7002-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 connected 7001 10923-15000 15002-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-3000 3002-5460 15001
        INFO

        want = [
          { id: '07c37dfeb235213a872192d90877d0cd55635b91', node_key: '127.0.0.1:30004', role: 'slave',
            primary_id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', ping_sent: '0', pong_recv: '1426238317239',
            config_epoch: '4', link_state: 'connected', slots: [] },
          { id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', node_key: '127.0.0.1:30002', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '2', link_state: 'connected', slots: [[3001, 3001], [5461, 7000], [7002, 10_922]] },
          { id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', node_key: '127.0.0.1:30003', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238318243',
            config_epoch: '3', link_state: 'connected', slots: [[7001, 7001], [10_923, 15_000], [15_002, 16_383]] },
          { id: '6ec23923021cf3ffec47632106199cb7f496ce01', node_key: '127.0.0.1:30005', role: 'slave',
            primary_id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '5', link_state: 'connected', slots: [] },
          { id: '824fe116063bc5fcf9f4ffd895bc17aee7731ac3', node_key: '127.0.0.1:30006', role: 'slave',
            primary_id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', ping_sent: '0', pong_recv: '1426238317741',
            config_epoch: '6', link_state: 'connected', slots: [] },
          { id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', node_key: '127.0.0.1:30001', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '0', config_epoch: '1', link_state: 'connected', slots: [[0, 3000], [3002, 5460], [15_001, 15_001]] }
        ]

        assert_equal(want, ::RedisClient::Cluster::Node.send(:parse_node_info, info), 'Case: success: discrete slots')

        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 connected 3001 5461-7000 7002-10922 [5462->-292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f]
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 connected 7001 10923-15000 15002-16383 [5462-<-67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1]
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-3000 3002-5460 15001
        INFO

        want = [
          { id: '07c37dfeb235213a872192d90877d0cd55635b91', node_key: '127.0.0.1:30004', role: 'slave',
            primary_id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', ping_sent: '0', pong_recv: '1426238317239',
            config_epoch: '4', link_state: 'connected', slots: [] },
          { id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', node_key: '127.0.0.1:30002', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '2', link_state: 'connected', slots: [[3001, 3001], [5461, 7000], [7002, 10_922]] },
          { id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', node_key: '127.0.0.1:30003', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238318243',
            config_epoch: '3', link_state: 'connected', slots: [[7001, 7001], [10_923, 15_000], [15_002, 16_383]] },
          { id: '6ec23923021cf3ffec47632106199cb7f496ce01', node_key: '127.0.0.1:30005', role: 'slave',
            primary_id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '5', link_state: 'connected', slots: [] },
          { id: '824fe116063bc5fcf9f4ffd895bc17aee7731ac3', node_key: '127.0.0.1:30006', role: 'slave',
            primary_id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', ping_sent: '0', pong_recv: '1426238317741',
            config_epoch: '6', link_state: 'connected', slots: [] },
          { id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', node_key: '127.0.0.1:30001', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '0', config_epoch: '1', link_state: 'connected', slots: [[0, 3000], [3002, 5460], [15_001, 15_001]] }
        ]

        assert_equal(want, ::RedisClient::Cluster::Node.send(:parse_node_info, info), 'Case: success: discrete slots and resharding')

        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 disconnected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 disconnected 5461-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 disconnected 10923-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 disconnected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 disconnected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 disconnected 0-5460
        INFO

        assert_empty(::RedisClient::Cluster::Node.send(:parse_node_info, info), 'Case: failure: link state')

        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 fail?,slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 fail,master - 0 1426238316232 2 connected 5461-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master,handshake - 0 1426238318243 3 connected 10923-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 noaddr,slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 noflags 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,fail,master - 0 0 1 connected 0-5460
        INFO

        assert_empty(::RedisClient::Cluster::Node.send(:parse_node_info, info), 'Case: failure: flags')
      end

      def test_inspect
        assert_match(/^#<RedisClient::Cluster::Node [0-9., :]*>$/, @test_node.inspect)
      end

      def test_enumerable
        refute(@test_node.any?(&:nil?))
      end

      def test_node_keys
        want = @test_node_info.map { |info| info[:node_key] }
        @test_node.node_keys.each do |got|
          assert_includes(want, got, "Case: #{got}")
        end
      end

      def test_primary_node_keys
        want = @test_node_info.filter_map { |info| info[:role] == 'master' ? info[:node_key] : nil }.sort
        got = @test_node.primary_node_keys
        assert_equal(want, got)
      end

      def test_replica_node_keys
        want = @test_node_info.filter_map { |info| info[:role] == 'master' ? info[:node_key] : nil }.sort
        got = @test_node.replica_node_keys
        assert_equal(want, got)

        want = @test_node_info.filter_map { |info| info[:role] == 'slave' ? info[:node_key] : nil }.sort
        got = @test_node_with_scale_read.replica_node_keys
        assert_equal(want, got)
      end

      def test_find_by
        @test_node_info.each do |info|
          msg = "Case: primary only: #{info[:node_key]}"
          got = -> { @test_node.find_by(info[:node_key]) }
          if info[:role] == 'master'
            assert_instance_of(::RedisClient, got.call, msg)
          else
            assert_raises(::RedisClient::Cluster::Node::ReloadNeeded, msg, &got)
          end

          msg = "Case: scale read: #{info[:node_key]}"
          got = @test_node_with_scale_read.find_by(info[:node_key])
          assert_instance_of(::RedisClient, got, msg)
        end
      end

      def test_call_all
        want = (1..(@test_node_info.count { |info| info[:role] == 'master' })).map { |_| 'PONG' }
        got = @test_node.call_all(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: primary only')

        want = (1..(@test_node_info.count)).map { |_| 'PONG' }
        got = @test_node_with_scale_read.call_all(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: scale read')
      end

      def test_call_primaries
        want = (1..(@test_node_info.count { |info| info[:role] == 'master' })).map { |_| 'PONG' }
        got = @test_node.call_primaries(:call_v, ['PING'], [])
        assert_equal(want, got)
      end

      def test_call_replicas
        want = (1..(@test_node_info.count { |info| info[:role] == 'master' })).map { |_| 'PONG' }

        got = @test_node.call_replicas(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: primary only')

        got = @test_node_with_scale_read.call_replicas(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: scale read')
      end

      def test_clients_for_scanning # rubocop:disable Metrics/CyclomaticComplexity
        want = @test_node_info.select { |info| info[:role] == 'master' }.map { |info| info[:node_key] }.sort
        got = @test_node.clients_for_scanning.map { |client| "#{client.config.host}:#{client.config.port}" }
        assert_equal(want, got, 'Case: primary only')

        want = @test_node_info.select { |info| info[:role] == 'slave' }.map { |info| info[:node_key] }
        got = @test_node_with_scale_read.clients_for_scanning.map { |client| "#{client.config.host}:#{client.config.port}" }
        got.each { |e| assert_includes(want, e, 'Case: scale read') }
      end

      def test_find_node_key_of_primary
        sample_node = @test_node_info.find { |info| info[:role] == 'master' }
        sample_slot = sample_node[:slots].first.first
        got = @test_node.find_node_key_of_primary(sample_slot)
        assert_equal(sample_node[:node_key], got, 'Case: sample slot')
        assert_nil(@test_node.find_node_key_of_primary(nil), 'Case: nil')
      end

      def test_find_node_key_of_replica
        sample_node = @test_node_info.find { |info| info[:role] == 'master' }
        sample_slot = sample_node[:slots].first.first
        got = @test_node.find_node_key_of_replica(sample_slot)
        assert_equal(sample_node[:node_key], got, 'Case: primary only')

        sample_replicas = @test_node_info.select { |info| info[:role] == 'slave' }
        sample_primary = @test_node_info.find { |info| info[:id] == sample_replicas.first[:primary_id] }
        sample_slot = sample_primary[:slots].first.first
        got = @test_node_with_scale_read.find_node_key_of_replica(sample_slot)
        want = sample_replicas.map { |info| info[:node_key] }
        assert_includes(want, got, 'Case: scale read')

        assert_nil(@test_node.find_node_key_of_replica(nil), 'Case: nil')
      end

      def test_update_slot
        sample_slot = 0
        base_node_key = @test_node.find_node_key_of_primary(sample_slot)
        another_node_key = @test_node_info.find { |info| info[:node_key] != base_node_key && info[:role] == 'master' }
        @test_node.update_slot(sample_slot, another_node_key)
        assert_equal(another_node_key, @test_node.find_node_key_of_primary(sample_slot))
      end

      def test_make_topology_class
        [
          { with_replica: false, replica_affinity: :foo, want: ::RedisClient::Cluster::Node::PrimaryOnly },
          { with_replica: true, replica_affinity: :foo, want: ::RedisClient::Cluster::Node::PrimaryOnly },
          { with_replica: true, replica_affinity: :random, want: ::RedisClient::Cluster::Node::RandomReplica },
          { with_replica: true, replica_affinity: :latency, want: ::RedisClient::Cluster::Node::LatencyReplica }
        ].each_with_index do |c, i|
          got = @test_node.send(:make_topology_class, c[:with_replica], c[:replica_affinity])
          assert_equal(c[:want], got, "Case: #{i}")
        end
      end

      def test_build_slot_node_mappings
        node_info = [
          { node_key: '127.0.0.1:7001', slots: [[0, 3000], [3002, 5460], [15_001, 15_001]] },
          { node_key: '127.0.0.1:7002', slots: [[3001, 3001], [5461, 7000], [7002, 10_922]] },
          { node_key: '127.0.0.1:7003', slots: [[7001, 7001], [10_923, 15_000], [15_002, 16_383]] },
          { node_key: '127.0.0.1:7004', slots: [] },
          { node_key: '127.0.0.1:7005', slots: [] },
          { node_key: '127.0.0.1:7006', slots: [] }
        ]
        got = @test_node.send(:build_slot_node_mappings, node_info)
        node_info.each do |info|
          next if info[:slots].empty?

          info[:slots].each do |range|
            (range[0]..range[1]).each { |slot| assert_same(info[:node_key], got[slot], "Case: #{slot}") }
          end
        end
      end

      def test_build_replication_mappings
        node_key1 = '127.0.0.1:7001'
        node_key2 = '127.0.0.1:7002'
        node_key3 = '127.0.0.1:7003'
        node_key4 = '127.0.0.1:7004'
        node_key5 = '127.0.0.1:7005'
        node_key6 = '127.0.0.1:7006'
        node_key7 = '127.0.0.1:7007'
        node_key8 = '127.0.0.1:7008'
        node_key9 = '127.0.0.1:7009'

        node_info = [
          { id: '1', node_key: node_key1, primary_id: '-' },
          { id: '2', node_key: node_key2, primary_id: '-' },
          { id: '3', node_key: node_key3, primary_id: '-' },
          { id: '4', node_key: node_key4, primary_id: '1' },
          { id: '5', node_key: node_key5, primary_id: '2' },
          { id: '6', node_key: node_key6, primary_id: '3' },
          { id: '7', node_key: node_key7, primary_id: '1' },
          { id: '8', node_key: node_key8, primary_id: '2' },
          { id: '9', node_key: node_key9, primary_id: '3' }
        ]
        got = @test_node.send(:build_replication_mappings, node_info)
        got.transform_values!(&:sort!)
        assert_same(node_key4, got[node_key1][0], 'Case: regular')
        assert_same(node_key7, got[node_key1][1], 'Case: regular')
        assert_same(node_key5, got[node_key2][0], 'Case: regular')
        assert_same(node_key8, got[node_key2][1], 'Case: regular')
        assert_same(node_key6, got[node_key3][0], 'Case: regular')
        assert_same(node_key9, got[node_key3][1], 'Case: regular')

        node_info = [
          { id: '1', role: 'master', node_key: node_key1, primary_id: '-' },
          { id: '3', role: 'master', node_key: node_key3, primary_id: '-' },
          { id: '4', role: 'slave', node_key: node_key4, primary_id: '1' },
          { id: '5', role: 'master', node_key: node_key5, primary_id: '-' },
          { id: '6', role: 'slave', node_key: node_key6, primary_id: '3' }
        ]
        got = @test_node.send(:build_replication_mappings, node_info)
        got.transform_values!(&:sort!)
        assert_equal(3, got.size, 'Case: lack of replica')
        assert_same(node_key4, got[node_key1][0], 'Case: lack of replica')
        assert_same(node_key6, got[node_key3][0], 'Case: lack of replica')
        assert_empty(got[node_key5], 'Case: lack of replica')
      end

      def test_try_map
        primary_node_keys = @test_node_info.select { |info| info[:role] == 'master' }.map { |info| info[:node_key] }
        [
          { block: ->(_, client) { client.call('PING') }, results: primary_node_keys.to_h { |k| [k, 'PONG'] } },
          { block: ->(_, client) { client.call('UNKNOWN') }, errors: ::RedisClient::CommandError }
        ].each_with_index do |c, idx|
          msg = "Case: #{idx}"
          clients = @test_node.instance_variable_get(:@topology).clients
          results, errors = @test_node.send(:try_map, clients, &c[:block])
          if c.key?(:errors)
            errors.each_value { |e| assert_instance_of(c[:errors], e, msg) }
          else
            assert_equal(c[:results], results, msg)
          end
        end
      end
    end
  end
end
