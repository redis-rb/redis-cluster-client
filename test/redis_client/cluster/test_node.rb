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

    # rubocop:disable Metrics/ClassLength
    class TestNode < TestingWrapper
      def setup
        @test_node = make_node.tap(&:reload!)
        @test_node_with_scale_read = make_node(replica: true).tap(&:reload!)
        @test_node_info_list = @test_node.instance_variable_get(:@node_info)
      end

      def teardown
        @test_nodes&.each { |n| n&.each(&:close) }
      end

      def make_node(capture_buffer: [], pool: nil, **kwargs)
        config = ::RedisClient::ClusterConfig.new(**{
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: [CommandCaptureMiddleware],
          custom: { captured_commands: capture_buffer },
          **TEST_GENERIC_OPTIONS
        }.merge(kwargs))
        concurrent_worker = ::RedisClient::Cluster::ConcurrentWorker.create
        ::RedisClient::Cluster::Node.new({}, concurrent_worker, pool: pool, config: config).tap do |node|
          @test_nodes ||= []
          @test_nodes << node
        end
      end

      def test_parse_cluster_node_reply_continuous_slots
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

        got = @test_node.send(:parse_cluster_node_reply, info)
        assert_equal(want, got.map(&:to_h))
      end

      def test_parse_cluster_node_reply_discrete_slots
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

        got = @test_node.send(:parse_cluster_node_reply, info)
        assert_equal(want, got.map(&:to_h))
      end

      def test_parse_cluster_node_reply_discrete_slots_and_resharding
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

        got = @test_node.send(:parse_cluster_node_reply, info)
        assert_equal(want, got.map(&:to_h))
      end

      def test_parse_cluster_node_reply_with_hostname
        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004,localhost slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002,localhost master - 0 1426238316232 2 connected 5461-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003,localhost master - 0 1426238318243 3 connected 10923-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005,localhost slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006,localhost slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001,localhost myself,master - 0 0 1 connected 0-5460
        INFO

        want = [
          { id: '07c37dfeb235213a872192d90877d0cd55635b91', node_key: 'localhost:30004', role: 'slave',
            primary_id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', ping_sent: '0', pong_recv: '1426238317239',
            config_epoch: '4', link_state: 'connected', slots: [] },
          { id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', node_key: 'localhost:30002', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '2', link_state: 'connected', slots: [[5461, 10_922]] },
          { id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', node_key: 'localhost:30003', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238318243',
            config_epoch: '3', link_state: 'connected', slots: [[10_923, 16_383]] },
          { id: '6ec23923021cf3ffec47632106199cb7f496ce01', node_key: 'localhost:30005', role: 'slave',
            primary_id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '5', link_state: 'connected', slots: [] },
          { id: '824fe116063bc5fcf9f4ffd895bc17aee7731ac3', node_key: 'localhost:30006', role: 'slave',
            primary_id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', ping_sent: '0', pong_recv: '1426238317741',
            config_epoch: '6', link_state: 'connected', slots: [] },
          { id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', node_key: 'localhost:30001', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '0', config_epoch: '1', link_state: 'connected', slots: [[0, 5460]] }
        ]

        got = @test_node.send(:parse_cluster_node_reply, info)
        assert_equal(want, got.map(&:to_h))
      end

      def test_parse_cluster_node_reply_with_hostname_and_auxiliaries
        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004,localhost,shard-id=69bc080733d1355567173199cff4a6a039a2f024 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002,localhost,shard-id=114f6674a35b84949fe567f5dfd41415ee776261 master - 0 1426238316232 2 connected 5461-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003,localhost,shard-id=fdb36c73e72dd027bc19811b7c219ef6e55c550e master - 0 1426238318243 3 connected 10923-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005,localhost,shard-id=114f6674a35b84949fe567f5dfd41415ee776261 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006,localhost,shard-id=fdb36c73e72dd027bc19811b7c219ef6e55c550e slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001,localhost,shard-id=69bc080733d1355567173199cff4a6a039a2f024 myself,master - 0 0 1 connected 0-5460
        INFO

        want = [
          { id: '07c37dfeb235213a872192d90877d0cd55635b91', node_key: 'localhost:30004', role: 'slave',
            primary_id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', ping_sent: '0', pong_recv: '1426238317239',
            config_epoch: '4', link_state: 'connected', slots: [] },
          { id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', node_key: 'localhost:30002', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '2', link_state: 'connected', slots: [[5461, 10_922]] },
          { id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', node_key: 'localhost:30003', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '1426238318243',
            config_epoch: '3', link_state: 'connected', slots: [[10_923, 16_383]] },
          { id: '6ec23923021cf3ffec47632106199cb7f496ce01', node_key: 'localhost:30005', role: 'slave',
            primary_id: '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', ping_sent: '0', pong_recv: '1426238316232',
            config_epoch: '5', link_state: 'connected', slots: [] },
          { id: '824fe116063bc5fcf9f4ffd895bc17aee7731ac3', node_key: 'localhost:30006', role: 'slave',
            primary_id: '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', ping_sent: '0', pong_recv: '1426238317741',
            config_epoch: '6', link_state: 'connected', slots: [] },
          { id: 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', node_key: 'localhost:30001', role: 'master',
            primary_id: '-', ping_sent: '0', pong_recv: '0', config_epoch: '1', link_state: 'connected', slots: [[0, 5460]] }
        ]

        got = @test_node.send(:parse_cluster_node_reply, info)
        assert_equal(want, got.map(&:to_h))
      end

      def test_parse_cluster_node_reply_with_auxiliaries
        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004,,shard-id=69bc080733d1355567173199cff4a6a039a2f024 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002,,shard-id=114f6674a35b84949fe567f5dfd41415ee776261 master - 0 1426238316232 2 connected 5461-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003,,shard-id=fdb36c73e72dd027bc19811b7c219ef6e55c550e master - 0 1426238318243 3 connected 10923-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005,,shard-id=114f6674a35b84949fe567f5dfd41415ee776261 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006,,shard-id=fdb36c73e72dd027bc19811b7c219ef6e55c550e slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001,,shard-id=69bc080733d1355567173199cff4a6a039a2f024 myself,master - 0 0 1 connected 0-5460
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

        got = @test_node.send(:parse_cluster_node_reply, info)
        assert_equal(want, got.map(&:to_h))
      end

      def test_parse_cluster_node_reply_failure_link_state
        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 disconnected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 disconnected 5461-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 disconnected 10923-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 disconnected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 disconnected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 disconnected 0-5460
        INFO

        assert_empty(@test_node.send(:parse_cluster_node_reply, info))
      end

      def test_parse_cluster_node_reply_failure_flags
        info = <<~INFO
          07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 fail?,slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
          67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 fail,master - 0 1426238316232 2 connected 5461-10922
          292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master,handshake - 0 1426238318243 3 connected 10923-16383
          6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 noaddr,slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
          824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 noflags 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
          e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,fail,master - 0 0 1 connected 0-5460
        INFO

        assert_empty(@test_node.send(:parse_cluster_node_reply, info))
      end

      def test_inspect
        assert_match(/^#<RedisClient::Cluster::Node [0-9., :]*>$/, @test_node.inspect)
      end

      def test_enumerable
        refute(@test_node.any?(&:nil?))
      end

      def test_node_keys
        want = @test_node_info_list.map(&:node_key)
        @test_node.node_keys.each do |got|
          assert_includes(want, got, "Case: #{got}")
        end
      end

      def test_find_by
        @test_node_info_list.each do |info|
          msg = "Case: primary only: #{info.node_key}"
          got = -> { @test_node.find_by(info.node_key) }
          if info.primary?
            assert_instance_of(::RedisClient, got.call, msg)
          else
            assert_raises(::RedisClient::Cluster::Node::ReloadNeeded, msg, &got)
          end

          msg = "Case: scale read: #{info.node_key}"
          got = @test_node_with_scale_read.find_by(info.node_key)
          assert_instance_of(::RedisClient, got, msg)
        end
      end

      def test_call_all
        want = (1..(@test_node_info_list.count(&:primary?))).map { |_| 'PONG' }
        got = @test_node.call_all(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: primary only')

        want = (1..(@test_node_info_list.count)).map { |_| 'PONG' }
        got = @test_node_with_scale_read.call_all(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: scale read')
      end

      def test_call_primaries
        want = (1..(@test_node_info_list.count(&:primary?))).map { |_| 'PONG' }

        got = @test_node.call_primaries(:call_v, ['PING'], [])
        assert_equal(want, got)

        got = @test_node_with_scale_read.call_primaries(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: scale read')
      end

      def test_call_replicas
        want = (1..(@test_node_info_list.count(&:primary?))).map { |_| 'PONG' }

        got = @test_node.call_replicas(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: primary only')

        got = @test_node_with_scale_read.call_replicas(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: scale read')
      end

      def test_send_ping
        want = (1..(@test_node_info_list.count(&:primary?))).map { |_| 'PONG' }
        got = @test_node.send_ping(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: primary only')

        want = (1..(@test_node_info_list.count)).map { |_| 'PONG' }
        got = @test_node_with_scale_read.send_ping(:call_v, ['PING'], [])
        assert_equal(want, got, 'Case: scale read')
      end

      def test_clients_for_scanning # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        test_config = @test_node.instance_variable_get(:@config)
        want = @test_node_info_list.select(&:primary?)
                                   .map(&:node_key)
                                   # need to call client_config_for_node so that if we're using fixed_hostname in this test,
                                   # we get the actual hostname we're connecting to, not the one returned by the cluster API
                                   .map { |key| test_config.client_config_for_node(key) }
                                   .map { |cfg| "#{cfg[:host]}:#{cfg[:port]}" }
                                   .sort
        got = @test_node.clients_for_scanning.map { |client| "#{client.config.host}:#{client.config.port}" }.sort
        assert_equal(want, got, 'Case: primary only')

        want = @test_node_info_list.select(&:replica?)
                                   .map(&:node_key)
                                   # As per above, we need to get the real hostname, not that reported by Redis,
                                   # if fixed_hostname is set.
                                   .map { |key| test_config.client_config_for_node(key) }
                                   .map { |cfg| "#{cfg[:host]}:#{cfg[:port]}" }
                                   .sort
        got = @test_node_with_scale_read.clients_for_scanning.map { |client| "#{client.config.host}:#{client.config.port}" }
        got.each { |e| assert_includes(want, e, 'Case: scale read') }
      end

      def test_find_node_key_of_primary
        sample_node = @test_node_info_list.find(&:primary?)
        sample_slot = sample_node.slots.first.first
        got = @test_node.find_node_key_of_primary(sample_slot)
        assert_equal(sample_node.node_key, got, 'Case: sample slot')
        assert_nil(@test_node.find_node_key_of_primary(nil), 'Case: nil')
      end

      def test_find_node_key_of_replica
        sample_node = @test_node_info_list.find(&:primary?)
        sample_slot = sample_node.slots.first.first
        got = @test_node.find_node_key_of_replica(sample_slot)
        assert_equal(sample_node.node_key, got, 'Case: primary only')

        sample_replicas = @test_node_info_list.select(&:replica?)
        sample_primary = @test_node_info_list.find { |info| info.id == sample_replicas.first.primary_id }
        sample_slot = sample_primary.slots.first.first
        got = @test_node_with_scale_read.find_node_key_of_replica(sample_slot)
        want = sample_replicas.map(&:node_key)
        assert_includes(want, got, 'Case: scale read')

        assert_nil(@test_node.find_node_key_of_replica(nil), 'Case: nil')
      end

      def test_any_primary_node_key
        primary_node_keys = @test_node_info_list.select(&:primary?).map(&:node_key)

        got = @test_node.any_primary_node_key
        assert_includes(primary_node_keys, got, 'Case: primary only')

        got = @test_node_with_scale_read.any_primary_node_key
        assert_includes(primary_node_keys, got, 'Case: scale read')
      end

      def test_any_replica_node_key
        primary_node_keys = @test_node_info_list.select(&:primary?).map(&:node_key)
        replica_node_keys = @test_node_info_list.select(&:replica?).map(&:node_key)

        got = @test_node.any_replica_node_key
        assert_includes(primary_node_keys, got, 'Case: primary only')

        got = @test_node_with_scale_read.any_replica_node_key
        assert_includes(replica_node_keys, got, 'Case: scale read')
      end

      def test_update_slot
        sample_slot = 0
        base_node_key = @test_node.find_node_key_of_primary(sample_slot)
        another_node_key = @test_node_info_list.find { |info| info.node_key != base_node_key && info.primary? }&.node_key
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
        node_info_list = [
          { node_key: '127.0.0.1:7001', role: 'master', slots: [[0, 3000], [3002, 5460], [15_001, 15_001]] },
          { node_key: '127.0.0.1:7002', role: 'master', slots: [[3001, 3001], [5461, 7000], [7002, 10_922]] },
          { node_key: '127.0.0.1:7003', role: 'master', slots: [[7001, 7001], [10_923, 15_000], [15_002, 16_383]] },
          { node_key: '127.0.0.1:7004', role: 'slave', slots: [] },
          { node_key: '127.0.0.1:7005', role: 'slave', slots: [] },
          { node_key: '127.0.0.1:7006', role: 'slave', slots: [] }
        ].map { |info| ::RedisClient::Cluster::Node::Info.new(**info) }

        got = @test_node.send(:build_slot_node_mappings, node_info_list)

        node_info_list.each do |info|
          next if info.slots.empty?

          info.slots.each do |range|
            (range[0]..range[1]).each { |slot| assert_same(info.node_key, got[slot], "Case: #{slot}") }
          end
        end
      end

      def test_make_array_for_slot_node_mappings_optimized
        node_info_list = Array.new(256) do |i|
          ::RedisClient::Cluster::Node::Info.new(
            node_key: "127.0.0.1:#{1024 + i + 1}",
            role: 'master'
          )
        end

        want = node_info_list.first.node_key
        got = @test_node.send(:make_array_for_slot_node_mappings, node_info_list)
        assert_instance_of(::RedisClient::Cluster::Node::USE_CHAR_ARRAY_SLOT ? ::RedisClient::Cluster::Node::CharArray : Array, got)
        ::RedisClient::Cluster::Node::SLOT_SIZE.times do |i|
          got[i] = want
          assert_equal(want, got[i], "Case: #{i}")
        end
      end

      def test_make_array_for_slot_node_mappings_unoptimized
        node_info_list = Array.new(257) do |i|
          ::RedisClient::Cluster::Node::Info.new(
            node_key: "127.0.0.1:#{1024 + i + 1}",
            role: 'master'
          )
        end

        want = node_info_list.first.node_key
        got = @test_node.send(:make_array_for_slot_node_mappings, node_info_list)
        assert_instance_of(Array, got)
        ::RedisClient::Cluster::Node::SLOT_SIZE.times do |i|
          got[i] = want
          assert_equal(want, got[i], "Case: #{i}")
        end
      end

      def test_make_array_for_slot_node_mappings_max_shard_size
        node_info_list = Array.new(255) do |i|
          ::RedisClient::Cluster::Node::Info.new(
            node_key: "127.0.0.1:#{1024 + i + 1}",
            role: 'master'
          )
        end

        got = @test_node.send(:make_array_for_slot_node_mappings, node_info_list)
        assert_instance_of(::RedisClient::Cluster::Node::USE_CHAR_ARRAY_SLOT ? ::RedisClient::Cluster::Node::CharArray : Array, got)

        ::RedisClient::Cluster::Node::SLOT_SIZE.times { |i| got[i] = node_info_list.first.node_key }

        got[0] = 'newbie:6379'
        assert_equal('newbie:6379', got[0])
        assert_raises(RangeError) { got[0] = 'zombie:6379' } if ::RedisClient::Cluster::Node::USE_CHAR_ARRAY_SLOT

        assert_raises(IndexError) { got[-1] = 'newbie:6379' } if ::RedisClient::Cluster::Node::USE_CHAR_ARRAY_SLOT
        assert_raises(IndexError) { got[-1] } if ::RedisClient::Cluster::Node::USE_CHAR_ARRAY_SLOT

        got[16_384] = 'newbie:6379'
        assert_nil(got[16_384]) if ::RedisClient::Cluster::Node::USE_CHAR_ARRAY_SLOT
      end

      def test_build_replication_mappings_regular
        node_key1 = '127.0.0.1:7001'
        node_key2 = '127.0.0.1:7002'
        node_key3 = '127.0.0.1:7003'
        node_key4 = '127.0.0.1:7004'
        node_key5 = '127.0.0.1:7005'
        node_key6 = '127.0.0.1:7006'
        node_key7 = '127.0.0.1:7007'
        node_key8 = '127.0.0.1:7008'
        node_key9 = '127.0.0.1:7009'

        node_info_list = [
          { id: '1', node_key: node_key1, primary_id: '-' },
          { id: '2', node_key: node_key2, primary_id: '-' },
          { id: '3', node_key: node_key3, primary_id: '-' },
          { id: '4', node_key: node_key4, primary_id: '1' },
          { id: '5', node_key: node_key5, primary_id: '2' },
          { id: '6', node_key: node_key6, primary_id: '3' },
          { id: '7', node_key: node_key7, primary_id: '1' },
          { id: '8', node_key: node_key8, primary_id: '2' },
          { id: '9', node_key: node_key9, primary_id: '3' }
        ].map { |info| ::RedisClient::Cluster::Node::Info.new(**info) }

        got = @test_node.send(:build_replication_mappings, node_info_list)
        got.transform_values!(&:sort!)

        assert_same(node_key4, got[node_key1][0])
        assert_same(node_key7, got[node_key1][1])
        assert_same(node_key5, got[node_key2][0])
        assert_same(node_key8, got[node_key2][1])
        assert_same(node_key6, got[node_key3][0])
        assert_same(node_key9, got[node_key3][1])
      end

      def test_build_replication_mappings_lack_of_replica
        node_key1 = '127.0.0.1:7001'
        # node_key2 = '127.0.0.1:7002' # lack
        node_key3 = '127.0.0.1:7003'
        node_key4 = '127.0.0.1:7004'
        node_key5 = '127.0.0.1:7005'
        node_key6 = '127.0.0.1:7006'

        node_info_list = [
          { id: '1', role: 'master', node_key: node_key1, primary_id: '-' },
          { id: '3', role: 'master', node_key: node_key3, primary_id: '-' },
          { id: '4', role: 'slave', node_key: node_key4, primary_id: '1' },
          { id: '5', role: 'master', node_key: node_key5, primary_id: '-' },
          { id: '6', role: 'slave', node_key: node_key6, primary_id: '3' }
        ].map { |info| ::RedisClient::Cluster::Node::Info.new(**info) }

        got = @test_node.send(:build_replication_mappings, node_info_list)
        got.transform_values!(&:sort!)

        assert_equal(3, got.size)
        assert_same(node_key4, got[node_key1][0])
        assert_same(node_key6, got[node_key3][0])
        assert_empty(got[node_key5])
      end

      def test_try_map
        primary_node_keys = @test_node_info_list.select(&:primary?).map(&:node_key)
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

      def test_reload
        capture_buffer = []
        test_node = make_node(replica: true, capture_buffer: capture_buffer)

        capture_buffer.clear
        test_node.reload!

        # It should have reloaded by calling CLUSTER NODES on three of the startup nodes
        cluster_node_cmds = capture_buffer.select { |c| c.command == %w[CLUSTER NODES] }
        assert_equal RedisClient::Cluster::Node::MAX_STARTUP_SAMPLE, cluster_node_cmds.size

        # It should have connected to all of the clients.
        assert_equal TEST_NUMBER_OF_NODES, test_node.to_a.size

        # If we reload again, it should NOT change the redis client instances we have.
        original_client_ids = test_node.to_a.map(&:object_id).to_set
        test_node.reload!
        new_client_ids = test_node.to_a.map(&:object_id).to_set
        assert_equal original_client_ids, new_client_ids
      end

      def test_reload_with_original_config
        bootstrap_node = TEST_NODE_URIS.first
        capture_buffer = []
        test_node = make_node(
          nodes: [bootstrap_node],
          replica: true,
          connect_with_original_config: true,
          capture_buffer: capture_buffer
        )

        test_node.reload!
        # After reloading the first time, our Node object knows about all hosts, despite only starting with one:
        assert_equal TEST_NUMBER_OF_NODES, test_node.to_a.size

        # When we reload, it will only call CLUSTER NODES against a single node, the bootstrap node.
        capture_buffer.clear
        test_node.reload!

        cluster_node_cmds = capture_buffer.select { |c| c.command == %w[CLUSTER NODES] }
        assert_equal 1, cluster_node_cmds.size
        assert_equal bootstrap_node, cluster_node_cmds.first.server_url
      end

      def test_reload_concurrently
        capture_buffer = []
        test_node = make_node(replica: true, pool: { size: 2 }, capture_buffer: capture_buffer)

        # Simulate refetch_node_info_list taking a long time
        test_node.singleton_class.prepend(Module.new do
          def refetch_node_info_list(...)
            r = super
            sleep 2
            r
          end
        end)

        capture_buffer.clear
        t1 = Thread.new { test_node.reload! }
        t2 = Thread.new { test_node.reload! }
        [t1, t2].each(&:join)

        # We should only have reloaded once, which is to say, we only called CLUSTER NODES command MAX_STARTUP_SAMPLE
        # times
        cluster_node_cmds = capture_buffer.select { |c| c.command == %w[CLUSTER NODES] }
        assert_equal RedisClient::Cluster::Node::MAX_STARTUP_SAMPLE, cluster_node_cmds.size
      end
    end
    # rubocop:enable Metrics/ClassLength
  end
end
