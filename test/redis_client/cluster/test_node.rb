# frozen_string_literal: true

require 'uri'
require 'testing_helper'
require 'redis_client/cluster/node'

class RedisClient
  class Cluster
    class Node
      class TestConfig < Minitest::Test
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

    class TestNode < Minitest::Test
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

        got = ::RedisClient::Cluster::Node.send(:parse_node_info, info)
        assert_equal(want, got)
      end
    end
  end
end
