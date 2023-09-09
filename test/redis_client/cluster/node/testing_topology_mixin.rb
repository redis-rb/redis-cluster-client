# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      module TestingTopologyMixin
        def setup
          test_config = ::RedisClient::ClusterConfig.new(
            nodes: TEST_NODE_URIS,
            fixed_hostname: TEST_FIXED_HOSTNAME,
            **TEST_GENERIC_OPTIONS
          )
          @concurrent_worker = ::RedisClient::Cluster::ConcurrentWorker.create
          test_node_info_list = ::RedisClient::Cluster::Node.load_info(test_config.per_node_key, @concurrent_worker)
          if TEST_FIXED_HOSTNAME
            test_node_info_list.each do |info|
              _, port = ::RedisClient::Cluster::NodeKey.split(info.node_key)
              info.node_key = ::RedisClient::Cluster::NodeKey.build_from_host_port(TEST_FIXED_HOSTNAME, port)
            end
          end
          node_addrs = test_node_info_list.map { |info| ::RedisClient::Cluster::NodeKey.hashify(info.node_key) }
          test_config.update_node(node_addrs)
          @options = test_config.per_node_key
          test_node = ::RedisClient::Cluster::Node.new(@options, @concurrent_worker, node_info_list: test_node_info_list)
          @replications = test_node.instance_variable_get(:@replications)
          test_node&.each(&:close)
          @test_topology = topology_class.new(@replications, @options, nil, @concurrent_worker, **TEST_GENERIC_OPTIONS)
        end

        def teardown
          @test_topology&.clients&.each_value(&:close)
        end
      end
    end
  end
end
