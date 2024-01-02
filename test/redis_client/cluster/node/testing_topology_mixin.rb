# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      module TestingTopologyMixin
        def make_node(pool: nil, **kwargs)
          config = ::RedisClient::ClusterConfig.new(**{
            nodes: TEST_NODE_URIS,
            fixed_hostname: TEST_FIXED_HOSTNAME,
            **TEST_GENERIC_OPTIONS,
            **self.class::TESTING_TOPOLOGY_OPTIONS
          }.merge(kwargs))
          concurrent_worker = ::RedisClient::Cluster::ConcurrentWorker.create
          ::RedisClient::Cluster::Node.new(concurrent_worker, pool: pool, config: config).tap do |node|
            node.reload!
            @test_nodes ||= []
            @test_nodes << node
          end
        end

        def setup
          @test_node = make_node
          @test_topology = @test_node.instance_variable_get(:@topology)
          @replications = @test_node.instance_variable_get(:@replications)
        end

        def teardown
          @test_nodes&.each { |n| n.each(&:close) }
        end
      end
    end
  end
end
