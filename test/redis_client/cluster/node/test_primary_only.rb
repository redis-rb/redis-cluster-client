# frozen_string_literal: true

require 'uri'
require 'testing_helper'

class RedisClient
  class Cluster
    class Node
      class TestPrimaryOnly < TestingWrapper
        test_config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          **TEST_GENERIC_OPTIONS
        )
        test_node_info = ::RedisClient::Cluster::Node.load_info(test_config.per_node_key)
        if TEST_FIXED_HOSTNAME
          test_node_info.each do |info|
            _, port = ::RedisClient::Cluster::NodeKey.split(info[:node_key])
            info[:node_key] = ::RedisClient::Cluster::NodeKey.build_from_host_port(TEST_FIXED_HOSTNAME, port)
          end
        end
        node_addrs = test_node_info.map { |info| ::RedisClient::Cluster::NodeKey.hashify(info[:node_key]) }
        test_config.update_node(node_addrs)
        @options = test_config.per_node_key
        @test_node = ::RedisClient::Cluster::Node.new(@options, node_info: test_node_info)
        @cfg_attrs = %i[connect_timeout read_timeout write_timeout].freeze
      end

      def teardown
        @test_node&.each(&:close)
      end

      def test_build_clients
        subject = ::RedisClient::Cluster::Node::PrimaryOnly.new({}, {}, nil)

        got = subject.send(:build_clients,
                           @test_config.instance_variable_get(:@replications),
                           @options,
                           nil,
                           **TEST_GENERIC_OPTIONS.merge(timeout: 10))

        got.each_value { |client| assert_instance_of(::RedisClient, client) }
        assert_equal(@test_node_info.count { |info| info[:role] == 'master' }, got.size)
        got.each_value do |client|
          @cfg_attrs.each { |attr| assert_equal(10, client.config.send(attr), "Case: #{attr}") }
        end
        got.each_value(&:close)
      end

      def test_build_clients_with_connection_pooling
        subject = ::RedisClient::Cluster::Node::PrimaryOnly.new({}, {}, nil)

        got = subject.send(:build_clients,
                           @test_config.instance_variable_get(:@replications),
                           @options,
                           { timeout: 3, size: 2 },
                           **TEST_GENERIC_OPTIONS.merge(timeout: 10))

        got.each_value { |client| assert_instance_of(::RedisClient::Pooled, client) }
        assert_equal(@test_node_info.count { |info| info[:role] == 'master' }, got.size)
        got.each_value do |client|
          @cfg_attrs.each { |attr| assert_equal(10, client.config.send(attr), "Case: #{attr}") }
        end
        got.each_value(&:close)
      end
    end
  end
end
