# frozen_string_literal: true

require 'redis_client/cluster/errors'
require 'redis_client/cluster/node_key'

class RedisClient
  class Cluster
    module SlotLoader
      module_function

      def load(nodes)
        errors = nodes.map do |node|
          return fetch_slot_info(node)
        rescue ::RedisClient::ConnectionError, ::RedisClient::CommandError => e
          e
        end

        raise ::RedisClient::Cluster::InitialSetupError, errors
      end

      def fetch_slot_info(node)
        hash_with_default_arr = Hash.new { |h, k| h[k] = [] }
        node.call(%i[cluster slots])
            .flat_map { |arr| parse_slot_info(arr, default_ip: node.host) }
            .each_with_object(hash_with_default_arr) { |arr, h| h[arr[0]] << arr[1] }
      end

      def parse_slot_info(arr, default_ip:)
        first_slot, last_slot = arr[0..1]
        slot_range = (first_slot..last_slot).freeze
        arr[2..].map { |addr| [stringify_node_key(addr, default_ip), slot_range] }
      end

      def stringify_node_key(arr, default_ip)
        ip, port = arr
        ip = default_ip if ip.empty? # When cluster is down
        ::RedisClient::Cluster::NodeKey.build_from_host_port(ip, port)
      end

      private_class_method :fetch_slot_info, :parse_slot_info, :stringify_node_key
    end
  end
end
