# frozen_string_literal: true

require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    module NodeLoader
      module_function

      def load_flags(nodes)
        errors = nodes.map do |node|
          return fetch_node_info(node)
        rescue ::RedisClient::ConnectionError, ::RedisClient::CommandError => e
          e
        end

        raise ::RedisClient::Cluster::InitialSetupError, errors
      end

      def fetch_node_info(node)
        node.call(%i[cluster nodes])
            .split("\n")
            .map(&:split)
            .to_h { |arr| [arr[1].split('@').first, (arr[2].split(',') & %w[master slave]).first] }
      end

      private_class_method :fetch_node_info
    end
  end
end
