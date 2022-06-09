# frozen_string_literal: true

require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    module CommandLoader
      module_function

      def load(nodes)
        errors = nodes.map do |node|
          return fetch_command_details(node)
        rescue ::RedisClient::ConnectionError, ::RedisClient::CommandError => e
          e
        end

        raise ::RedisClient::Cluster::InitialSetupError, errors
      end

      def fetch_command_details(node)
        node.call(%i[command]).to_h do |reply|
          [reply[0], { arity: reply[1], flags: reply[2], first: reply[3], last: reply[4], step: reply[5] }]
        end
      end

      private_class_method :fetch_command_details
    end
  end
end
