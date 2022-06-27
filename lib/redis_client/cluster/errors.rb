# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    ERR_ARG_NORMALIZATION = ->(arg) { Array[arg].flatten.reject { |e| e.nil? || (e.respond_to?(:empty?) && e.empty?) } }

    class InitialSetupError < ::RedisClient::Error
      def initialize(errors)
        msg = ERR_ARG_NORMALIZATION.call(errors).map(&:message).uniq.join(',')
        super("Redis client could not fetch cluster information: #{msg}")
      end
    end

    class OrchestrationCommandNotSupported < ::RedisClient::Error
      def initialize(command)
        str = ERR_ARG_NORMALIZATION.call(command).map(&:to_s).join(' ').upcase
        msg = "#{str} command should be used with care " \
              'only by applications orchestrating Redis Cluster, like redis-cli, ' \
              'and the command if used out of the right context can leave the cluster ' \
              'in a wrong state or cause data loss.'
        super(msg)
      end
    end

    class ErrorCollection < ::RedisClient::Error
      attr_reader :errors

      def initialize(errors)
        @errors = {}
        if !errors.is_a?(Hash) || errors.empty?
          super('')
          return
        end

        @errors = errors
        messages = @errors.map { |node_key, error| "#{node_key}: #{error.message}" }
        super("Errors occurred on any node: #{messages.join(', ')}")
      end
    end

    class AmbiguousNodeError < ::RedisClient::Error
      def initialize(command)
        super("Cluster client doesn't know which node the #{command} command should be sent to.")
      end
    end

    class NodeMightBeDown < ::RedisClient::Error
      def initialize(_ = '')
        super(
          'The client is trying to fetch the latest cluster state ' \
          'because a subset of nodes might be down. ' \
          'It might continue to raise errors for a while.'
        )
      end
    end
  end
end
