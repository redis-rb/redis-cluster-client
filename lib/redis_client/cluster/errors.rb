# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    ERR_ARG_NORMALIZATION = ->(arg) { Array[arg].flatten.reject { |e| e.nil? || (e.respond_to?(:empty?) && e.empty?) } }

    # Raised when client connected to redis as cluster mode
    # and failed to fetch cluster state information by commands.
    class InitialSetupError < ::RedisClient::Error
      def initialize(errors)
        msg = ERR_ARG_NORMALIZATION.call(errors).map(&:message).uniq.join(',')
        super("Redis client could not fetch cluster information: #{msg}")
      end
    end

    # Raised when client connected to redis as cluster mode
    # and some cluster subcommands were called.
    class OrchestrationCommandNotSupported < ::RedisClient::Error
      def initialize(command)
        str = ERR_ARG_NORMALIZATION.call(command).map(&:to_s).join(' ').upcase
        msg = "#{str} command should be used with care "\
              'only by applications orchestrating Redis Cluster, like redis-cli, '\
              'and the command if used out of the right context can leave the cluster '\
              'in a wrong state or cause data loss.'
        super(msg)
      end
    end

    # Raised when error occurs on any node of cluster.
    class CommandErrorCollection < ::RedisClient::Error
      attr_reader :errors

      def initialize(errors)
        @errors = {}
        if !errors.is_a?(Hash) || errors.empty?
          super('')
          return
        end

        @errors = errors
        messages = @errors.map { |node_key, error| "#{node_key}: #{error.message}" }
        super("Command errors were replied on any node: #{messages.join(', ')}")
      end
    end

    # Raised when cluster client can't select node.
    class AmbiguousNodeError < ::RedisClient::Error
      def initialize(command)
        super("Cluster client doesn't know which node the #{command} command should be sent to.")
      end
    end
  end
end
