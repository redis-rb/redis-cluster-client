# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    # Raised when client connected to redis as cluster mode
    # and failed to fetch cluster state information by commands.
    class InitialSetupError < ::RedisClient::Error
      # @param errors [Array<Redis::BaseError>]
      def initialize(errors)
        super("Redis client could not fetch cluster information: #{errors&.map(&:message)&.uniq&.join(',')}")
      end
    end

    # Raised when client connected to redis as cluster mode
    # and some cluster subcommands were called.
    class OrchestrationCommandNotSupported < ::RedisClient::Error
      def initialize(command, subcommand = '')
        str = [command, subcommand].map(&:to_s).reject(&:empty?).join(' ').upcase
        msg = "#{str} command should be used with care "\
              'only by applications orchestrating Redis Cluster, like redis-trib, '\
              'and the command if used out of the right context can leave the cluster '\
              'in a wrong state or cause data loss.'
        super(msg)
      end
    end

    # Raised when error occurs on any node of cluster.
    class CommandErrorCollection < ::RedisClient::Error
      attr_reader :errors

      # @param errors [Hash{String => Redis::CommandError}]
      # @param error_message [String]
      def initialize(errors, error_message = 'Command errors were replied on any node')
        @errors = errors
        super(error_message)
      end
    end

    # Raised when cluster client can't select node.
    class AmbiguousNodeError < ::RedisClient::Error
      def initialize(command)
        super("Cluster client doesn't know which node the #{command} command should be sent to.")
      end
    end

    # Raised when commands in pipelining include cross slot keys.
    class CrossSlotPipeliningError < ::RedisClient::Error
      def initialize(keys)
        super("Cluster client couldn't send pipelining to single node. "\
              "The commands include cross slot keys. #{keys}")
      end
    end
  end
end
