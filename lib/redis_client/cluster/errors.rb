# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    class Error < ::RedisClient::Error
      def with_config(config)
        @config = config
        self
      end
    end

    ERR_ARG_NORMALIZATION = ->(arg) { Array[arg].flatten.reject { |e| e.nil? || (e.respond_to?(:empty?) && e.empty?) } }

    private_constant :ERR_ARG_NORMALIZATION

    class InitialSetupError < Error
      def self.from_errors(errors)
        msg = ERR_ARG_NORMALIZATION.call(errors).map(&:message).uniq.join(',')
        new("Redis client could not fetch cluster information: #{msg}")
      end
    end

    class OrchestrationCommandNotSupported < Error
      def self.from_command(command)
        str = ERR_ARG_NORMALIZATION.call(command).map(&:to_s).join(' ').upcase
        msg = "#{str} command should be used with care " \
              'only by applications orchestrating Redis Cluster, like redis-cli, ' \
              'and the command if used out of the right context can leave the cluster ' \
              'in a wrong state or cause data loss.'
        new(msg)
      end
    end

    class ErrorCollection < Error
      EMPTY_HASH = {}.freeze

      private_constant :EMPTY_HASH
      attr_reader :errors

      def self.with_errors(errors)
        if !errors.is_a?(Hash) || errors.empty?
          new(errors.to_s).with_errors(EMPTY_HASH)
        else
          messages = errors.map { |node_key, error| "#{node_key}: (#{error.class}) #{error.message}" }.freeze
          new(messages.join(', ')).with_errors(errors)
        end
      end

      def initialize(error_message = nil)
        @errors = nil
        super
      end

      def with_errors(errors)
        @errors = errors if @errors.nil?
        self
      end
    end

    class AmbiguousNodeError < Error
      def self.from_command(command)
        new("Cluster client doesn't know which node the #{command} command should be sent to.")
      end
    end

    class NodeMightBeDown < Error
      def initialize(_error_message = nil)
        super(
          'The client is trying to fetch the latest cluster state ' \
          'because a subset of nodes might be down. ' \
          'It might continue to raise errors for a while.'
        )
      end
    end
  end
end
