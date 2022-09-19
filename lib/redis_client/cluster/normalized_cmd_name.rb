# frozen_string_literal: true

require 'singleton'

class RedisClient
  class Cluster
    class NormalizedCmdName
      include Singleton

      EMPTY_STRING = ''

      def initialize
        @cache = {}
        @mutex = Mutex.new
      end

      def get_by_command(command)
        get(command, index: 0)
      end

      def get_by_subcommand(command)
        get(command, index: 1)
      end

      def get_by_name(name)
        normalize(name)
      end

      def clear
        @mutex.synchronize { @cache = {} }
      end

      private

      def get(command, index:)
        return EMPTY_STRING unless command.is_a?(Array)

        name = extract_name(command, index: index)
        return EMPTY_STRING if name.nil? || name.empty?

        normalize(name)
      end

      def extract_name(command, index:)
        case e = command[index]
        when String then e
        when Array then e[index]
        end
      end

      def normalize(name)
        return @cache[name] if @cache.key?(name)
        return name.to_s.downcase if @mutex.locked?

        @mutex.synchronize { @cache[name] = name.to_s.downcase }
        @cache[name]
      end
    end
  end
end
