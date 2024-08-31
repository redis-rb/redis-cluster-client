# frozen_string_literal: true

require 'singleton'

class RedisClient
  class Cluster
    class NormalizedCmdName
      include Singleton

      EMPTY_STRING = ''

      private_constant :EMPTY_STRING

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
        get(name, index: 0)
      end

      def clear
        @mutex.synchronize { @cache.clear }
        true
      end

      private

      def get(command, index:)
        name = extract_name(command, index: index)
        return EMPTY_STRING if name.nil? || name.empty?

        normalize(name)
      end

      def extract_name(command, index:)
        case command
        when String, Symbol then index.zero? ? command : nil
        when Array then extract_name_from_array(command, index: index)
        end
      end

      def extract_name_from_array(command, index:)
        return if command.size - 1 < index

        case e = command[index]
        when String, Symbol then e
        when Array then e[index]
        end
      end

      def normalize(name)
        return @cache[name] || name.to_s.downcase if @cache.key?(name)
        return name.to_s.downcase if @mutex.locked?

        str = name.to_s.downcase
        @mutex.synchronize { @cache[name] = str }
        str
      end
    end
  end
end
