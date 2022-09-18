# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    class Command
      EMPTY_STRING = ''

      class << self
        def load(nodes) # rubocop:disable Metrics/MethodLength
          errors = []
          cmd = nil
          nodes&.each do |node|
            break unless cmd.nil?

            reply = node.call('COMMAND')
            details = parse_command_details(reply)
            cmd = ::RedisClient::Cluster::Command.new(details)
          rescue ::RedisClient::Error => e
            errors << e
          end

          return cmd unless cmd.nil?

          raise ::RedisClient::Cluster::InitialSetupError, errors
        end

        private

        def parse_command_details(rows)
          rows&.reject { |row| row[0].nil? }.to_h do |row|
            [row[0].downcase, { arity: row[1], flags: row[2], first: row[3], last: row[4], step: row[5] }]
          end
        end
      end

      def initialize(details)
        @details = pick_details(details)
        @normalized_cmd_name_cache = {}
      end

      def extract_first_key(command)
        i = determine_first_key_position(command)
        return EMPTY_STRING if i == 0

        key = (command[i].is_a?(Array) ? command[i].flatten.first : command[i]).to_s
        hash_tag = extract_hash_tag(key)
        hash_tag.empty? ? key : hash_tag
      end

      def should_send_to_primary?(command)
        dig_details(command, :write)
      end

      def should_send_to_replica?(command)
        dig_details(command, :readonly)
      end

      def exists?(name)
        @details.key?(name.to_s.downcase)
      end

      private

      def pick_details(details)
        (details || {}).transform_values do |detail|
          {
            first_key_position: detail[:first],
            write: detail[:flags].include?('write'),
            readonly: detail[:flags].include?('readonly')
          }
        end
      end

      def dig_details(command, key)
        name = normalize_cmd_name(command)
        return if name.empty? || !@details.key?(name)

        @details.fetch(name).fetch(key)
      end

      def determine_first_key_position(command) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/MethodLength
        case normalize_cmd_name(command)
        when 'eval', 'evalsha', 'zinterstore', 'zunionstore' then 3
        when 'object' then 2
        when 'memory'
          command[1].to_s.casecmp('usage').zero? ? 2 : 0
        when 'migrate'
          command[3].empty? ? determine_optional_key_position(command, 'keys') : 3
        when 'xread', 'xreadgroup'
          determine_optional_key_position(command, 'streams')
        else
          dig_details(command, :first_key_position).to_i
        end
      end

      def determine_optional_key_position(command, option_name) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        idx = command&.flatten&.map(&:to_s)&.map(&:downcase)&.index(option_name&.downcase)
        idx.nil? ? 0 : idx + 1
      end

      # @see https://redis.io/topics/cluster-spec#keys-hash-tags Keys hash tags
      def extract_hash_tag(key)
        key = key.to_s
        s = key.index('{')
        e = key.index('}', s.to_i + 1)

        return EMPTY_STRING if s.nil? || e.nil?

        key[s + 1..e - 1]
      end

      def normalize_cmd_name(command)
        return EMPTY_STRING unless command.is_a?(Array)

        name = case e = command.first
               when String then e
               when Array then e.first
               end
        return EMPTY_STRING if name.nil? || name.empty?

        @normalized_cmd_name_cache[name] = name.downcase unless @normalized_cmd_name_cache.key?(name)
        @normalized_cmd_name_cache[name]
      end
    end
  end
end
