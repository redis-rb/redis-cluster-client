# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/normalized_cmd_name'

class RedisClient
  class Cluster
    class Command
      SLOW_COMMAND_TIMEOUT = Float(ENV.fetch('REDIS_CLIENT_SLOW_COMMAND_TIMEOUT', -1))

      EMPTY_STRING = ''
      LEFT_BRACKET = '{'
      RIGHT_BRACKET = '}'
      EMPTY_HASH = {}.freeze

      Detail = Struct.new(
        'RedisCommand',
        :first_key_position,
        :write?,
        :readonly?,
        keyword_init: true
      )

      class << self
        def load(nodes)
          cmd = errors = nil

          nodes&.each do |node|
            regular_timeout = node.read_timeout
            node.read_timeout = SLOW_COMMAND_TIMEOUT > 0.0 ? SLOW_COMMAND_TIMEOUT : regular_timeout
            reply = node.call('COMMAND')
            node.read_timeout = regular_timeout
            commands = parse_command_reply(reply)
            cmd = ::RedisClient::Cluster::Command.new(commands)
            break
          rescue ::RedisClient::Error => e
            errors ||= []
            errors << e
          end

          return cmd unless cmd.nil?

          raise ::RedisClient::Cluster::InitialSetupError, errors
        end

        private

        def parse_command_reply(rows)
          rows&.each_with_object({}) do |row, acc|
            next if row[0].nil?

            acc[row[0].downcase] = ::RedisClient::Cluster::Command::Detail.new(
              first_key_position: row[3],
              write?: row[2].include?('write'),
              readonly?: row[2].include?('readonly')
            )
          end.freeze || EMPTY_HASH
        end
      end

      def initialize(commands)
        @commands = commands || EMPTY_HASH
      end

      def extract_first_key(command)
        i = determine_first_key_position(command)
        return EMPTY_STRING if i == 0

        key = (command[i].is_a?(Array) ? command[i].flatten.first : command[i]).to_s
        hash_tag = extract_hash_tag(key)
        hash_tag.empty? ? key : hash_tag
      end

      def should_send_to_primary?(command)
        name = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        @commands[name]&.write?
      end

      def should_send_to_replica?(command)
        name = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        @commands[name]&.readonly?
      end

      def exists?(name)
        @commands.key?(::RedisClient::Cluster::NormalizedCmdName.instance.get_by_name(name))
      end

      private

      def determine_first_key_position(command) # rubocop:disable Metrics/CyclomaticComplexity
        case name = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        when 'eval', 'evalsha', 'zinterstore', 'zunionstore' then 3
        when 'object' then 2
        when 'memory'
          command[1].to_s.casecmp('usage').zero? ? 2 : 0
        when 'migrate'
          command[3].empty? ? determine_optional_key_position(command, 'keys') : 3
        when 'xread', 'xreadgroup'
          determine_optional_key_position(command, 'streams')
        else
          @commands[name]&.first_key_position.to_i
        end
      end

      def determine_optional_key_position(command, option_name) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        idx = command&.flatten&.map(&:to_s)&.map(&:downcase)&.index(option_name&.downcase)
        idx.nil? ? 0 : idx + 1
      end

      # @see https://redis.io/topics/cluster-spec#keys-hash-tags Keys hash tags
      def extract_hash_tag(key)
        key = key.to_s
        s = key.index(LEFT_BRACKET)
        return EMPTY_STRING if s.nil?

        e = key.index(RIGHT_BRACKET, s + 1)
        return EMPTY_STRING if e.nil?

        key[s + 1..e - 1]
      end
    end
  end
end
