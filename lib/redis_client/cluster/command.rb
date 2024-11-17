# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'
require 'redis_client/cluster/normalized_cmd_name'

class RedisClient
  class Cluster
    class Command
      EMPTY_STRING = ''
      EMPTY_HASH = {}.freeze
      EMPTY_ARRAY = [].freeze

      private_constant :EMPTY_STRING, :EMPTY_HASH, :EMPTY_ARRAY

      Detail = Struct.new(
        'RedisCommand',
        :first_key_position,
        :key_step,
        :write?,
        :readonly?,
        keyword_init: true
      )

      class << self
        def load(nodes, slow_command_timeout: -1) # rubocop:disable Metrics/AbcSize
          cmd = errors = nil

          nodes&.each do |node|
            regular_timeout = node.read_timeout
            node.read_timeout = slow_command_timeout > 0.0 ? slow_command_timeout : regular_timeout
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

          raise ::RedisClient::Cluster::InitialSetupError.from_errors(errors)
        end

        private

        def parse_command_reply(rows)
          rows&.each_with_object({}) do |row, acc|
            next if row[0].nil?

            acc[row[0].downcase] = ::RedisClient::Cluster::Command::Detail.new(
              first_key_position: row[3],
              key_step: row[5],
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

        command[i]
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

      def determine_optional_key_position(command, option_name)
        idx = command.map { |e| e.to_s.downcase }.index(option_name&.downcase)
        idx.nil? ? 0 : idx + 1
      end
    end
  end
end
