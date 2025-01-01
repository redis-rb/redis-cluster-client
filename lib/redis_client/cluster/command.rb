# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'

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
            reply = node.call('command')
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

        def parse_command_reply(rows) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
          rows&.each_with_object({}) do |row, acc|
            next if row.first.nil?

            # TODO: in redis 7.0 or later, subcommand information included in the command reply

            pos = case row.first
                  when 'eval', 'evalsha', 'zinterstore', 'zunionstore' then 3
                  when 'object', 'xgroup' then 2
                  when 'migrate', 'xread', 'xreadgroup' then 0
                  else row[3]
                  end

            writable = case row.first
                       when 'xgroup' then true
                       else row[2].include?('write')
                       end

            acc[row.first] = ::RedisClient::Cluster::Command::Detail.new(
              first_key_position: pos,
              key_step: row[5],
              write?: writable,
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
        find_command_info(command.first)&.write?
      end

      def should_send_to_replica?(command)
        find_command_info(command.first)&.readonly?
      end

      def exists?(name)
        @commands.key?(name) || @commands.key?(name.to_s.downcase(:ascii))
      end

      private

      def find_command_info(name)
        @commands[name] || @commands[name.to_s.downcase(:ascii)]
      end

      def determine_first_key_position(command) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/AbcSize, Metrics/PerceivedComplexity
        i = find_command_info(command.first)&.first_key_position.to_i
        return i if i > 0

        cmd_name = command.first
        if cmd_name.casecmp('xread').zero?
          determine_optional_key_position(command, 'streams')
        elsif cmd_name.casecmp('xreadgroup').zero?
          determine_optional_key_position(command, 'streams')
        elsif cmd_name.casecmp('migrate').zero?
          command[3].empty? ? determine_optional_key_position(command, 'keys') : 3
        elsif cmd_name.casecmp('memory').zero?
          command[1].to_s.casecmp('usage').zero? ? 2 : 0
        else
          i
        end
      end

      def determine_optional_key_position(command, option_name)
        i = command.index { |v| v.to_s.casecmp(option_name).zero? }
        i.nil? ? 0 : i + 1
      end
    end
  end
end
