# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/key_slot_converter'

class RedisClient
  class Cluster
    class Command
      EMPTY_STRING = ''
      EMPTY_HASH = {}.freeze
      REQUEST_POLICY_PREFIX = 'request_policy:'
      RESPONSE_POLICY_PREFIX = 'response_policy:'
      SUBCOMMAND_DELIMITER = '|'

      private_constant :EMPTY_HASH, :REQUEST_POLICY_PREFIX,
                       :RESPONSE_POLICY_PREFIX, :SUBCOMMAND_DELIMITER

      # @see https://redis.io/docs/latest/commands/command/ The reply of the COMMAND command
      # @see https://redis.io/docs/latest/develop/reference/command-tips/ Command tips
      Spec = Struct.new(
        'RedisCommandSpec',
        :first_key_position,
        :key_step,
        :write?,
        :readonly?,
        :request_policy,
        :response_policy,
        :subcommands,
        keyword_init: true
      ) do
        def extract_first_key(command)
          i = first_key_position.to_i
          return command[i] if i > 0

          i = determine_first_key_position(command)
          return ::RedisClient::Cluster::Command::EMPTY_STRING if i == 0

          command[i]
        end

        def should_send_to_primary?
          write?
        end

        def should_send_to_replica?
          readonly?
        end

        private

        def determine_first_key_position(command) # rubocop:disable Metrics/AbcSize
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
            0
          end
        end

        def determine_optional_key_position(command, option_name)
          i = command.index { |v| v.to_s.casecmp(option_name).zero? }
          i.nil? ? 0 : i + 1
        end
      end

      class << self
        def load(nodes, slow_command_timeout: -1) # rubocop:disable Metrics/AbcSize
          cmd = errors = nil

          nodes&.each do |node|
            regular_timeout = node.read_timeout
            node.read_timeout = slow_command_timeout > 0.0 ? slow_command_timeout : regular_timeout
            reply = node.call('command')
            commands = parse_command_reply(reply)
            cmd = ::RedisClient::Cluster::Command.new(commands)
            break
          rescue ::RedisClient::Error => e
            errors ||= []
            errors << e
          ensure
            node.read_timeout = regular_timeout
          end

          return cmd unless cmd.nil?

          raise ::RedisClient::Cluster::InitialSetupError.from_errors(errors)
        end

        private

        def parse_command_reply(rows)
          rows&.each_with_object({}) do |row, acc|
            next if row.first.nil?

            acc[row.first] = build_spec(row)
          end.freeze || EMPTY_HASH
        end

        def build_spec(row)
          ::RedisClient::Cluster::Command::Spec.new(
            first_key_position: parse_first_key_position(row),
            key_step: row[5],
            write?: parse_writability(row),
            readonly?: row[2].include?('readonly'),
            request_policy: parse_policy_tip(row[7], REQUEST_POLICY_PREFIX),
            response_policy: parse_policy_tip(row[7], RESPONSE_POLICY_PREFIX),
            subcommands: parse_subcommands(row[9])
          ).freeze
        end

        # The redis 6.2 or earlier doesn't include the information of the subcommands in the reply.
        # These hard-coded positions are the fallback for such old versions.
        def parse_first_key_position(row)
          case row.first
          when 'eval', 'evalsha', 'zinterstore', 'zunionstore' then 3
          when 'object', 'xgroup' then 2
          when 'migrate', 'xread', 'xreadgroup' then 0
          else row[3]
          end
        end

        def parse_writability(row)
          case row.first
          when 'xgroup' then true
          else row[2].include?('write')
          end
        end

        # The command tips are available in the redis 7.0 or later.
        # It returns a single value instead of a pair to avoid the allocation of the array.
        def parse_policy_tip(tips, prefix)
          tip = tips&.find { |t| t.start_with?(prefix) }
          tip.nil? ? nil : -tip.delete_prefix(prefix)
        end

        # The information of the subcommands is available in the redis 7.0 or later.
        # A container command such as XINFO reports its key positions per subcommand.
        def parse_subcommands(rows)
          return if rows.nil? || rows.empty?

          rows.each_with_object({}) do |row, acc|
            name = row.first
            next if name.nil?

            # The server replies with a full name of the subcommand such as `xinfo|stream`.
            i = name.index(SUBCOMMAND_DELIMITER)
            acc[i.nil? ? name : name[i + 1, name.size]] = build_spec(row)
          end.freeze
        end
      end

      def initialize(commands)
        @commands = commands || EMPTY_HASH
      end

      def get_spec(command) # rubocop:disable Metrics/AbcSize
        name = command.first
        spec = @commands[name] || @commands[name.to_s.downcase(:ascii)]
        return spec if spec.nil? || spec.subcommands.nil?

        subcommand = command[1]
        return spec if subcommand.nil?

        spec.subcommands[subcommand] || spec.subcommands[subcommand.to_s.downcase(:ascii)] || spec
      end

      def exists?(name)
        @commands.key?(name) || @commands.key?(name.to_s.downcase(:ascii))
      end
    end
  end
end
