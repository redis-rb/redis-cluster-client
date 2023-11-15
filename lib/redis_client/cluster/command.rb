# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/normalized_cmd_name'

class RedisClient
  class Cluster
    class Command
      EMPTY_STRING = ''
      LEFT_BRACKET = '{'
      RIGHT_BRACKET = '}'
      EMPTY_HASH = {}.freeze
      EMPTY_ARRAY = [].freeze

      Detail = Struct.new(
        'RedisCommand',
        :first_key_position,
        :last_key_position,
        :key_step,
        :write?,
        :readonly?,
        keyword_init: true
      )

      class << self
        def load(nodes, slow_command_timeout: -1)
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

          raise ::RedisClient::Cluster::InitialSetupError, errors
        end

        private

        def parse_command_reply(rows)
          rows&.each_with_object({}) do |row, acc|
            next if row[0].nil?

            acc[row[0].downcase] = ::RedisClient::Cluster::Command::Detail.new(
              first_key_position: row[3],
              last_key_position: row[4],
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

        key = (command[i].is_a?(Array) ? command[i].flatten.first : command[i]).to_s
        hash_tag = extract_hash_tag(key)
        hash_tag.empty? ? key : hash_tag
      end

      def extract_all_keys(command)
        keys_start = determine_first_key_position(command)
        keys_end = determine_last_key_position(command, keys_start)
        keys_step = determine_key_step(command)
        return EMPTY_ARRAY if [keys_start, keys_end, keys_step].any?(&:zero?)

        keys_end = [keys_end, command.size - 1].min
        # use .. inclusive range because keys_end is a valid index.
        (keys_start..keys_end).step(keys_step).map { |i| command[i] }
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

      # IMPORTANT: this determines the last key position INCLUSIVE of the last key -
      # i.e. command[determine_last_key_position(command)] is a key.
      # This is in line with what Redis returns from COMMANDS.
      def determine_last_key_position(command, keys_start) # rubocop:disable Metrics/AbcSize
        case name = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        when 'eval', 'evalsha', 'zinterstore', 'zunionstore'
          # EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
          # ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
          command[2].to_i + 2
        when 'object', 'memory'
          # OBJECT [ENCODING | FREQ | IDLETIME | REFCOUNT] key
          # MEMORY USAGE key [SAMPLES count]
          keys_start
        when 'migrate'
          # MIGRATE host port <key | ""> destination-db timeout [COPY] [REPLACE] [AUTH password | AUTH2 username password] [KEYS key [key ...]]
          command[3].empty? ? (command.length - 1) : 3
        when 'xread', 'xreadgroup'
          # XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
          keys_start + ((command.length - keys_start) / 2) - 1
        else
          # If there is a fixed, non-variable number of keys, don't iterate past that.
          if @commands[name].last_key_position >= 0
            @commands[name].last_key_position
          else
            command.length - 1
          end
        end
      end

      def determine_key_step(command)
        name = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        # Some commands like EVALSHA have zero as the step in COMMANDS somehow.
        @commands[name].key_step == 0 ? 1 : @commands[name].key_step
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
