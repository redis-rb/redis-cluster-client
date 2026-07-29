# frozen_string_literal: true

require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    class Router
      # The routing table for the commands which shouldn't be routed by their keys.
      # The built-in entries can be overridden with the `command_routings` option of the config.
      class RoutingTable
        BuildError = Class.new(::RedisClient::Cluster::Error)
        RoutingAction = Struct.new('RedisCommandRoutingAction', :method_name, :reply_transformer, keyword_init: true)
        PICK_FIRST = ->(reply) { reply.first } # rubocop:disable Style/SymbolProc
        FLATTEN_STRINGS = ->(reply) { reply.flatten.sort_by(&:to_s) }
        SUM_NUM = ->(reply) { reply.select { |e| e.is_a?(Integer) }.sum }
        SORT_NUMBERS = ->(reply) { reply.sort_by(&:to_i) }
        if Object.const_defined?(:Ractor, false) && Ractor.respond_to?(:make_shareable)
          Ractor.make_shareable(PICK_FIRST)
          Ractor.make_shareable(FLATTEN_STRINGS)
          Ractor.make_shareable(SUM_NUM)
          Ractor.make_shareable(SORT_NUMBERS)
        end
        DEDICATED_ACTIONS = lambda do # rubocop:disable Metrics/BlockLength
          multiple_key_action = RoutingAction.new(method_name: :send_multiple_keys_command)
          all_node_first_action = RoutingAction.new(method_name: :send_command_to_all_nodes, reply_transformer: PICK_FIRST)
          primary_first_action = RoutingAction.new(method_name: :send_command_to_primaries, reply_transformer: PICK_FIRST)
          not_supported_action = RoutingAction.new(method_name: :fail_not_supported_command)
          keyless_action = RoutingAction.new(method_name: :fail_keyless_command)
          single_node_action = RoutingAction.new(method_name: :assign_node_and_send_command)
          {
            'ping' => RoutingAction.new(method_name: :send_ping_command, reply_transformer: PICK_FIRST),
            'wait' => RoutingAction.new(method_name: :send_wait_command),
            'keys' => RoutingAction.new(method_name: :send_command_to_replicas, reply_transformer: FLATTEN_STRINGS),
            'dbsize' => RoutingAction.new(method_name: :send_command_to_replicas, reply_transformer: SUM_NUM),
            'scan' => RoutingAction.new(method_name: :send_scan_command),
            'lastsave' => RoutingAction.new(method_name: :send_command_to_all_nodes, reply_transformer: SORT_NUMBERS),
            'role' => RoutingAction.new(method_name: :send_command_to_all_nodes),
            'config' => RoutingAction.new(method_name: :send_config_command),
            'client' => RoutingAction.new(method_name: :send_client_command),
            'cluster' => RoutingAction.new(method_name: :send_cluster_command),
            'memory' => RoutingAction.new(method_name: :send_memory_command),
            'script' => RoutingAction.new(method_name: :send_script_command),
            'pubsub' => RoutingAction.new(method_name: :send_pubsub_command),
            'watch' => RoutingAction.new(method_name: :send_watch_command),
            'mget' => multiple_key_action,
            'mset' => multiple_key_action,
            'del' => multiple_key_action,
            'acl' => all_node_first_action,
            'auth' => all_node_first_action,
            'bgrewriteaof' => all_node_first_action,
            'bgsave' => all_node_first_action,
            'quit' => all_node_first_action,
            'save' => all_node_first_action,
            'select' => all_node_first_action,
            'flushall' => primary_first_action,
            'flushdb' => primary_first_action,
            # The redis 7.0 tags RANDOMKEY with `request_policy:all_shards` but without any response policy.
            # It was corrected to `response_policy:special` in the redis 7.2.
            # This entry keeps the historical single node routing for the redis 7.0.
            'randomkey' => single_node_action,
            'readonly' => not_supported_action,
            'readwrite' => not_supported_action,
            'shutdown' => not_supported_action,
            'discard' => keyless_action,
            'exec' => keyless_action,
            'multi' => keyless_action,
            'unwatch' => keyless_action
          }.each_with_object({}) do |(k, v), acc|
            acc[k] = v.freeze
            acc[k.upcase] = v.freeze
          end
        end.call.freeze

        # The routing which the command tips of the COMMAND command reply instruct.
        # The `multi_shard` and the `special` request policies are out of scope.
        # The `special` response policy is also out of scope because the aggregation is undefined,
        # and the fan-out would change the return value of commands such as INFO.
        # The `agg_min`, `agg_max` and `agg_logical_*` response policies are out of scope too:
        # the only reachable command with them today is WAITAOF, whose reply is an array,
        # and the aggregation of array replies is undefined. Such a command falls back to
        # the single node routing until a command with a settled semantics appears.
        # The entries of the DEDICATED_ACTIONS take precedence over these to keep the existing behavior.
        # @see https://redis.io/docs/latest/develop/reference/command-tips/
        POLICY_ACTIONS = {
          'all_shards' => {
            nil => RoutingAction.new(method_name: :send_command_to_primaries).freeze,
            'all_succeeded' => RoutingAction.new(method_name: :send_command_to_primaries, reply_transformer: PICK_FIRST).freeze,
            'one_succeeded' => RoutingAction.new(method_name: :send_command_to_primaries_leniently, reply_transformer: PICK_FIRST).freeze,
            'agg_sum' => RoutingAction.new(method_name: :send_command_to_primaries, reply_transformer: SUM_NUM).freeze
          }.freeze,
          'all_nodes' => {
            nil => RoutingAction.new(method_name: :send_command_to_all_nodes).freeze,
            'all_succeeded' => RoutingAction.new(method_name: :send_command_to_all_nodes, reply_transformer: PICK_FIRST).freeze,
            'one_succeeded' => RoutingAction.new(method_name: :send_command_to_all_nodes_leniently, reply_transformer: PICK_FIRST).freeze,
            'agg_sum' => RoutingAction.new(method_name: :send_command_to_all_nodes, reply_transformer: SUM_NUM).freeze
          }.freeze
        }.freeze

        private_constant :RoutingAction, :PICK_FIRST, :FLATTEN_STRINGS, :SUM_NUM, :SORT_NUMBERS,
                         :DEDICATED_ACTIONS, :POLICY_ACTIONS

        class << self
          # Builds the routing table: the built-in entries overridden by the command routings
          # which the config validated and normalized. A nil routing removes the built-in entry
          # of the command, so that the command follows the default resolution: the command tips
          # which the server reports, or the routing by its key.
          # It raises a BuildError for an unnormalized input as a defense, e.g. an unsupported policy.
          def build(routings)
            return DEDICATED_ACTIONS if routings.nil? || routings.empty?

            routings.each_with_object(DEDICATED_ACTIONS.dup) do |(key, policies), acc|
              merge_action(acc, key, fetch_action(key, policies))
            end.freeze
          end

          def find_policy_action(request_policy, response_policy)
            POLICY_ACTIONS.dig(request_policy, response_policy)
          end

          private

          # Stores both the lowercase and the uppercase keys to avoid a per-call case conversion.
          def merge_action(table, key, action)
            if action.nil?
              table.delete(key)
              table.delete(key.upcase)
            else
              table[key] = action
              table[key.upcase] = action
            end
          end

          def fetch_action(key, policies)
            return if policies.nil?

            action = POLICY_ACTIONS.dig(policies[:request_policy], policies[:response_policy])
            return action unless action.nil?

            raise BuildError, "the policies of the #{key} command are unsupported: #{policies.inspect}"
          end
        end
      end
    end
  end
end
