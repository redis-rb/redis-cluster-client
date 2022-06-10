# frozen_string_literal: true

require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    class Node
      include Enumerable

      SLOT_SIZE = 16_384
      ReloadNeeded = Class.new(StandardError)

      class << self
        def load_info(options, **kwargs)
          tmp_nodes = ::RedisClient::Cluster::Node.new(options, **kwargs)

          errors = tmp_nodes.map do |tmp_node|
            reply = tmp_node.call(%w[CLUSTER NODES])
            return parse_node_info(reply)
          rescue ::RedisClient::ConnectionError, ::RedisClient::CommandError => e
            e
          end

          raise ::RedisClient::Cluster::InitialSetupError, errors
        ensure
          tmp_nodes&.each(&:close)
        end

        private

        # @see https://redis.io/commands/cluster-nodes/
        def parse_node_info(info) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          info.split("\n").map(&:split)
              .map { |arr| arr[2] = arr[2].split(',') }
              .select { |arr| arr[7] == 'connected' && (arr[2] & %w[fail? fail handshake noaddr noflags]).empty? }
              .map { |arr| arr[2] = (arr[2] & %w[master slave]).first }
              .map { |arr| arr[1] = arr[1].split('@').first }
              .map { |arr| arr[8] = arr[8].nil? ? [] : arr[8].split(',').map { |r| r.split('-') } }
        end
      end

      def initialize(options, node_info = {}, pool = nil, with_replica: false, **kwargs)
        @with_replica = with_replica
        @slots = build_slot_node_mappings(node_info)
        @replications = build_replication_mappings(node_info)
        @clients = build_clients(options, pool, **kwargs)
      end

      def each(&block)
        @clients.values.each(&block)
      end

      def sample
        @clients.values.sample
      end

      def find_by(node_key)
        @clients.fetch(node_key)
      rescue KeyError
        raise ReloadNeeded
      end

      def call_all(command, &block)
        try_map { |_, client| client.call(command, &block) }.values
      end

      def call_master(command, &block)
        try_map do |node_key, client|
          next if slave?(node_key)

          client.call(command, &block)
        end.values
      end

      def call_slave(command, &block)
        return call_master(command, &block) if replica_disabled?

        try_map do |node_key, client|
          next if master?(node_key)

          client.call(command, &block)
        end.values
      end

      def process_all(commands, &block)
        try_map { |_, client| client.process(commands, &block) }.values
      end

      def scale_reading_clients
        reading_clients = []

        @clients.each do |node_key, client|
          next unless replica_disabled? ? master?(node_key) : slave?(node_key)

          reading_clients << client
        end

        reading_clients
      end

      def slot_exists?(slot)
        !@slots[slot].nil?
      end

      def find_node_key_of_master(slot)
        @slots[slot]
      end

      def find_node_key_of_slave(slot)
        return @slots[slot] if replica_disabled?

        @replications[@slots[slot]].sample
      end

      def update_slot(slot, node_key)
        @slots[slot] = node_key
      end

      private

      def replica_disabled?
        !@with_replica
      end

      def master?(node_key)
        !slave?(node_key)
      end

      def slave?(node_key)
        @replications[node_key].size.zero?
      end

      def build_clients(options, pool, **kwargs)
        options.filter_map do |node_key, option|
          next if replica_disabled? && slave?(node_key)

          config = ::RedisClient.config(option)
          client = pool.nil? ? config.new_client(**kwargs) : config.new_pool(**pool, **kwargs)
          client.call('READONLY') if slave?(node_key) # FIXME: Send every pooled conns

          [node_key, client]
        end.to_h
      end

      def build_slot_node_mappings(node_info)
        slots = Array.new(SLOT_SIZE)
        node_info.each do |arr|
          next if arr[8].nil? || arr[8].empty?

          arr[8].each do |start, last|
            (start..last).each { |i| slots[i] = arr[1] }
          end
        end

        slots
      end

      def build_replication_mappings(node_info)
        dict = node_info.to_h { |arr| [arr[0], arr] }
        node_info.each_with_object(Hash.new { |h, k| h[k] = [] }) do |arr, acc|
          primary_info = dict[arr[3]]
          acc[primary_info[1]] << arr[1] unless primary_info.nil?
          acc[arr[1]]
        end
      end

      def try_map # rubocop:disable Metrics/MethodLength
        errors = {}
        results = {}

        @clients.each do |node_key, client|
          reply = yield(node_key, client)
          results[node_key] = reply unless reply.nil?
        rescue ::RedisClient::CommandError => e
          errors[node_key] = e
          next
        end

        return results if errors.empty?

        raise ::RedisClient::Cluster::CommandErrorCollection, errors
      end
    end
  end
end
