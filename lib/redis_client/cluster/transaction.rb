# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    class Transaction
      ConsistencyError = Class.new(::RedisClient::Error)
      STARTING_COMMANDS = %w[watch].freeze
      COMPLETING_COMMANDS = %w[exec discard unwatch].freeze

      def self.command_starts_transaction?(command)
        STARTING_COMMANDS.include?(::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command))
      end

      def self.command_ends_transaction?(command)
        COMPLETING_COMMANDS.include?(::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command))
      end

      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @node_key = nil
        @node = nil
        @pool = nil
        @state = :unstarted
      end

      attr_reader :node

      def send_command(method, command, *args, &block)
        # Force-disable retries in transactions
        method = make_method_once(method)

        # redis-rb wants to do this when it probably doesn't need to.
        cmd = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        if complete?
          return if COMPLETING_COMMANDS.include?(cmd)

          raise ArgumentError, 'Transaction is already complete'
        end

        begin
          ensure_same_node command
          ret = @router.try_send(@node, method, command, args, retry_count: 0, &block)
        rescue ::RedisClient::ConnectionError
          mark_complete # Abort transaction on connection errors
          raise
        rescue StandardError
          # Abort transaction if the first command is a failure.
          mark_complete if @state == :unstarted
          raise
        else
          @state = :in_progress
          mark_complete if COMPLETING_COMMANDS.include?(cmd)
          ret
        end
      end

      def complete?
        # The router closes the node on ConnectionError.
        mark_complete if @state != :complete && @node && !@node.connected?
        @state == :complete
      end

      def multi(watch: nil) # rubocop:disable Metrics/AbcSize
        send_command(:call_once_v, ['WATCH', *watch]) if watch&.any?

        begin
          command_buffer = MultiBuffer.new
          yield command_buffer
          return [] unless command_buffer.commands.any?

          command_buffer.commands.each { |command| ensure_same_node(command) }
          res = execute_commands_pipelined(command_buffer.commands)
          # Because we directly called MULTI ... EXEC on the underlying node through the pipeline,
          # and not in send_command,
          mark_complete
          res.last
        ensure
          send_command(:call_once_v, ['UNWATCH']) if watch&.any? && !complete?
        end
      end

      def ensure_same_node(command)
        node_key = @router.find_primary_node_key(command)
        if node_key.nil?
          # If ome previous command worked out what node to use, this is OK.
          raise ConsistencyError, "Client couldn't determine the node to be executed the transaction by: #{command}" unless @node
        elsif @node.nil?
          # This is the first node key we've seen
          @node = @router.find_node(node_key)
          @node_key = node_key
          if @node.respond_to?(:pool, true)
            # This is a hack, but we need to check out a connection from the pool and keep it checked out until we unwatch,
            # if we're using a Pooled backend. Otherwise, we might not run commands on the same session we watched on.
            # Note that ConnectionPool keeps a reference to this connection in a threadlocal, so we don't need to actually _use_ it
            # explicitly; it'll get returned from #with like normal.
            @pool = @node.send(:pool)
            @node = @pool.checkout
          end
        elsif node_key != @node_key
          raise ConsistencyError, "The transaction should be done for single node: #{@node_key}, #{node_key}"
        end
      end

      private

      def mark_complete
        @pool&.checkin
        @pool = nil
        @node = nil
        @state = :complete
      end

      def make_method_once(method)
        case method
        when :call
          :call_once
        when :call_v
          :call_once_v
        else
          method
        end
      end

      def execute_commands_pipelined(commands)
        @router.try_delegate(@node, :pipelined, retry_count: 0) do |p|
          p.call_once_v ['MULTI']
          commands.each do |command|
            p.call_once_v command
          end
          p.call_once_v ['EXEC']
        end
      end

      class MultiBuffer
        def initialize
          @commands = []
        end

        def call(*command, **_kwargs, &_)
          @commands << command
          nil
        end

        def call_v(command, &_)
          @commands << command
          nil
        end

        alias call_once call
        alias call_once_v call_v
        attr_accessor :commands
      end
    end
  end
end
