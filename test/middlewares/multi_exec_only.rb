# frozen_string_literal: true

module Middlewares
  # Drops pipelined commands outside MULTI/EXEC spans on the wire so tests can
  # assert end state only changes when the full transaction reached Redis.
  module MultiExecOnly
    module Injected
      def write_multi(commands)
        state = multi_exec_only_state
        return super unless state

        state[:dropped] = []
        wire_commands = []
        in_transaction = false

        commands.each_with_index do |cmd, index|
          name = cmd[0].to_s.downcase
          if name == 'multi'
            in_transaction = true
            wire_commands << cmd
          elsif in_transaction
            wire_commands << cmd
            in_transaction = false if name == 'exec'
          else
            state[:dropped] << index
          end
        end
        state[:position] = 0

        super(wire_commands) unless wire_commands.empty?
        nil
      end

      def read(timeout = nil)
        state = multi_exec_only_state
        return super(timeout) unless state

        state[:position] += 1
        if state[:dropped].include?(state[:position] - 1)
          nil
        else
          super(timeout)
        end
      end

      private

      def multi_exec_only_state
        return nil unless Thread.current[:multi_exec_only_enabled]

        registry = Thread.current[:multi_exec_only_registry] ||= {}
        registry[self] ||= { dropped: [], position: 0 }
      end
    end

    module_function

    def install!
      return if @installed

      ::RedisClient::RubyConnection.prepend(Injected)
      @installed = true
    end

    def with
      install!
      Thread.current[:multi_exec_only_enabled] = true
      Thread.current[:multi_exec_only_registry] = {}
      yield
    ensure
      Thread.current[:multi_exec_only_enabled] = nil
      Thread.current[:multi_exec_only_registry] = nil
    end
  end
end
