# frozen_string_literal: true

module Middlewares
  # Drops matching commands on the wire and returns MOVED on +read+ so cluster
  # +call_pipelined_aware_of_redirection+ handles redirection like a real node.
  # When the match falls inside MULTI/EXEC, EXEC is replaced with DISCARD on the wire
  # (so the connection is not left in MULTI) and the EXEC reply is replaced with EXECABORT.
  module RedirectReadInject
    Setting = Struct.new(
      'RedirectReadInjectSetting',
      :slot, :to, :command, :once, keyword_init: true
    )

    module Injected
      def write_multi(commands)
        context = Thread.current[:redirect_read_inject_context]
        return super unless context

        setting = context[:setting]
        injections = []
        wire_commands = []
        in_transaction = false
        matched_in_transaction = false

        commands.each do |cmd|
          name = cmd[0].to_s.downcase
          if name == 'multi'
            in_transaction = true
            wire_commands << cmd
            injections << nil
          elsif name == 'exec'
            if matched_in_transaction
              wire_commands << %w[DISCARD]
              injections << :execabort
            else
              wire_commands << cmd
              injections << nil
            end
            in_transaction = false
            matched_in_transaction = false
          elsif ::Middlewares::RedirectReadInject.matching_command?(cmd, setting)
            injections << :moved
            matched_in_transaction = true if in_transaction
          else
            wire_commands << cmd
            injections << nil
          end
        end

        connection_registry[self] = {
          injections: injections,
          position: 0,
          redirect_count: context[:redirect_count],
          setting: setting
        }
        super(wire_commands) unless wire_commands.empty?
        nil
      end

      def read(timeout = nil)
        state = connection_registry[self]
        return super(timeout) unless state

        index = state[:position]
        state[:position] += 1
        injection = state[:injections][index]
        case injection
        when :moved
          state[:redirect_count]&.moved
          s = state[:setting]
          ::RedisClient::CommandError.new("MOVED #{s.slot} #{s.to}")
        when :execabort
          super(timeout)
          ::RedisClient::CommandError.new('EXECABORT')
        else
          super(timeout)
        end
      end

      private

      def connection_registry
        Thread.current[:redirect_read_inject_registry] ||= {}
      end
    end

    def call_pipelined(commands, cfg)
      setting = cfg.custom[:redirect_read_inject]
      return super unless setting

      ::Middlewares::RedirectReadInject.install!
      Thread.current[:redirect_read_inject_context] = {
        setting: setting,
        redirect_count: cfg.custom[:redirect_count]
      }

      super
    ensure
      Thread.current[:redirect_read_inject_context] = nil
      Thread.current[:redirect_read_inject_registry] = nil
    end

    module_function

    def install!
      return if @installed

      ::RedisClient::RubyConnection.prepend(Injected)
      @installed = true
    end

    def matching_command?(cmd, setting)
      return false unless cmd == setting.command

      if setting.once
        registry = Thread.current[:redirect_read_inject_once] ||= {}
        return false if registry[setting]

        registry[setting] = true
      end

      true
    end
  end
end
