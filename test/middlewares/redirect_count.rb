# frozen_string_literal: true

module Middlewares
  module RedirectCount
    class Counter
      Result = Struct.new('RedirectCountResult', :moved, :ask, keyword_init: true)

      def initialize
        @moved = 0
        @ask = 0
        @mutex = Mutex.new
      end

      def moved
        @mutex.synchronize { @moved += 1 }
      end

      def ask
        @mutex.synchronize { @ask += 1 }
      end

      def get
        @mutex.synchronize { Result.new(moved: @moved, ask: @ask) }
      end

      def zero?
        @mutex.synchronize { @moved == 0 && @ask == 0 }
      end

      def clear
        @mutex.synchronize do
          @moved = 0
          @ask = 0
        end
      end
    end

    def call(cmd, cfg)
      super
    rescue ::RedisClient::CommandError => e
      if e.message.start_with?('MOVED')
        cfg.custom.fetch(:redirect_count).moved
      elsif e.message.start_with?('ASK')
        cfg.custom.fetch(:redirect_count).ask
      end

      raise
    end

    def call_pipelined(cmd, cfg)
      super
    rescue ::RedisClient::CommandError => e
      if e.message.start_with?('MOVED')
        cfg.custom.fetch(:redirect_count).moved
      elsif e.message.start_with?('ASK')
        cfg.custom.fetch(:redirect_count).ask
      end

      raise
    end
  end
end
