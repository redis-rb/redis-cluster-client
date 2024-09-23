# frozen_string_literal: true

module Middlewares
  module RedirectFake
    Setting = Struct.new(
      'RedirectFakeSetting',
      :slot, :to, :command, keyword_init: true
    )

    def call(cmd, cfg)
      s = cfg.custom.fetch(:redirect_fake)
      raise RedisClient::CommandError, "MOVED #{s.slot} #{s.to}" if cmd == s.command

      super
    end

    def call_pipelined(cmd, cfg)
      s = cfg.custom.fetch(:redirect_fake)
      raise RedisClient::CommandError, "MOVED #{s.slot} #{s.to}" if cmd == s.command

      super
    end
  end
end
