# frozen_string_literal: true

module RedirectionEmulationMiddleware
  Setting = Struct.new(
    'RedirectionEmulationMiddlewareSetting',
    :slot, :to, :command, keyword_init: true
  )

  def call(cmd, cfg)
    s = cfg.custom.fetch(:redirect)
    raise RedisClient::CommandError, "MOVED #{s.slot} #{s.to}" if cmd == s.command

    super
  end

  def call_pipelined(cmd, cfg)
    s = cfg.custom.fetch(:redirect)
    raise RedisClient::CommandError, "MOVED #{s.slot} #{s.to}" if cmd == s.command

    super
  end
end
