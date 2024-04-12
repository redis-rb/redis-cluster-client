# frozen_string_literal: true

module RedirectionEmulationMiddleware
  def call(cmd, cfg)
    r = cfg.custom[:redirect]
    raise RedisClient::CommandError, "MOVED #{r[:slot]} #{r[:to]}" if cmd == r[:command]

    super
  end

  def call_pipelined(cmd, cfg)
    r = cfg.custom[:redirect]
    raise RedisClient::CommandError, "MOVED #{r[:slot]} #{r[:to]}" if cmd == r[:command]

    super
  end
end
