# frozen_string_literal: true

require 'redis_client/cluster_config'

class RedisClient
  class << self
    def cluster(**kwargs)
      ClusterConfig.new(**kwargs)
    end
  end
end
