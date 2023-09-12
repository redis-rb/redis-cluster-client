# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/concurrent_worker/mixin'

class RedisClient
  class Cluster
    module ConcurrentWorker
      class TestOnDemand < TestingWrapper
        include Mixin

        private

        def model
          :on_demand
        end
      end
    end
  end
end
