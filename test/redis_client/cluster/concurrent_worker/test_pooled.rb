# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/concurrent_worker/mixin'

class RedisClient
  class Cluster
    module ConcurrentWorker
      class TestPooled < TestingWrapper
        include Mixin

        private

        def model
          :pooled
        end
      end
    end
  end
end
