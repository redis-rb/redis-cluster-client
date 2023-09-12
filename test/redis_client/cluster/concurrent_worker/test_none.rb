# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/concurrent_worker/mixin'

class RedisClient
  class Cluster
    module ConcurrentWorker
      class TestNone < TestingWrapper
        include Mixin

        private

        def model
          :none
        end
      end
    end
  end
end
