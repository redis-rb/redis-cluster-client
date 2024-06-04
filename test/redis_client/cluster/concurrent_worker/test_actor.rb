# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/concurrent_worker/mixin'

class RedisClient
  class Cluster
    module ConcurrentWorker
      class TestActor < TestingWrapper
        include Mixin if RUBY_ENGINE == 'ruby' && TEST_RUBY_MAJOR_VERSION >= 3

        private

        def model
          :actor
        end
      end
    end
  end
end
