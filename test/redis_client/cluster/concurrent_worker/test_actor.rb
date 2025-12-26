# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/concurrent_worker/mixin'

class RedisClient
  class Cluster
    module ConcurrentWorker
      class TestActor < TestingWrapper
        include Mixin if RUBY_ENGINE == 'ruby' && RUBY_ENGINE_VERSION.split('.').take(2).join('.').to_f >= 4.0

        private

        def model
          :actor
        end
      end
    end
  end
end
