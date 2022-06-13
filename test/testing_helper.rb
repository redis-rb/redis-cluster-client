# frozen_string_literal: true

require 'minitest/autorun'
require 'redis_client'

class RedisClient
  module TestingHelper
    REDIS_SCHEME = ENV.fetch('REDIS_SCHEME', 'redis')
    NODE_ADDRS = (7000..7005).map { |port| "#{REDIS_SCHEME}://127.0.0.1:#{port}" }.freeze

    def setup
      @clients = NODE_ADDRS.map { |addr| ::RedisClient.config(url: addr).new_client }
    end

    def teardown
      @clients&.each(&:close)
    end
  end
end
