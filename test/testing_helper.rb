# frozen_string_literal: true

require 'minitest/autorun'
require 'redis_client'

class RedisClient
  module TestingHelper
    NODE_ADDRS = (6379..6384).map { |port| "#{ENV.fetch('REDIS_SCHEME', 'redis')}://127.0.0.1:#{port}" }.freeze

    def setup
      @raw_clients = NODE_ADDRS.map { |addr| ::RedisClient.config(url: addr).new_client }
    end

    def teardown
      @raw_clients&.each(&:close)
    end
  end
end
