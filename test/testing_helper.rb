# frozen_string_literal: true

# @see https://docs.ruby-lang.org/en/2.1.0/MiniTest/Assertions.html

require 'minitest/autorun'
require 'redis_client'
require 'redis_cluster_client'
require 'testing_constants'
require 'cluster_controller'

case ENV.fetch('REDIS_CONNECTION_DRIVER', 'ruby')
when 'hiredis' then require 'hiredis-client'
end

class TestingWrapper < Minitest::Test; end
