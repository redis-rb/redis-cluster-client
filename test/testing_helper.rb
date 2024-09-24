# frozen_string_literal: true

# @see https://docs.ruby-lang.org/en/2.1.0/MiniTest/Assertions.html

require 'minitest/autorun'
require 'redis-cluster-client'
require 'testing_constants'
require 'cluster_controller'
require 'middlewares/command_capture'
require 'middlewares/redirect_count'
require 'middlewares/redirect_fake'

case ENV.fetch('REDIS_CONNECTION_DRIVER', 'ruby')
when 'hiredis' then require 'hiredis-client'
end

MaxRetryExceeded = Class.new(StandardError)

class TestingWrapper < Minitest::Test
  private

  def swap_timeout(client, timeout:)
    return if client.nil?

    node = client.instance_variable_get(:@router)&.instance_variable_get(:@node)
    raise 'The client must be initialized.' if node.nil?

    updater = lambda do |c, t|
      c.read_timeout = t
      c.config.instance_variable_set(:@read_timeout, t)
    end

    regular_timeout = node.first.read_timeout
    node.each { |cli| updater.call(cli, timeout) }
    result = yield client
    node.each { |cli| updater.call(cli, regular_timeout) }
    result
  end
end
