# frozen_string_literal: true

# @see https://ruby-doc.org/stdlib-3.0.1/libdoc/minitest/rdoc/Minitest/Benchmark.html

require 'minitest/benchmark'
require 'minitest/autorun'
require 'redis_cluster_client'
require 'testing_constants'

case ENV.fetch('REDIS_CONNECTION_DRIVER', 'ruby')
when 'hiredis' then require 'hiredis-client'
end

class BenchmarkWrapper < Minitest::Benchmark
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
