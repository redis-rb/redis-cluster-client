# frozen_string_literal: true

# @see https://ruby-doc.org/stdlib-3.0.1/libdoc/minitest/rdoc/Minitest/Benchmark.html

require 'minitest/benchmark'
require 'minitest/autorun'
require 'redis_cluster_client'
require 'testing_constants'

case ENV.fetch('REDIS_CONNECTION_DRIVER', 'ruby')
when 'hiredis' then require 'hiredis-client'
end

class BenchmarkWrapper < Minitest::Benchmark; end
