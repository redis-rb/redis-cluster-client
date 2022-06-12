# frozen_string_literal: true

require 'rake/testtask'

task default: :test

Rake::TestTask.new :test do |t|
  t.libs << :test
  t.libs << :lib
  t.test_files = Dir['test/**/test_*.rb']
end

desc 'Wait for cluster to be ready'
task :wait do
  $LOAD_PATH.unshift(File.expand_path('test', __dir__))
  require 'redis_client/cluster/controller'
  nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }
  ctrl = ::RedisClient::Cluster::Controller.new(nodes)
  ctrl.wait_for_cluster_to_be_ready
end
