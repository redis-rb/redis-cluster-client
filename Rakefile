# frozen_string_literal: true

require 'rake/testtask'

task default: :test

Rake::TestTask.new :test do |t|
  t.libs << :test
  t.libs << :lib
  files = Dir['test/**/test_*.rb'].grep_v(/test_against_cluster_state/)
  files = ARGV[1..] if ARGV.size > 1
  t.test_files = files
  t.options = '-v'
end

Rake::TestTask.new :test_cluster_state do |t|
  t.libs << :test
  t.libs << :lib
  t.test_files = %w[test/test_against_cluster_state.rb]
  t.options = '-v'
end

desc 'Wait for cluster to be ready'
task :wait do
  $LOAD_PATH.unshift(File.expand_path('test', __dir__))
  require 'testing_constants'
  require 'cluster_controller'
  ::ClusterController.new(
    TEST_NODE_URIS,
    replica_size: TEST_REPLICA_SIZE,
    **TEST_GENERIC_OPTIONS
  ).wait_for_cluster_to_be_ready
end
