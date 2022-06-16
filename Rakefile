# frozen_string_literal: true

require 'rake/testtask'

task default: :test

Rake::TestTask.new :test do |t|
  t.libs << :test
  t.libs << :lib
  t.test_files = ARGV.size == 1 ? Dir['test/**/test_*.rb'] : ARGV[1..]
  t.options = '-v'
end

desc 'Wait for cluster to be ready'
task :wait do
  $LOAD_PATH.unshift(File.expand_path('test', __dir__))
  require 'testing_helper'
  ::TestingHelper::ClusterController.new(
    ::TestingHelper::TEST_NODE_URIS,
    **::TestingHelper::TEST_GENERIC_OPTIONS
  ).wait_for_cluster_to_be_ready
end
