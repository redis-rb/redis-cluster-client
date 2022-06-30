# frozen_string_literal: true

require 'rake/testtask'

SLUGGISH_TEST_TYPES = %w[broken scale state].freeze

task default: :test

Rake::TestTask.new(:test) do |t|
  t.libs << :lib
  t.libs << :test
  t.options = '-v'
  t.test_files = if ARGV.size > 1
                   ARGV[1..]
                 else
                   Dir['test/**/test_*.rb'].grep_v(/test_against_cluster_(#{SLUGGISH_TEST_TYPES.join('|')})/)
                 end
end

SLUGGISH_TEST_TYPES.each do |type|
  Rake::TestTask.new("test_cluster_#{type}".to_sym) do |t|
    t.libs << :lib
    t.libs << :test
    t.options = '-v'
    t.test_files = ["test/test_against_cluster_#{type}.rb"]
  end
end

Rake::TestTask.new(:bench) do |t|
  t.libs << :lib
  t.libs << :test
  t.warning = false
  t.test_files = ARGV.size > 1 ? ARGV[1..] : Dir['test/**/bench_*.rb']
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
