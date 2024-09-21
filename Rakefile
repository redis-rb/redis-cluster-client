# frozen_string_literal: true

require 'rake/testtask'
require 'rubocop/rake_task'
require 'bundler/gem_helper'

RuboCop::RakeTask.new
Bundler::GemHelper.install_tasks

SLUGGISH_TEST_TYPES = %w[broken scale state].freeze

task default: :test

Rake::TestTask.new(:test) do |t|
  t.libs << :lib
  t.libs << :test
  t.options = '-v -c'
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
    pattern = ''
    pattern = "-n #{ARGV[1]}" if ARGV.size > 1
    t.options = "-v -c #{pattern}"
    t.test_files = ["test/test_against_cluster_#{type}.rb"]
  end
end

%i[bench ips prof].each do |k|
  Rake::TestTask.new(k) do |t|
    t.libs << :lib
    t.libs << :test
    t.options = '-v'
    t.warning = false
    t.test_files = ARGV.size > 1 ? ARGV[1..] : Dir["test/**/#{k}_*.rb"]
  end
end

desc 'Wait for cluster to be ready'
task :wait do
  $LOAD_PATH.unshift(File.expand_path('test', __dir__))
  require 'testing_constants'
  require 'cluster_controller'
  ::ClusterController.new(
    TEST_NODE_URIS,
    shard_size: TEST_SHARD_SIZE,
    replica_size: TEST_REPLICA_SIZE,
    **TEST_GENERIC_OPTIONS
  ).wait_for_cluster_to_be_ready
end

desc 'Build cluster'
task :build_cluster, %i[addr1 addr2] do |_, args|
  $LOAD_PATH.unshift(File.expand_path('test', __dir__))
  require 'cluster_controller'
  hosts = args.values_at(:addr1, :addr2).compact
  ports = (6379..6384).to_a
  nodes = hosts.product(ports).map { |host, port| "redis://#{host}:#{port}" }
  shard_size = 3
  replica_size = (nodes.size / shard_size) - 1
  ::ClusterController.new(
    nodes,
    shard_size: shard_size,
    replica_size: replica_size,
    timeout: 30.0
  ).rebuild
end

desc 'Build cluster for benchmark'
task :build_cluster_for_bench do
  $LOAD_PATH.unshift(File.expand_path('test', __dir__))
  require 'cluster_controller'
  nodes = (6379..6387).map { |port| "redis://127.0.0.1:#{port}" }
  ::ClusterController.new(nodes, shard_size: 3, replica_size: 2, timeout: 30.0).rebuild
end
