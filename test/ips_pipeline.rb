# frozen_string_literal: true

require 'benchmark/ips'
require 'redis_cluster_client'
require 'testing_constants'
require 'hiredis-client'
require 'async/redis/cluster_client'
require 'valkey'

module IpsPipeline
  module_function

  ATTEMPTS = 100

  def run
    print "################################################################################\n"
    print "# Pipeline\n"
    print "################################################################################\n"
    print "\n"

    keys = Array.new(ATTEMPTS) { |i| "key#{i}" }
    client = make_client(:none)

    client.pipelined do |pi|
      keys.each { |key| pi.call('set', key, key) }
    end

    clients = {
      none: client,
      pooled: make_client(:pooled),
      ondemand: make_client(:on_demand),
      envoy: make_client_for_envoy,
      cproxy: make_client_for_cluster_proxy
    }
    valkey = make_valkey_client

    Benchmark.ips do |x|
      x.time = 5
      x.warmup = 1

      clients.each do |sbj, cli|
        x.report(sbj) do
          cli.pipelined do |pi|
            keys.each { |key| pi.call('get', key) }
          end
        end
      end

      x.report('valkey') do
        valkey.pipelined do |pi|
          keys.each { |key| pi.get(key) }
        end
      end

      x.compare!
    end

    Sync do |parent_task|
      async = make_async_client

      Benchmark.ips do |x|
        x.time = 5
        x.warmup = 1

        x.report('async') do
          async.pipeline(task: parent_task) do |context|
            keys.each { |key| context.get(key) }
            context.collect
          end
        end

        x.compare!
      end
    end
  end

  def make_client(model)
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      concurrency: { model: model },
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def make_client_for_envoy
    ::RedisClient.config(
      **TEST_GENERIC_OPTIONS.merge(BENCH_ENVOY_OPTIONS)
    ).new_client
  end

  def make_client_for_cluster_proxy
    ::RedisClient.config(
      **TEST_GENERIC_OPTIONS.merge(BENCH_REDIS_CLUSTER_PROXY_OPTIONS)
    ).new_client
  end

  def make_async_client
    endpoints = TEST_NODE_URIS.map { |e| ::Async::Redis::Endpoint.parse(e) }
    ::Async::Redis::ClusterClient.new(endpoints)
  end

  def make_valkey_client
    ::Valkey.new(
      nodes: [{ host: TEST_REDIS_HOST, port: TEST_REDIS_PORT }],
      timeout: TEST_TIMEOUT_SEC,
      cluster_mode: true,
      protocol: 3
    )
  end
end

module Async
  module Redis
    class ClusterClient
      class DummyContext
        def initialize(cluster, task: ::Async::Task.current)
          @cluster = cluster
          @parent_task = task
          @clients = {}
          @keys = {}
          @indices = {}
          @idx = 0
        end

        def get(key)
          slot = @cluster.slot_for(key)
          client = @cluster.client_for(slot)
          id = client.endpoint.to_s
          @clients[id] ||= client
          keys = (@keys[id] ||= [])
          keys << key
          indices = (@indices[id] ||= [])
          indices << @idx
          @idx += 1
        end

        def collect
          tasks = @clients.to_h do |id, client|
            task = @parent_task.async do |_child_task|
              client.pipeline do |context|
                @keys.fetch(id).each { |key| context.get(key) }
                context.collect
              end
            end

            [id, task]
          end

          tasks.each_with_object(Array.new(@idx)) do |(id, task), acc|
            indices = @indices.fetch(id)
            task.wait.each_with_index { |r, i| acc[indices[i]] = r }
          end
        end
      end

      unless method_defined? :pipeline
        def pipeline(task: ::Async::Task.current)
          ctx = DummyContext.new(self, task: task)
          yield(ctx)
        end
      end
    end
  end
end

IpsPipeline.run
