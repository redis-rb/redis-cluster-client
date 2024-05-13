# frozen_string_literal: true

require 'redis_client/cluster/concurrent_worker'
require 'redis_client/cluster/pipeline'
require 'redis_client/cluster/pub_sub'
require 'redis_client/cluster/router'
require 'redis_client/cluster/transaction'
require 'redis_client/cluster/optimistic_locking'

class RedisClient
  class Cluster
    ZERO_CURSOR_FOR_SCAN = '0'

    attr_reader :config

    def initialize(config, pool: nil, concurrency: nil, **kwargs)
      @config = config
      @concurrent_worker = ::RedisClient::Cluster::ConcurrentWorker.create(**(concurrency || {}))
      @command_builder = config.command_builder
      @pool = pool
      @kwargs = kwargs
    end

    def inspect
      "#<#{self.class.name} #{router.node_keys.join(', ')}>"
    end

    def call(*args, **kwargs, &block)
      command = @command_builder.generate(args, kwargs)
      router.send_command(:call_v, command, &block)
    end

    def call_v(command, &block)
      command = @command_builder.generate(command)
      router.send_command(:call_v, command, &block)
    end

    def call_once(*args, **kwargs, &block)
      command = @command_builder.generate(args, kwargs)
      router.send_command(:call_once_v, command, &block)
    end

    def call_once_v(command, &block)
      command = @command_builder.generate(command)
      router.send_command(:call_once_v, command, &block)
    end

    def blocking_call(timeout, *args, **kwargs, &block)
      command = @command_builder.generate(args, kwargs)
      router.send_command(:blocking_call_v, command, timeout, &block)
    end

    def blocking_call_v(timeout, command, &block)
      command = @command_builder.generate(command)
      router.send_command(:blocking_call_v, command, timeout, &block)
    end

    def scan(*args, **kwargs, &block)
      raise ArgumentError, 'block required' unless block

      seed = Random.new_seed
      cursor = ZERO_CURSOR_FOR_SCAN
      loop do
        cursor, keys = router.scan('SCAN', cursor, *args, seed: seed, **kwargs)
        keys.each(&block)
        break if cursor == ZERO_CURSOR_FOR_SCAN
      end
    end

    def sscan(key, *args, **kwargs, &block)
      node = router.assign_node(['SSCAN', key])
      router.try_delegate(node, :sscan, key, *args, **kwargs, &block)
    end

    def hscan(key, *args, **kwargs, &block)
      node = router.assign_node(['HSCAN', key])
      router.try_delegate(node, :hscan, key, *args, **kwargs, &block)
    end

    def zscan(key, *args, **kwargs, &block)
      node = router.assign_node(['ZSCAN', key])
      router.try_delegate(node, :zscan, key, *args, **kwargs, &block)
    end

    def pipelined(exception: true)
      seed = @config.use_replica? && @config.replica_affinity == :random ? nil : Random.new_seed
      pipeline = ::RedisClient::Cluster::Pipeline.new(
        router,
        @command_builder,
        @concurrent_worker,
        exception: exception,
        seed: seed
      )

      yield pipeline
      return [] if pipeline.empty?

      pipeline.execute
    end

    def multi(watch: nil)
      if watch.nil? || watch.empty?
        transaction = ::RedisClient::Cluster::Transaction.new(router, @command_builder)
        yield transaction
        return transaction.execute
      end

      ::RedisClient::Cluster::OptimisticLocking.new(router).watch(watch) do |c, slot, asking|
        transaction = ::RedisClient::Cluster::Transaction.new(
          router, @command_builder, node: c, slot: slot, asking: asking
        )
        yield transaction
        transaction.execute
      end
    end

    def pubsub
      ::RedisClient::Cluster::PubSub.new(router, @command_builder)
    end

    def with(...)
      raise NotImplementedError, 'No way to use'
    end

    def close
      @router&.close
      @concurrent_worker.close
      nil
    end

    private

    def router
      @router ||= ::RedisClient::Cluster::Router.new(@config, @concurrent_worker, pool: @pool, **@kwargs)
    end

    def method_missing(name, *args, **kwargs, &block)
      if router.command_exists?(name)
        args.unshift(name)
        command = @command_builder.generate(args, kwargs)
        return router.send_command(:call_v, command, &block)
      end

      super
    end

    def respond_to_missing?(name, include_private = false)
      return true if router.command_exists?(name)

      super
    end
  end
end
