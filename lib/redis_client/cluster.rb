# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/pipeline'
require 'redis_client/cluster/pub_sub'
require 'redis_client/cluster/router'

class RedisClient
  class Cluster
    ZERO_CURSOR_FOR_SCAN = '0'

    def initialize(config, pool: nil, **kwargs)
      @router = ::RedisClient::Cluster::Router.new(config, pool: pool, **kwargs)
      @command_builder = config.command_builder
    end

    def inspect
      "#<#{self.class.name} #{@router.node.node_keys.join(', ')}>"
    end

    def call(*command, **kwargs)
      @router.send_command(:call, *command, **kwargs)
    end

    def call_once(*command, **kwargs)
      @router.send_command(:call_once, *command, **kwargs)
    end

    def blocking_call(timeout, *command, **kwargs)
      @router.send_command(:blocking_call, timeout, *command, **kwargs)
    end

    def scan(*args, **kwargs, &block)
      raise ArgumentError, 'block required' unless block

      cursor = ZERO_CURSOR_FOR_SCAN
      loop do
        cursor, keys = @router.scan('SCAN', cursor, *args, **kwargs)
        keys.each(&block)
        break if cursor == ZERO_CURSOR_FOR_SCAN
      end
    end

    def sscan(key, *args, **kwargs, &block)
      node = @router.assign_node('SSCAN', key)
      @router.try_send(node, :sscan, key, *args, **kwargs, &block)
    end

    def hscan(key, *args, **kwargs, &block)
      node = @router.assign_node('HSCAN', key)
      @router.try_send(node, :hscan, key, *args, **kwargs, &block)
    end

    def zscan(key, *args, **kwargs, &block)
      node = @router.assign_node('ZSCAN', key)
      @router.try_send(node, :zscan, key, *args, **kwargs, &block)
    end

    def pipelined
      pipeline = ::RedisClient::Cluster::Pipeline.new(@router)
      yield pipeline
      return [] if pipeline.empty? == 0

      pipeline.execute
    end

    def pubsub
      ::RedisClient::Cluster::PubSub.new(@router)
    end

    def close
      @router.node.call_all(:close)
      nil
    end

    private

    def method_missing(name, *args, **kwargs)
      if @router.command_exists?(name)
        args.unshift(name)
        args = @command_builder.generate!(args, kwargs)
        @router.send_command(:call, *args)
      else
        super
      end
    end

    def respond_to_missing?(name, include_private = false)
      if @router.command_exists?(name)
        true
      else
        super
      end
    end
  end
end
