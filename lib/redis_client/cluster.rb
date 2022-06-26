# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/pipeline'
require 'redis_client/cluster/pubsub'
require 'redis_client/cluster/router'

class RedisClient
  class Cluster
    ZERO_CURSOR_FOR_SCAN = '0'
    CMD_SCAN = 'SCAN'
    CMD_SSCAN = 'SSCAN'
    CMD_HSCAN = 'HSCAN'
    CMD_ZSCAN = 'ZSCAN'

    def initialize(config, pool: nil, **kwargs)
      @router = ::RedisClient::Cluster::Router.new(config, pool: pool, **kwargs)
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
        cursor, keys = @router.scan(CMD_SCAN, cursor, *args, **kwargs)
        keys.each(&block)
        break if cursor == ZERO_CURSOR_FOR_SCAN
      end
    end

    def sscan(key, *args, **kwargs, &block)
      node = @router.assign_node(CMD_SSCAN, key)
      @router.try_send(node, :sscan, key, *args, **kwargs, &block)
    end

    def hscan(key, *args, **kwargs, &block)
      node = @router.assign_node(CMD_HSCAN, key)
      @router.try_send(node, :hscan, key, *args, **kwargs, &block)
    end

    def zscan(key, *args, **kwargs, &block)
      node = @router.assign_node(CMD_ZSCAN, key)
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
  end
end
