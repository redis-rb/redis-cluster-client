#!/usr/bin/env ruby
# frozen_string_literal: true

require 'logger'
require 'bundler/setup'
require 'redis_cluster_client'

logger = Logger.new($stdout)

pids = Array.new(10) do
  Process.fork do
    Signal.trap(:INT, 'IGNORE')
    nodes = (6379..6384).map { |port| "redis://127.0.0.1:#{port}" }.freeze
    client = RedisClient.cluster(nodes: nodes, connect_with_original_config: true).new_client
    Signal.trap(:TERM) do
      client.close
      Process.exit
    end

    loop do
      logger.info("#{Process.pid}: reloaded") if client.send(:router).renew_cluster_state
    rescue StandardError => e
      logger.error("#{Process.pid}: #{e.message}")
    ensure
      sleep 0.01
    end
  end
end

Signal.trap(:INT) do
  pids.each { |pid| Process.kill(:TERM, pid) }
end

pids.each do |pid|
  Process.waitpid2(pid)
  logger.info("#{pid}: child process closed")
end

logger.info("#{Process.pid}: parent process closed")
Process.exit
