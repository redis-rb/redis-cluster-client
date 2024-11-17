# frozen_string_literal: true

require 'vernier'
require 'redis_cluster_client'
require 'testing_constants'

module ProfStack2
  SIZE = 40
  ATTEMPTS = 1000

  module_function

  def run
    client = make_client
    prepare(client)

    case mode = ENV.fetch('PROFILE_MODE', :single).to_sym
    when :single
      execute(client, mode) do |client|
        ATTEMPTS.times { |i| client.call('GET', "key#{i}") }
      end
    when :transaction
      execute(client, mode) do |client|
        ATTEMPTS.times do |i|
          client.multi do |tx|
            SIZE.times do |j|
              n = SIZE * i + j
              tx.call('SET', "{group:#{i}}:key:#{n}", n)
            end
          end
        end
      end
    when :pipeline
      execute(client, mode) do |client|
        ATTEMPTS.times do |i|
          client.pipelined do |pi|
            SIZE.times do |j|
              n = SIZE * i + j
              pi.call('GET', "key#{n}")
            end
          end
        end
      end
    end
  end

  def execute(client, mode)
    Vernier.profile(out: "vernier_#{mode}.json") do
      yield(client)
    end
  end

  def make_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      replica_affinity: :random,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      # concurrency: { model: :on_demand, size: 6 },
      # concurrency: { model: :pooled, size: 6 },
      concurrency: { model: :none },
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def prepare(client)
    ATTEMPTS.times do |i|
      client.pipelined do |pi|
        SIZE.times do |j|
          n = SIZE * i + j
          pi.call('SET', "key#{n}", "val#{n}")
        end
      end
    end
  end
end

ProfStack2.run
