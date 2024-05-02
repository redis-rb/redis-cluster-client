# frozen_string_literal: true

require 'json'
require 'tmpdir'
require 'stackprof'
require 'redis_cluster_client'
require 'testing_constants'

module ProfStack
  SIZE = 40
  ATTEMPTS = 1000
  ORIGINAL_MGET = (%w[MGET] + Array.new(SIZE) { |i| "{key}#{i}" }).freeze
  EMULATED_MGET = (%w[MGET] + Array.new(SIZE) { |i| "key#{i}" }).freeze

  module_function

  def run
    client = make_client
    mode = ENV.fetch('PROFILE_MODE', :single).to_sym
    prepare(client)
    profile = StackProf.run(mode: :cpu, raw: true) { execute(client, mode) }
    StackProf::Report.new(profile).print_text(false, 40)
  end

  def make_client
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      replica: true,
      replica_affinity: :random,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      **TEST_GENERIC_OPTIONS
    ).new_client
  end

  def prepare(client)
    ATTEMPTS.times do |i|
      client.pipelined do |pi|
        SIZE.times do |j|
          n = SIZE * i + j
          pi.call('SET', "key#{n}", "val#{n}")
          pi.call('SET', "{key}#{n}", "val#{n}")
        end
      end
    end
  end

  def execute(client, mode)
    case mode
    when :single
      (ATTEMPTS * SIZE).times { |i| client.call('GET', "key#{i}") }
    when :excessive_pipelining
      client.pipelined do |pi|
        (ATTEMPTS * SIZE).times { |i| pi.call('GET', "key#{i}") }
      end
    when :pipelining_in_moderation
      ATTEMPTS.times do |i|
        client.pipelined do |pi|
          SIZE.times do |j|
            n = SIZE * i + j
            pi.call('GET', "key#{n}")
          end
        end
      end
    when :original_mget
      ATTEMPTS.times { client.call_v(ORIGINAL_MGET) }
    when :emulated_mget
      ATTEMPTS.times { client.call_v(EMULATED_MGET) }
    else raise ArgumentError, mode
    end
  end
end

ProfStack.run
