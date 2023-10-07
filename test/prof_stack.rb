# frozen_string_literal: true

require 'json'
require 'tmpdir'
require 'stackprof'
require 'redis_cluster_client'
require 'testing_constants'

module ProfStack
  PIPELINE_SIZE = 100
  ATTEMPTS = 1000

  module_function

  def run
    client = make_client

    client.pipelined do |pi|
      PIPELINE_SIZE.times do |i|
        pi.call('SET', "key#{i}", "val#{i}")
      end
    end

    profile = StackProf.run(mode: :cpu, raw: true) do
      client.pipelined do |pi|
        ATTEMPTS.times do
          PIPELINE_SIZE.times do |i|
            pi.call('GET', "key#{i}")
          end
        end
      end
    end

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
end

ProfStack.run
