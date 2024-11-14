# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class Cluster
    class TestErrors < TestingWrapper
      DummyError = Class.new(StandardError)

      def test_initial_setup_error
        [
          {
            errors: [DummyError.new('foo')],
            want: 'Redis client could not fetch cluster information: foo'
          },
          {
            errors: [DummyError.new('foo'), DummyError.new('bar')],
            want: 'Redis client could not fetch cluster information: foo,bar'
          },
          { errors: [], want: 'Redis client could not fetch cluster information: ' },
          { errors: '', want: 'Redis client could not fetch cluster information: ' },
          { errors: nil, want: 'Redis client could not fetch cluster information: ' }
        ].each_with_index do |c, idx|
          raise ::RedisClient::Cluster::InitialSetupError.from_errors(c[:errors])
        rescue StandardError => e
          assert_equal(c[:want], e.message, "Case: #{idx}")
        end
      end

      def test_orchestration_command_not_supported_error
        [
          { command: %w[CLUSTER FORGET], want: 'CLUSTER FORGET command should be' },
          { command: [], want: ' command should be' },
          { command: '', want: ' command should be' },
          { command: nil, want: ' command should be' }
        ].each_with_index do |c, idx|
          raise ::RedisClient::Cluster::OrchestrationCommandNotSupported.from_command(c[:command])
        rescue StandardError => e
          assert(e.message.start_with?(c[:want]), "Case: #{idx}")
        end
      end

      def test_error_collection_error
        [
          {
            errors: { '127.0.0.1:6379' => DummyError.new('foo') },
            want: { msg: '127.0.0.1:6379: (RedisClient::Cluster::TestErrors::DummyError) foo', size: 1 }
          },
          {
            errors: { '127.0.0.1:6379' => DummyError.new('foo'), '127.0.0.1:6380' => DummyError.new('bar') },
            want: { msg: '127.0.0.1:6379: (RedisClient::Cluster::TestErrors::DummyError) foo, 127.0.0.1:6380: (RedisClient::Cluster::TestErrors::DummyError) bar', size: 2 }
          },
          { errors: {}, want: { msg: '{}', size: 0 } },
          { errors: [], want: { msg: '[]', size: 0 } },
          { errors: '', want: { msg: '', size: 0 } },
          { errors: nil, want: { msg: '', size: 0 } }
        ].each_with_index do |c, idx|
          raise ::RedisClient::Cluster::ErrorCollection.with_errors(c[:errors])
        rescue StandardError => e
          assert_equal(c[:want][:msg], e.message, "Case: #{idx}")
          assert_equal(c[:want][:size], e.errors.size, "Case: #{idx}")
        end
      end

      def test_ambiguous_node_error
        [
          { command: 'MULTI', want: "Cluster client doesn't know which node the MULTI command should be sent to." },
          { command: nil, want: "Cluster client doesn't know which node the  command should be sent to." }
        ].each_with_index do |c, idx|
          raise ::RedisClient::Cluster::AmbiguousNodeError.from_command(c[:command])
        rescue StandardError => e
          assert_equal(e.message, c[:want], "Case: #{idx}")
        end
      end
    end
  end
end
