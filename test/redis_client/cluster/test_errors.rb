# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    class TestErrors < Minitest::Test
      DummyError = Struct.new('DummyError', :message)

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
          raise ::RedisClient::Cluster::InitialSetupError, c[:errors]
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
          raise ::RedisClient::Cluster::OrchestrationCommandNotSupported, c[:command]
        rescue StandardError => e
          assert(e.message.start_with?(c[:want]), "Case: #{idx}")
        end
      end

      def test_command_error_collection_error
        [
          {
            errors: [DummyError.new('foo')],
            want: { msg: 'Command errors were replied on any node: foo', size: 1 }
          },
          {
            errors: [DummyError.new('foo'), DummyError.new('bar')],
            want: { msg: 'Command errors were replied on any node: foo,bar', size: 2 }
          },
          { errors: [], want: { msg: 'Command errors were replied on any node: ', size: 0 } },
          { errors: '', want: { msg: 'Command errors were replied on any node: ', size: 0 } },
          { errors: nil, want: { msg: 'Command errors were replied on any node: ', size: 0 } }
        ].each_with_index do |c, idx|
          raise ::RedisClient::Cluster::CommandErrorCollection, c[:errors]
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
          raise ::RedisClient::Cluster::AmbiguousNodeError, c[:command]
        rescue StandardError => e
          assert_equal(e.message, c[:want], "Case: #{idx}")
        end
      end

      def test_cross_slot_pipelining_error
        [
          { keys: %w[foo bar baz], want: 'keys: foo,bar,baz' },
          { keys: '', want: 'keys: ' },
          { keys: nil, want: 'keys: ' }
        ].each_with_index do |c, idx|
          raise ::RedisClient::Cluster::CrossSlotPipeliningError, c[:keys]
        rescue StandardError => e
          assert(e.message.end_with?(c[:want]), "Case: #{idx}")
        end
      end
    end
  end
end