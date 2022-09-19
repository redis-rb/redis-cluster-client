# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class Cluster
    class TestNormalizedCmdName < TestingWrapper
      def setup
        @subject = ::RedisClient::Cluster::NormalizedCmdName.instance
        @subject.clear
      end

      def teardown
        @subject.clear
      end

      def test_get_by_command
        [
          { want: 'set', command: %w[SET foo bar] },
          { want: 'set', command: %i[SET foo bar] },
          { want: 'set', command: 'SET' },
          { want: 'set', command: 'Set' },
          { want: 'set', command: :set },
          { want: '', command: [] },
          { want: '', command: {} },
          { want: '', command: '' },
          { want: '', command: nil }
        ].each do |c|
          got = @subject.get_by_command(c[:command])
          assert_equal(c[:want], got, "Case: #{c[:command]}")
        end
      end

      def test_get_by_subcommand
        [
          { want: 'cat', command: %w[ACL CAT dangerous] },
          { want: 'cat', command: %w[acl cat dangerous] },
          { want: 'cat', command: %i[ACL CAT dangerous] },
          { want: '', command: 'ACL CAT dangerous' },
          { want: '', command: :acl },
          { want: '', command: [] },
          { want: '', command: {} },
          { want: '', command: '' },
          { want: '', command: nil }
        ].each do |c|
          got = @subject.get_by_subcommand(c[:command])
          assert_equal(c[:want], got, "Case: #{c[:command]}")
        end
      end

      def test_get_by_name
        [
          { want: 'ping', name: 'PING' },
          { want: 'ping', name: :ping },
          { want: '', name: [] },
          { want: '', name: {} },
          { want: '', name: '' },
          { want: '', name: nil }
        ].each do |c|
          got = @subject.get_by_name(c[:name])
          assert_equal(c[:want], got, "Case: #{c[:name]}")
        end
      end

      def test_thread_safety
        results = Array.new(10, true)
        threads = results.each_with_index.map do |_, i|
          Thread.new do
            Thread.pass
            if i.even?
              results[i] = @subject.get_by_command(%w[SET foo bar]) == 'set'
            else
              @subject.clear
            end
          rescue StandardError
            results[i] = false
          end
        end

        threads.each(&:join)
        refute_includes(results, false)
      end
    end
  end
end
