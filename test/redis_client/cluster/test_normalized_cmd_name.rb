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
        @subject&.clear
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
        attempts = Array.new(100, 'dummy')

        threads = attempts.each_with_index.map do |_, i|
          Thread.new do
            Thread.current.thread_variable_set(:index, i)
            got = if i.even?
                    @subject.get_by_command(%w[SET foo bar])
                  else
                    @subject.clear ? 'set' : 'clear failed'
                  end
            Thread.current.thread_variable_set(:got, got)
          rescue StandardError => e
            Thread.current.thread_variable_set(:got, "#{e.class.name}: #{e.message}")
          end
        end

        threads.each do |t|
          t.join
          attempts[t.thread_variable_get(:index)] = t.thread_variable_get(:got)
        end

        attempts.each { |got| assert_equal('set', got) }
      end
    end
  end
end
