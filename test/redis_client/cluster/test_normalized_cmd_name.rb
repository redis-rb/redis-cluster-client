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
        assert_equal('set', @subject.get_by_command(%w[SET foo bar]))
      end

      def test_get_by_subcommand
        assert_equal('cat', @subject.get_by_subcommand(%w[ACL CAT dangerous]))
      end

      def test_get_by_name
        assert_equal('ping', @subject.get_by_name('PING'))
      end
    end
  end
end
