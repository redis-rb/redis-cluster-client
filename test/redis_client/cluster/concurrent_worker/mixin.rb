# frozen_string_literal: true

class RedisClient
  class Cluster
    module ConcurrentWorker
      module Mixin
        def setup
          @worker = ::RedisClient::Cluster::ConcurrentWorker.create(model: model)
        end

        def test_work_group
          size = 10
          group = @worker.new_group(size: size)

          size.times do |i|
            group.push(i, i) do |n|
              sleep 0.001
              n * 2
            end
          end

          want = Array.new(size) { |i| i * 2 }
          got = []

          group.each do |_, v| # rubocop:disable Style/HashEachMethods
            got << v
          end

          group.close

          assert_equal(want, got.sort)
        end

        def test_too_many_tasks
          group = @worker.new_group(size: 5)
          5.times { |i| group.push(i, i) { |n| n } }
          assert_raises(InvalidNumberOfTasks) { group.push(5, 5) { |n| n } }
          sum = 0
          group.each { |_, v| sum += v } # rubocop:disable Style/HashEachMethods
          assert_equal(10, sum)
          group.close
        end

        def test_fewer_tasks
          group = @worker.new_group(size: 5)
          4.times { |i| group.push(i, i) { |n| n } }
          sum = 0
          assert_raises(InvalidNumberOfTasks) { group.each { |_, v| sum += v } } # rubocop:disable Style/HashEachMethods
          group.push(4, 4) { |n| n }
          group.each { |_, v| sum += v } # rubocop:disable Style/HashEachMethods
          assert_equal(10, sum)
          group.close
        end

        def teardown
          @worker.close
        end
      end
    end
  end
end
