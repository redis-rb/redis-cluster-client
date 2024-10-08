# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterDown < TestingWrapper
  WAIT_SEC = 0.1
  CASES = %w[Single Pipeline Transaction Subscriber Publisher].freeze
  SINGLE_KEYS = %w[single1 single3 single4].freeze
  PIPELINE_KEYS = %w[pipeline1 pipeline2 pipeline4].freeze
  TRANSACTION_KEYS = %w[transaction1 transaction3 transaction4].freeze
  CHANNELS = %w[chan1 chan2 chan3].freeze
  NUMBER_OF_JOBS = SINGLE_KEYS.size + PIPELINE_KEYS.size + TRANSACTION_KEYS.size + CHANNELS.size * 2

  def setup
    @captured_commands = ::Middlewares::CommandCapture::CommandBuffer.new
    @redirect_count = ::Middlewares::RedirectCount::Counter.new
    @clients = Array.new(NUMBER_OF_JOBS) { build_client }
    @threads = []
    @controller = nil
    @cluster_down_counter = Counter.new
    @recorders = Array.new(NUMBER_OF_JOBS) { Recorder.new }
    @captured_commands.clear
    @redirect_count.clear
  end

  def teardown
    @controller&.close
    @threads&.each(&:exit)
    @clients&.each(&:close)
    print "#{@redirect_count.get}, "\
      "ClusterNodesCall: #{@captured_commands.count('cluster', 'nodes')}, "\
      "ClusterDownError: #{@cluster_down_counter.get} = "
  end

  def test_recoverability_from_cluster_down
    SINGLE_KEYS.each_with_index { |key, i| @threads << spawn_single(@clients[i], @recorders[i], key) }
    PIPELINE_KEYS.each_with_index { |key, i| @threads << spawn_pipeline(@clients[i + 3], @recorders[i + 3], key) }
    TRANSACTION_KEYS.each_with_index { |key, i| @threads << spawn_transaction(@clients[i + 6], @recorders[i + 6], key) }
    CHANNELS.each_with_index do |channel, i|
      @threads << spawn_subscriber(@clients[i + 9], @recorders[i + 9], channel)
      @threads << spawn_publisher(@clients[i + 12], @recorders[i + 12], channel)
    end
    wait_for_jobs_to_be_stable

    system('docker compose --progress quiet down', exception: true)
    system('docker system prune --force --volumes', exception: true, out: File::NULL)
    system('docker compose --progress quiet up --detach', exception: true)
    @controller = build_controller
    @controller.wait_for_cluster_to_be_ready
    wait_for_jobs_to_be_stable

    refute(@cluster_down_counter.get.zero?, 'Case: cluster down count')
    refute(@captured_commands.count('cluster', 'nodes').zero?, 'Case: cluster nodes calls')

    @values_a = @recorders.map { |r| r.get.to_i }
    wait_for_jobs_to_be_stable
    @values_b = @recorders.map { |r| r.get.to_i }
    @recorders.each_with_index do |_, i|
      assert(@values_a[i] < @values_b[i], "#{CASES[i]}: #{@values_a[i]} < #{@values_b[i]}")
    end
  end

  private

  def build_client(
    custom: { captured_commands: @captured_commands, redirect_count: @redirect_count },
    middlewares: [::Middlewares::CommandCapture, ::Middlewares::RedirectCount],
    **opts
  )
    ::RedisClient.cluster(
      nodes: TEST_NODE_URIS,
      connect_with_original_config: true,
      fixed_hostname: TEST_FIXED_HOSTNAME,
      custom: custom,
      middlewares: middlewares,
      **TEST_GENERIC_OPTIONS,
      **opts
    ).new_client
  end

  def build_controller
    ClusterController.new(
      TEST_NODE_URIS,
      replica_size: TEST_REPLICA_SIZE,
      **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
    )
  end

  def spawn_single(client, recorder, key)
    Thread.new(client, recorder, key) do |cli, rec, k|
      loop do
        handle_errors do
          reply = cli.call('incr', k)
          rec.set(reply)
        end
      ensure
        sleep WAIT_SEC
      end
    end
  end

  def spawn_pipeline(client, recorder, key)
    Thread.new(client, recorder, key) do |cli, rec, k|
      loop do
        handle_errors do
          reply = cli.pipelined do |pi|
            pi.call('incr', k)
            pi.call('incr', k)
          end

          rec.set(reply.last)
        end
      ensure
        sleep WAIT_SEC
      end
    end
  end

  def spawn_transaction(client, recorder, key)
    Thread.new(client, recorder, key) do |cli, rec, k|
      i = 0
      loop do
        handle_errors do
          reply = cli.multi(watch: i.odd? ? [k] : nil) do |tx|
            tx.call('incr', k)
            tx.call('incr', k)
          end

          rec.set(reply.last)
          i += 1
        end
      ensure
        sleep WAIT_SEC
      end
    end
  end

  def spawn_publisher(client, recorder, channel)
    Thread.new(client, recorder, channel) do |cli, rec, chan|
      i = 0
      loop do
        handle_errors do
          cli.call('spublish', chan, i)
        end

        rec.set(i)
        i += 1
      ensure
        sleep WAIT_SEC
      end
    end
  end

  def spawn_subscriber(client, recorder, channel)
    Thread.new(client, recorder, channel) do |cli, rec, chan|
      ps = nil

      loop do
        ps = cli.pubsub
        ps.call('ssubscribe', chan)
        break
      rescue StandardError
        ps&.close
      ensure
        sleep WAIT_SEC
      end

      loop do
        handle_errors do
          event = ps.next_event(WAIT_SEC)
          case event&.first
          when 'smessage' then rec.set(event[2])
          when 'sunsubscribe' then ps.call('ssubscribe', chan)
          end
        end
      end
    rescue StandardError, SignalException
      ps&.close
      raise
    end
  end

  def handle_errors
    yield
  rescue ::RedisClient::ConnectionError, ::RedisClient::Cluster::InitialSetupError, ::RedisClient::Cluster::NodeMightBeDown
    @cluster_down_counter.increment
  rescue ::RedisClient::CommandError => e
    raise unless e.message.start_with?('CLUSTERDOWN')

    @cluster_down_counter.increment
  rescue ::RedisClient::Cluster::ErrorCollection => e
    raise unless e.errors.values.all? do |err|
      err.message.start_with?('CLUSTERDOWN') || err.is_a?(::RedisClient::ConnectionError)
    end

    @cluster_down_counter.increment
  end

  def wait_for_jobs_to_be_stable(attempts: 100)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC, :microsecond)
    sleep_sec = WAIT_SEC * (@threads.size * 2)

    @recorders.each do |recorder|
      loop do
        raise MaxRetryExceeded if attempts <= 0

        attempts -= 1
        next sleep(sleep_sec) unless recorder.updated?(start)

        value_a = recorder.get.to_i
        sleep sleep_sec
        value_b = recorder.get.to_i
        break if value_a < value_b
      end
    end
  end

  class Counter
    def initialize
      @count = 0
      @mutex = Mutex.new
    end

    def increment
      @mutex.synchronize { @count += 1 }
    end

    def get
      @mutex.synchronize { @count }
    end
  end

  class Recorder
    def initialize
      @last_value = nil
      @updated_at = nil
      @mutex = Mutex.new
    end

    def set(value)
      @mutex.synchronize do
        @last_value = value
        @updated_at = Process.clock_gettime(Process::CLOCK_MONOTONIC, :microsecond)
      end
    end

    def get
      @mutex.synchronize { @last_value }
    end

    def updated?(since)
      @mutex.synchronize do
        if @updated_at.nil?
          false
        else
          since < @updated_at
        end
      end
    end
  end
end
