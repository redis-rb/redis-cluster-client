# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterDown < TestingWrapper
  WAIT_SEC = 0.1

  def setup
    @captured_commands = ::Middlewares::CommandCapture::CommandBuffer.new
    @redirect_count = ::Middlewares::RedirectCount::Counter.new
    @clients = Array.new(5) { build_client }
    @threads = []
    @controller = nil
    @cluster_down_counter = Counter.new
    @pubsub_recorder = Recorder.new
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
    @threads << spawn_single(@clients[0])
    @threads << spawn_pipeline(@clients[1])
    @threads << spawn_transaction(@clients[2])
    @threads << spawn_subscriber(@clients[3])
    @threads << spawn_publisher(@clients[4])
    wait_for_jobs_to_be_stable

    system('docker compose --progress quiet down', exception: true)
    system('docker system prune --force --volumes', exception: true, out: File::NULL)
    system('docker compose --progress quiet up --detach', exception: true)
    @controller = build_controller
    @controller.wait_for_cluster_to_be_ready
    wait_for_jobs_to_be_stable

    refute(@cluster_down_counter.get.zero?, 'Case: cluster down count')
    refute(@captured_commands.count('cluster', 'nodes').zero?, 'Case: cluster nodes calls')

    client = build_client(custom: nil, middlewares: nil)
    @clients << client

    single_value1 = client.call('get', 'single', &:to_i)
    pipeline_value1 = client.call('get', 'pipeline', &:to_i)
    transaction_value1 = client.call('get', 'transaction', &:to_i)
    pubsub_message1 = @pubsub_recorder.get.to_i

    wait_for_jobs_to_be_stable

    single_value2 = client.call('get', 'single', &:to_i)
    pipeline_value2 = client.call('get', 'pipeline', &:to_i)
    transaction_value2 = client.call('get', 'transaction', &:to_i)
    pubsub_message2 = @pubsub_recorder.get.to_i

    assert(single_value1 < single_value2, "Single: #{single_value1} < #{single_value2}")
    assert(pipeline_value1 < pipeline_value2, "Pipeline: #{pipeline_value1} < #{pipeline_value2}")
    assert(transaction_value1 < transaction_value2, "Transaction: #{transaction_value1} < #{transaction_value2}")
    assert(pubsub_message1 < pubsub_message2, "PubSub: #{pubsub_message1} < #{pubsub_message2}")
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

  def spawn_single(cli)
    Thread.new(cli) do |r|
      loop do
        handle_errors do
          r.call('incr', 'single')
          r.call('incr', 'single')
        end
      ensure
        sleep WAIT_SEC
      end
    end
  end

  def spawn_pipeline(cli)
    Thread.new(cli) do |r|
      loop do
        handle_errors do
          r.pipelined do |pi|
            pi.call('incr', 'pipeline')
            pi.call('incr', 'pipeline')
          end
        end
      ensure
        sleep WAIT_SEC
      end
    end
  end

  def spawn_transaction(cli)
    Thread.new(cli) do |r|
      i = 0
      loop do
        handle_errors do
          r.multi(watch: i.odd? ? %w[transaction] : nil) do |tx|
            i += 1
            tx.call('incr', 'transaction')
            tx.call('incr', 'transaction')
          end
        end
      ensure
        sleep WAIT_SEC
      end
    end
  end

  def spawn_publisher(cli)
    Thread.new(cli) do |r|
      i = 0
      loop do
        handle_errors do
          r.call('spublish', 'chan', i)
          i += 1
        end
      ensure
        sleep WAIT_SEC
      end
    end
  end

  def spawn_subscriber(cli)
    Thread.new(cli) do |r|
      ps = nil

      loop do
        ps = r.pubsub
        ps.call('ssubscribe', 'chan')
        break
      rescue StandardError
        ps&.close
      ensure
        sleep WAIT_SEC
      end

      loop do
        handle_errors do
          event = ps.next_event(0.01)
          case event&.first
          when 'smessage' then @pubsub_recorder.set(event[2])
          end
        end
      ensure
        sleep WAIT_SEC
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
    now = Process.clock_gettime(Process::CLOCK_MONOTONIC, :microsecond)

    loop do
      raise MaxRetryExceeded if attempts <= 0

      attempts -= 1
      before = @cluster_down_counter.get
      sleep WAIT_SEC * (@threads.size * 2)
      after = @cluster_down_counter.get
      break if before == after && @pubsub_recorder.updated?(now)
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

    def updated?(microsecond)
      return false if @updated_at.nil?

      microsecond < @updated_at
    end
  end
end
