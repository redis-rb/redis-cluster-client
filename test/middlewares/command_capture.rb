# frozen_string_literal: true

module Middlewares
  module CommandCapture
    CapturedCommand = Struct.new('CapturedCommand', :server_url, :command, :pipelined, keyword_init: true) do
      def inspect
        "#<#{self.class.name} [on #{server_url}] #{command.join(' ')} >"
      end
    end

    # The CommandBuffer is what should be set as the :captured_commands custom option.
    # It needs to be threadsafe, because redis-cluster-client performs some redis operations on
    # multiple nodes in parallel, and in e.g. jruby it's not safe to concurrently manipulate the same array.
    class CommandBuffer
      def initialize
        @array = []
        @mutex = Mutex.new
      end

      def to_a
        @mutex.synchronize do
          @array.dup
        end
      end

      def <<(command)
        @mutex.synchronize do
          @array << command
        end
      end

      def count(*cmd)
        @mutex.synchronize do
          next 0 if @array.empty?

          @array.count do |e|
            cmd.size.times.all? { |i| cmd[i].downcase == e.command[i]&.downcase }
          end
        end
      end

      def clear
        @mutex.synchronize do
          @array.clear
        end
      end
    end

    def call(command, redis_config)
      redis_config.custom[:captured_commands] << CapturedCommand.new(
        server_url: ::Middlewares::CommandCapture.normalize_captured_url(redis_config.server_url),
        command: command,
        pipelined: false
      )
      super
    end

    def call_pipelined(commands, redis_config)
      commands.map do |command|
        redis_config.custom[:captured_commands] << CapturedCommand.new(
          server_url: ::Middlewares::CommandCapture.normalize_captured_url(redis_config.server_url),
          command: command,
          pipelined: true
        )
      end
      super
    end

    def self.normalize_captured_url(url)
      URI.parse(url).tap do |u|
        u.path = ''
      end.to_s
    end
  end
end
