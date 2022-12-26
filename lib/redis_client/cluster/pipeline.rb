# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/errors'
require 'redis_client/connection_mixin'
require 'redis_client/middlewares'
require 'redis_client/pooled'

class RedisClient
  class Cluster
    class Pipeline
      class Extended < ::RedisClient::Pipeline
        attr_reader :outer_indices

        def initialize(command_builder)
          super
          @outer_indices = nil
        end

        def add_outer_index(index)
          @outer_indices ||= []
          @outer_indices << index
        end

        def get_inner_index(outer_index)
          @outer_indices&.find_index(outer_index)
        end

        def get_callee_method(inner_index)
          if @timeouts.is_a?(Array) && !@timeouts[inner_index].nil?
            :blocking_call_v
          elsif _retryable?
            :call_once_v
          else
            :call_v
          end
        end

        def get_command(inner_index)
          @commands.is_a?(Array) ? @commands[inner_index] : nil
        end

        def get_timeout(inner_index)
          @timeouts.is_a?(Array) ? @timeouts[inner_index] : nil
        end

        def get_block(inner_index)
          @blocks.is_a?(Array) ? @blocks[inner_index] : nil
        end
      end

      ::RedisClient::ConnectionMixin.module_eval do
        def call_pipelined_aware_of_redirection(commands, timeouts) # rubocop:disable Metrics/AbcSize
          size = commands.size
          results = Array.new(commands.size)
          @pending_reads += size
          write_multi(commands)

          redirection_indices = nil
          size.times do |index|
            timeout = timeouts && timeouts[index]
            result = read(timeout)
            @pending_reads -= 1
            if result.is_a?(CommandError)
              result._set_command(commands[index])
              if result.message.start_with?('MOVED', 'ASK')
                redirection_indices ||= []
                redirection_indices << index
              end
            end

            results[index] = result
          end

          return results if redirection_indices.nil?

          err = ::RedisClient::Cluster::Pipeline::RedirectionNeeded.new
          err.replies = results
          err.indices = redirection_indices
          raise err
        end
      end

      ::RedisClient.class_eval do
        attr_reader :middlewares

        def ensure_connected_cluster_scoped(retryable: true, &block)
          ensure_connected(retryable: retryable, &block)
        end
      end

      ReplySizeError = Class.new(::RedisClient::Error)

      class RedirectionNeeded < ::RedisClient::Error
        attr_accessor :replies, :indices
      end

      MAX_THREADS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', 5))

      def initialize(router, command_builder, seed: Random.new_seed)
        @router = router
        @command_builder = command_builder
        @seed = seed
        @pipelines = nil
        @size = 0
      end

      def call(*args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        append_pipeline(node_key).call_v(command, &block)
      end

      def call_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        append_pipeline(node_key).call_v(command, &block)
      end

      def call_once(*args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        append_pipeline(node_key).call_once_v(command, &block)
      end

      def call_once_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        append_pipeline(node_key).call_once_v(command, &block)
      end

      def blocking_call(timeout, *args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        append_pipeline(node_key).blocking_call_v(timeout, command, &block)
      end

      def blocking_call_v(timeout, args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        append_pipeline(node_key).blocking_call_v(timeout, command, &block)
      end

      def empty?
        @size.zero?
      end

      def execute # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        all_replies = errors = nil
        @pipelines&.each_slice(MAX_THREADS) do |chuncked_pipelines|
          threads = chuncked_pipelines.map do |node_key, pipeline|
            Thread.new(node_key, pipeline) do |nk, pl|
              Thread.current[:node_key] = nk
              replies = do_pipelining(@router.find_node(nk), pl)
              raise ReplySizeError, "commands: #{pl._size}, replies: #{replies.size}" if pl._size != replies.size

              Thread.current[:replies] = replies
            rescue ::RedisClient::Cluster::Pipeline::RedirectionNeeded => e
              Thread.current[:redirection_needed] = e
            rescue StandardError => e
              Thread.current[:error] = e
            end
          end

          threads.each(&:join)
          threads.each do |t|
            if t.key?(:replies)
              all_replies ||= Array.new(@size)
              @pipelines[t[:node_key]]
                .outer_indices
                .each_with_index { |outer, inner| all_replies[outer] = t[:replies][inner] }
            elsif t.key?(:redirection_needed)
              all_replies ||= Array.new(@size)
              pipeline = @pipelines[t[:node_key]]
              err = t[:redirection_needed]
              err.indices.each { |i| err.replies[i] = handle_redirection(err.replies[i], pipeline, i) }
              pipeline.outer_indices.each_with_index { |outer, inner| all_replies[outer] = err.replies[inner] }
            elsif t.key?(:error)
              errors ||= {}
              errors[t[:node_key]] = t[:error]
            end
          end
        end

        raise ::RedisClient::Cluster::ErrorCollection, errors unless errors.nil?

        all_replies
      end

      private

      def append_pipeline(node_key)
        @pipelines ||= {}
        @pipelines[node_key] ||= ::RedisClient::Cluster::Pipeline::Extended.new(@command_builder)
        @pipelines[node_key].add_outer_index(@size)
        @size += 1
        @pipelines[node_key]
      end

      def do_pipelining(client, pipeline)
        case client
        when ::RedisClient then send_pipeline(client, pipeline)
        when ::RedisClient::Pooled then client.with { |cli| send_pipeline(cli, pipeline) }
        else raise NotImplementedError, "#{client.class.name}#pipelined for cluster client"
        end
      end

      def send_pipeline(client, pipeline)
        results = client.ensure_connected_cluster_scoped(retryable: pipeline._retryable?) do |connection|
          commands = pipeline._commands
          client.middlewares.call_pipelined(commands, client.config) do
            connection.call_pipelined_aware_of_redirection(commands, pipeline._timeouts)
          end
        end

        pipeline._coerce!(results)
      end

      def handle_redirection(err, pipeline, inner_index)
        return err unless err.is_a?(::RedisClient::CommandError)

        if err.message.start_with?('MOVED')
          node = @router.assign_redirection_node(err.message)
          try_redirection(node, pipeline, inner_index)
        elsif err.message.start_with?('ASK')
          node = @router.assign_asking_node(err.message)
          try_asking(node) ? try_redirection(node, pipeline, inner_index) : err
        else
          err
        end
      end

      def try_redirection(node, pipeline, inner_index)
        redirect_command(node, pipeline, inner_index)
      rescue StandardError => e
        e
      end

      def redirect_command(node, pipeline, inner_index)
        method = pipeline.get_callee_method(inner_index)
        command = pipeline.get_command(inner_index)
        timeout = pipeline.get_timeout(inner_index)
        block = pipeline.get_block(inner_index)
        args = timeout.nil? ? [] : [timeout]

        if block.nil?
          @router.try_send(node, method, command, args)
        else
          @router.try_send(node, method, command, args, &block)
        end
      end

      def try_asking(node)
        node.call('ASKING') == 'OK'
      rescue StandardError
        false
      end
    end
  end
end
