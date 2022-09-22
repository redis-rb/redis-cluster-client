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
          @outer_indices[@outer_indices.size] = index
        end

        def get_inner_index(outer_index)
          @outer_indices&.find_index(outer_index)
        end

        def get_callee_method(inner_index)
          if !@timeouts[inner_index].nil?
            :blocking_call_v
          elsif _retryable?
            :call_once_v
          else
            :call_v
          end
        end

        def get_command(inner_index)
          @commands[inner_index]
        end

        def get_timeout(inner_index)
          @timeouts[inner_index]
        end

        def get_block(inner_index)
          @blocks[inner_index]
        end
      end

      ::RedisClient::ConnectionMixin.module_eval do
        def call_pipelined_without_raising_error(commands, timeouts)
          size = commands.size
          results = Array.new(commands.size)
          @pending_reads += size
          write_multi(commands)

          size.times do |index|
            timeout = timeouts && timeouts[index]
            result = read(timeout)
            @pending_reads -= 1
            result._set_command(commands[index]) if result.is_a?(CommandError)
            results[index] = result
          end

          results
        end
      end

      ReplySizeError = Class.new(::RedisClient::Error)
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
        get_pipeline(node_key).call_v(command, &block)
      end

      def call_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).call_v(command, &block)
      end

      def call_once(*args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).call_once_v(command, &block)
      end

      def call_once_v(args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).call_once_v(command, &block)
      end

      def blocking_call(timeout, *args, **kwargs, &block)
        command = @command_builder.generate(args, kwargs)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).blocking_call_v(timeout, command, &block)
      end

      def blocking_call_v(timeout, args, &block)
        command = @command_builder.generate(args)
        node_key = @router.find_node_key(command, seed: @seed)
        get_pipeline(node_key).blocking_call_v(timeout, command, &block)
      end

      def empty?
        @size.zero?
      end

      def execute # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        all_replies = errors = nil
        @pipelines&.each_slice(MAX_THREADS) do |chuncked_pipelines|
          threads = chuncked_pipelines.map do |node_key, pipeline|
            Thread.new(node_key, pipeline) do |nk, pl|
              Thread.pass
              Thread.current.thread_variable_set(:node_key, nk)
              replies = do_pipelining(@router.find_node(nk), pl)
              raise ReplySizeError, "commands: #{pl._size}, replies: #{replies.size}" if pl._size != replies.size

              Thread.current.thread_variable_set(:replies, replies)
            rescue StandardError => e
              Thread.current.thread_variable_set(:error, e)
            end
          end

          threads.each do |t|
            t.join
            if t.thread_variable?(:replies)
              all_replies ||= Array.new(@size)
              @pipelines[t.thread_variable_get(:node_key)]
                .outer_indices
                .each_with_index { |outer, inner| all_replies[outer] = t.thread_variable_get(:replies)[inner] }
            elsif t.thread_variable?(:error)
              errors ||= {}
              errors[t.thread_variable_get(:node_key)] = t.thread_variable_get(:error)
            end
          end
        end

        raise ::RedisClient::Cluster::ErrorCollection, errors unless errors.nil?

        retry_redirections_if_needed(all_replies)
      end

      private

      def get_pipeline(node_key)
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
        results = client.send(:ensure_connected, retryable: pipeline._retryable?) do |connection|
          commands = pipeline._commands
          ::RedisClient::Middlewares.call_pipelined(commands, client.config) do
            connection.call_pipelined_without_raising_error(commands, pipeline._timeouts)
          end
        end

        pipeline._coerce!(results)
      end

      def retry_redirections_if_needed(replies)
        replies.map!.with_index do |reply, i|
          case reply
          when ::RedisClient::CommandError
            if reply.message.start_with?('MOVED')
              node = assign_redirection_node(reply.message)
              retry_redirection(node, reply, i)
            elsif reply.message.start_with?('ASK')
              node = assign_asking_node(reply.message)
              retry_redirection(node, reply, i)
            else
              reply
            end
          else reply
          end
        end
      end

      def retry_redirection(node, err, outer_index)
        _, _, node_key = err.message.split
        return err unless @pipelines.key?(node_key)

        pl = @pipelines.fetch(node_key)
        inner_index = pl.get_inner_index(outer_index)
        method = pl.get_callee_method(inner_index)
        command = pl.get_command(inner_index)
        timeout = pl.get_timeout(inner_index)
        args = timeout.nil? ? [] : [timeout]
        block = pl.get_block(inner_index)
        if block.nil?
          @router.try_send(node, method, command, args)
        else
          @router.try_send(node, method, command, args, &block)
        end
      end
    end
  end
end
