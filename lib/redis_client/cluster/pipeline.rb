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

        def initialize(...)
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
        def call_pipelined_aware_of_redirection(commands, timeouts, exception:) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          size = commands.size
          results = Array.new(commands.size)
          @pending_reads += size
          write_multi(commands)
          redirection_indices = stale_cluster_state = first_exception = nil

          size.times do |index|
            timeout = timeouts && timeouts[index]
            result = read(connection_timeout(timeout))
            @pending_reads -= 1

            if result.is_a?(::RedisClient::Error)
              result._set_command(commands[index])
              result._set_config(config)

              if result.is_a?(::RedisClient::CommandError) && result.message.start_with?('MOVED', 'ASK')
                redirection_indices ||= []
                redirection_indices << index
              elsif exception
                first_exception ||= result
              end

              stale_cluster_state = true if result.message.start_with?('CLUSTERDOWN')
            end

            results[index] = result
          end

          if redirection_indices
            err = ::RedisClient::Cluster::Pipeline::RedirectionNeeded.new
            err.replies = results
            err.indices = redirection_indices
            err.first_exception = first_exception
            raise err
          end

          if stale_cluster_state
            err = ::RedisClient::Cluster::Pipeline::StaleClusterState.new
            err.replies = results
            err.first_exception = first_exception
            raise err
          end

          raise first_exception if first_exception

          results
        end
      end

      ::RedisClient.class_eval do
        attr_reader :middlewares

        def ensure_connected_cluster_scoped(retryable: true, &block)
          ensure_connected(retryable: retryable, &block)
        end
      end

      ReplySizeError = Class.new(::RedisClient::Error)

      class StaleClusterState < ::RedisClient::Error
        attr_accessor :replies, :first_exception
      end

      class RedirectionNeeded < ::RedisClient::Error
        attr_accessor :replies, :indices, :first_exception
      end

      def initialize(router, command_builder, concurrent_worker, exception:, seed: Random.new_seed)
        @router = router
        @command_builder = command_builder
        @concurrent_worker = concurrent_worker
        @exception = exception
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
        return if @pipelines.nil? || @pipelines.empty?

        work_group = @concurrent_worker.new_group(size: @pipelines.size)

        @pipelines.each do |node_key, pipeline|
          work_group.push(node_key, @router.find_node(node_key), pipeline) do |cli, pl|
            replies = do_pipelining(cli, pl)
            raise ReplySizeError, "commands: #{pl._size}, replies: #{replies.size}" if pl._size != replies.size

            replies
          end
        end

        all_replies = errors = required_redirections = cluster_state_errors = nil

        work_group.each do |node_key, v|
          case v
          when ::RedisClient::Cluster::Pipeline::RedirectionNeeded
            required_redirections ||= {}
            required_redirections[node_key] = v
          when ::RedisClient::Cluster::Pipeline::StaleClusterState
            cluster_state_errors ||= {}
            cluster_state_errors[node_key] = v
          when StandardError
            cluster_state_errors ||= {} if v.is_a?(::RedisClient::ConnectionError)
            errors ||= {}
            errors[node_key] = v
          else
            all_replies ||= Array.new(@size)
            @pipelines[node_key].outer_indices.each_with_index { |outer, inner| all_replies[outer] = v[inner] }
          end
        end

        work_group.close
        @router.renew_cluster_state if cluster_state_errors
        raise ::RedisClient::Cluster::ErrorCollection, errors unless errors.nil?

        required_redirections&.each do |node_key, v|
          raise v.first_exception if v.first_exception

          all_replies ||= Array.new(@size)
          pipeline = @pipelines[node_key]
          v.indices.each { |i| v.replies[i] = handle_redirection(v.replies[i], pipeline, i) }
          pipeline.outer_indices.each_with_index { |outer, inner| all_replies[outer] = v.replies[inner] }
        end

        cluster_state_errors&.each do |node_key, v|
          raise v.first_exception if v.first_exception

          all_replies ||= Array.new(@size)
          @pipelines[node_key].outer_indices.each_with_index { |outer, inner| all_replies[outer] = v.replies[inner] }
        end

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
            connection.call_pipelined_aware_of_redirection(commands, pipeline._timeouts, exception: @exception)
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
        @exception ? raise : e
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
